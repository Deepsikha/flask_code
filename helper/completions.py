from collections import defaultdict
from ai_project.models.completions import Completions, CompletionsMeta
from ai_project.models.user_projects import UserProjects
from ai_project.utils.labeling_config import parse_config
from ai_project.utils.misc import logger
from ai_project.models.tasks import Tasks


def completion_to_exclude(
    completion, reviewer, assignee, owner_or_manager, username
):
    if owner_or_manager:
        return False
    completion_owner = completion.get("created_username")
    loggedin_user = username

    submitted = bool(completion.get("submitted_at"))
    if loggedin_user in reviewer and submitted:
        return False

    if completion_owner != loggedin_user:
        return True


def filter_ground_truth(completions):
    new_completions = []
    for c in completions:
        if "honeypot" in c and c["honeypot"]:
            new_completions.append(c)
    return new_completions


def prepare_completions_json(
    project_id,
    tags: list,
    ground_truth_flag,
    exclude_tasks_without_completions_flag,
):
    if exclude_tasks_without_completions_flag:
        data = Completions.get_completions(
            project_id=project_id,
            tags=tags,
            ground_truth=ground_truth_flag,
            fields=[
                Completions.completions,
                Completions.predictions,
                Completions.created_at,
                Completions.created_by,
                Completions.data,
                Completions.title,
                Completions.completion_id,
            ],
        )
    else:
        data = Tasks.get_all_tasks_with_completions(project_id, tags)
    final_data = list()
    for item in data:
        filtered_completions = []
        if item.completions:
            filtered_completions = [
                completion
                for completion in item.completions
                if not completion.get("deleted_at")
            ]
        item.data.pop("pagination", None)
        export_json = {
            "completions": filtered_completions
            if not ground_truth_flag
            else filter_ground_truth(filtered_completions),
            "predictions": item.predictions or [],
            "created_at": item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "created_by": item.created_by,
            "data": item.data,
            "id": item.completion_id
            if item.completion_id is not None
            else item.task_id,
        }
        if item.title:
            export_json["data"].update({"title": item.title})
        final_data.append(export_json)
    return final_data


def identify_config_type(data):
    if data.get("text"):
        return "text"
    elif data.get("longText"):
        return "longText"


def update_completions_meta_table(project_id, **kwargs):
    # Handle for older projects created before < 260
    project = UserProjects.get_project_by_project_id(
        project_id, ["created_version"]
    )
    if not project.created_version:
        return

    db_entry = CompletionsMeta.read(project_id)

    if kwargs.get("label_config"):
        from_name_to_name_type = list()
        parsed_config = parse_config(kwargs["label_config"])
        for from_name, to in parsed_config.items():
            from_name_to_name_type.append(
                {
                    "from_name": from_name,
                    "to_name": to["to_name"][0],
                    "type": to["type"].lower(),
                }
            )
        if not db_entry:
            CompletionsMeta(project_id, from_name_to_name_type).save()
        else:
            db_entry.from_name_to_name_type = from_name_to_name_type
            db_entry.save()

    elif kwargs.get("new_completion"):
        existing_info = db_entry.used_labels_info
        logger.debug(f"used_labels_info before db update: {existing_info}")
        new_info = get_labels_info(kwargs["new_completion"])
        logger.debug(f"new_info parsed from new_completion: {new_info}")
        merged_info = merge_labels_info(existing_info, new_info, "add")
        logger.debug(f"used_labels_info after db update: {merged_info}")
        CompletionsMeta.update(project_id, {"used_labels_info": merged_info})

    elif kwargs.get("deleted_completion"):
        existing_info = db_entry.used_labels_info
        logger.debug(f"used_labels_info before db update: {existing_info}")
        delete_info = get_labels_info(kwargs["deleted_completion"])
        logger.debug(
            f"delete_info parsed from deleted_completion: {delete_info}"
        )
        delete_labels_info(existing_info, delete_info)
        logger.debug(f"used_labels_info after db update: {existing_info}")
        CompletionsMeta.update(project_id, {"used_labels_info": existing_info})

    elif kwargs.get("updated_completion"):
        existing_info = db_entry.used_labels_info
        logger.debug(f"used_labels_info before db update: {existing_info}")
        old_info = get_labels_info(kwargs["updated_completion"]["old"])
        logger.debug(f"old_info parsed from old_completion: {old_info}")
        merged_info = merge_labels_info(existing_info, old_info, "sub")
        logger.debug(
            "merged_info after merging used_labels_info and old_info: "
            f"{merged_info}"
        )
        new_info = get_labels_info(kwargs["updated_completion"]["new"])
        merged_info = merge_labels_info(merged_info, new_info, "add")
        logger.debug(
            "merged_info after merging merged_info and new_info: "
            f"{merged_info}"
        )
        logger.debug(f"used_labels_info after db update: {merged_info}")
        CompletionsMeta.update(project_id, {"used_labels_info": merged_info})


def get_labels_info(completions):
    def default_to_regular(d):
        if isinstance(d, defaultdict):
            d = {k: default_to_regular(v) for k, v in d.items()}
        return d

    _info = defaultdict(lambda: defaultdict(int))
    for completion in completions:
        for result in completion.get("result", []):
            _type = result["type"]
            if _type in ["relation", "rating", "pairwise"]:
                continue
            if _type == "textarea":
                value = "text"
            else:
                value = result["value"][_type][0]
            _info[result["from_name"]][value] += 1
    return default_to_regular(_info)


def merge_labels_info(existing_info, new_info, action):
    if not existing_info:
        return new_info
    for name, values in new_info.items():
        if name not in existing_info:
            existing_info[name] = values
            continue
        for value, count in values.items():
            if value in existing_info[name].keys() and action == "add":
                existing_info[name][value] += count
            elif value in existing_info[name].keys() and action == "sub":
                existing_info[name][value] -= count
                if existing_info[name][value] < 1:
                    existing_info[name].pop(value)
            else:
                existing_info[name].update({value: count})
    return existing_info


def delete_labels_info(existing_info, delete_info):
    for name, values in existing_info.items():
        if name not in delete_info.keys():
            continue
        for value in list(values):
            if value not in delete_info[name].keys():
                continue
            existing_info[name][value] -= delete_info[name][value]
            if existing_info[name][value] < 1:
                existing_info[name].pop(value)


def validate_completion_data(completion, config):
    # No need to validate for setting/unsetting ground truth option
    if list(completion.keys()) == ["honeypot"]:
        return

    must_have_keys = ["from_name", "to_name", "type", "value"]
    completion_tuples_in_config = set()
    if completion.get("lead_time") and not isinstance(
        completion["lead_time"], int
    ):
        return "lead_time should be integer"
    for from_name, to in config.items():
        completion_tuples_in_config.add(
            (from_name, to["to_name"][0], to["type"].lower())
        )
    if "result" not in completion or not isinstance(
        completion["result"], list
    ):
        return "Missing/invalid 'result' format"

    for result in completion["result"]:
        if result["type"] in ["relation", "pairwise"]:
            continue
        if any(key not in result for key in must_have_keys):
            return "Missing from_name|to_name|type|value"
        if (
            result["from_name"],
            result["to_name"],
            result["type"],
        ) not in completion_tuples_in_config:
            return (
                "from_name|to_name|type should be according to the "
                "defined config"
            )
        if result["type"] in ["rating", "textarea"]:
            continue
        if result["type"] == "labels":
            if any(key not in result["value"] for key in ["start", "end"]):
                return "Missing start/end indexes"
            if not result.get("original_length") and not all(
                isinstance(result["value"][i], int) for i in ["start", "end"]
            ):
                return "start/end indexes should be integer"
        label = result["value"][result["type"]][0].strip()
        if label not in config[result["from_name"]]["labels"]:
            return f"Invalid {result['type']}: {label}"
