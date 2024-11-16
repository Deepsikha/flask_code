import json
from copy import deepcopy
from datetime import datetime
from ai_project.db import app
from flask import jsonify, request
from ai_project.models.tasks import Tasks
from ai_project.helpers.user import user_info
from ai_project.helpers.auth import check_permission
from ai_project.models.completions import Completions
from ai_project.models.user_projects import UserProjects
from ai_project.helpers.model_training import al_automatic_model_training
from ai_project.utils.misc import logger
from ai_project.helpers.projectai_project import (
    Projectai_project,
    project_is_visual_ner,
)
from ai_project.helpers.completions import (
    update_completions_meta_table,
    validate_completion_data,
)


@app.route(
    "/api/projects/<string:project_name>/completions_ids", methods=["GET"]
)
def api_all_completion_ids(project_name: str):
    """Get all completion ids"""
    project = Projectai_project(name=project_name)
    ids = project.get_completions_ids()
    return jsonify(ids), 200


@app.route(
    (
        "/api/projects/<string:project_name>/tasks/<int:task_id>"
        "/direct_submit_completion"
    ),
    methods=["POST"],
)
@check_permission("Annotator", "Reviewer", "Manager")
def api_direct_submit_completions(project_name: str, task_id: int):
    """
    Save and submit new completion
    """
    user_project = Projectai_project(name=project_name)
    project_id = UserProjects.get_project_by_project_name_if_exists(
        project_name=project_name, fields=["project_id"]
    ).project_id

    task_in_db = Tasks.get_task(project_id, task_id)

    # Not support for multi-page PDF (Visual NER)
    # Reason: PDF divided into pages, after submit 1st page,
    # user is unable to save work for other pages.
    if project_is_visual_ner(user_project.label_config_line) and isinstance(
        task_in_db.data.get("image"), list
    ):
        return (
            jsonify(
                {
                    "error": (
                        "Direct submit is not supported for multi-page PDF for"
                        " Visual NER project"
                    )
                }
            ),
            400,
        )

    completion = request.json
    if not completion.get("created_username"):
        completion["created_username"] = request.username
    if not completion.get("created_ago"):
        completion["created_ago"] = datetime.now().isoformat() + "Z"

    owner_or_manager = user_info(project_name)["owner_or_manager"]
    allowed = (
        owner_or_manager
        or request.username in task_in_db.assigned_to
        or request.username in task_in_db.reviewers
    )
    if not allowed:
        return jsonify({"error": "Permission denied"}), 403

    completion.pop("state", None)  # remove editor state
    completion_id = user_project.save_completion(
        task_id, completion, request.username
    )
    kwargs = {"new_completion": [completion]}
    update_completions_meta_table(project_id, **kwargs)
    logger.debug(f"OUTPUT={request.json}")
    logger.info(f"TASK_ID={task_id} COMPLETION SAVED!")

    # Active learning
    al_automatic_model_training(project_id, project_name)

    # Remove output schema for current project, if exists
    Projectai_project.clear_derived_output_schema(project_name)

    return jsonify({"id": completion_id}), 201


@app.route(
    "/api/projects/<string:project_name>/tasks/<int:task_id>/completions",
    methods=["POST"],
)
@check_permission("Annotator", "Reviewer", "Manager")
def api_save_completions(project_name: str, task_id: int):
    """
    Save new completion
    """
    user_project = Projectai_project(name=project_name)
    project_id = UserProjects.get_project_by_project_name_if_exists(
        project_name=project_name, fields=["project_id"]
    ).project_id

    completion = request.json
    completion["created_username"] = request.username
    completion["created_ago"] = datetime.now().isoformat() + "Z"
    confidence_range = completion.get("confidence_range")
    task_in_db = Tasks.get_task(project_id, task_id)
    if not task_in_db:
        return jsonify({"error": "Task not found"}), 404
    invalid_msg = validate_completion_data(
        completion, user_project.parsed_label_config
    )
    if invalid_msg:
        return jsonify({"error": invalid_msg}), 400

    owner_or_manager = user_info(project_name)["owner_or_manager"]
    allowed = (
        owner_or_manager
        or request.username in task_in_db.assigned_to
        or request.username in task_in_db.reviewers
    )
    if not allowed:
        return jsonify({"error": "Permission denied"}), 403
    # For copying completion
    if completion.get("copy"):
        completion_in_db = Completions.get_completion(task_in_db.id)
        completion_data = (
            completion_in_db.completions
            if completion.get("data_type") == "completion"
            else completion_in_db.predictions
        )
        for c in completion_data:
            if c.get("id") == int(completion["cid"]):
                result_ids = []
                results = []
                for result in c.get("result", []):
                    if (
                        result.get("value")
                        and confidence_range[0]
                        <= float(result["value"].get("confidence") or 0)
                        <= confidence_range[1]
                    ):
                        if (
                            result["value"].get("confidence") is None
                            and confidence_range[0] == 0
                        ):
                            result["value"]["confidence"] = (
                                1
                                if completion["data_type"] == "completion"
                                else 0
                            )
                        result_ids.append(result.get("id"))
                        results.append(result)
                    elif (
                        result.get("direction")
                        and result.get("from_id") in result_ids
                        and result.get("to_id") in result_ids
                    ):
                        results.append(result)
                completion["result"] = results
                break
        completion.pop("copy", None)

    completion.pop("state", None)  # remove editor state
    completion.pop("confidence_range", None)
    completion_id = user_project.save_completion(
        task_id, completion, request.username
    )
    kwargs = {"new_completion": [completion]}
    update_completions_meta_table(project_id, **kwargs)
    logger.debug(f"OUTPUT={request.json}")
    logger.info(f"TASK_ID={task_id} COMPLETION SAVED!")

    # Remove output schema for current project, if exists
    Projectai_project.clear_derived_output_schema(project_name)
    return jsonify({"id": completion_id}), 201


@app.route(
    (
        "/api/projects/<string:project_name>/tasks/<int:task_id>/completions"
        "/<int:completion_id>"
    ),
    methods=["DELETE"],
)
def api_completion_by_id(project_name: str, task_id: int, completion_id: int):
    """
    Delete completion
    """
    user_project = Projectai_project(name=project_name)

    project_id = UserProjects.get_project_by_project_name_if_exists(
        project_name=project_name, fields=["project_id"]
    ).project_id
    completion_data = Completions.get_completion_owner_submitted_timestamp(
        project_id=project_id, task_id=task_id, completion_id=completion_id
    )
    if not completion_data:
        return jsonify({"error": "Completion not found"}), 404

    if completion_data.created_username != request.username:
        return (
            jsonify(
                {
                    "error": (
                        f"user '{request.username}' is not allowed to delete"
                        " the completion."
                    )
                }
            ),
            400,
        )

    if completion_data.submitted_at:
        return (
            jsonify(
                {
                    "error": (
                        "Completion is already submitted. Cannot "
                        "delete submitted completion!"
                    )
                }
            ),
            400,
        )

    if user_project.config.get("allow_delete_completions", False):
        deleted_completion = user_project.delete_completions(
            [task_id], completion_id
        )
        kwargs = {"deleted_completion": deleted_completion}
        update_completions_meta_table(project_id, **kwargs)
        # Remove output schema for current project, if exists
        Projectai_project.clear_derived_output_schema(project_name)
        return (
            jsonify({"message": "Task completions removed successfully."}),
            204,
        )
    return (
        jsonify(
            {"error": "Completion removing is not allowed in server config"}
        ),
        422,
    )


@app.route(
    (
        "/api/projects/<string:project_name>/tasks/<int:task_id>/completions"
        "/<int:completion_id>/review"
    ),
    methods=["PATCH"],
)
@check_permission("Update", "Reviewer")
def api_review_completion(project_name: str, task_id: int, completion_id: int):
    """
    Add review to completion
    """
    review_data = request.json
    review_status = review_data.get("review_status", {})
    if (
        "review_status" not in review_data
        or "approved" not in review_data["review_status"]
    ):
        return jsonify({"error": "Invalid review data"}), 400
    if not review_data["review_status"].get("reviewed_at"):
        review_data["review_status"].update(
            {"reviewed_at": datetime.now().isoformat() + "Z"}
        )

    if not review_data["review_status"].get("reviewer"):
        review_data["review_status"].update({"reviewer": request.username})

    user_project = Projectai_project(name=project_name)

    project = UserProjects.get_project_by_project_name_if_exists(
        project_name=project_name, fields=["project_id", "owner"]
    )

    if review_status.get("approved") is False:
        review_data["honeypot"] = False

    completion_reviewer = Tasks.get_task(
        project_id=project.project_id, task_id=task_id
    )
    if request.username not in [
        *completion_reviewer.reviewers,
        project.owner.get("username"),
    ]:
        return (
            jsonify(
                {
                    "error": (
                        f"User '{request.username}' is not allowed to review "
                        "the completion."
                    )
                }
            ),
            400,
        )

    completion_data = Completions.get_completion_review_status(
        project_id=project.project_id,
        task_id=task_id,
        completion_id=completion_id,
    )
    if not completion_data:
        return jsonify({"error": "Completion not found"}), 404

    if not completion_data.submitted_at:
        return (
            jsonify({"error": "Cannot review unsubmitted completions!"}),
            400,
        )
    if completion_data.review_status:
        reviewer = json.loads(completion_data.review_status).get("reviewer")
        return (
            jsonify(
                {
                    "error": (
                        f"Completion is already reviewed by user '{reviewer}'"
                    )
                }
            ),
            400,
        )
    review_data["id"] = int(completion_id)
    user_project.save_completion(task_id, review_data, request.username)

    # Active learning
    al_automatic_model_training(project.project_id, project_name)

    return (
        jsonify({"message": "Completion review successfully submitted"}),
        201,
    )


@app.route(
    (
        "/api/projects/<string:project_name>/tasks/<int:task_id>/completions"
        "/<int:completion_id>"
    ),
    methods=["PATCH"],
)
def api_completion_update(project_name: str, task_id: int, completion_id: int):
    """
    Rewrite existing completion with patch
    """
    user_project = Projectai_project(name=project_name)

    project_id = UserProjects.get_project_by_project_name_if_exists(
        project_name=project_name, fields=["project_id"]
    ).project_id

    completion_data = Completions.get_completion_owner_submitted_timestamp(
        project_id=project_id, task_id=task_id, completion_id=completion_id
    )

    if not completion_data:
        return jsonify({"error": "Completion Not Found"}), 404

    invalid_msg = validate_completion_data(
        request.json, user_project.parsed_label_config
    )
    if invalid_msg:
        return jsonify({"error": invalid_msg}), 400

    if request.username not in completion_data.created_username:
        return (
            jsonify(
                {
                    "error": (
                        f"user '{request.username}' is not allowed to "
                        "update the completion."
                    )
                }
            ),
            400,
        )

    completion = deepcopy(request.json)
    new_completion = deepcopy(request.json)

    if completion_data.submitted_at and ["honeypot"] != list(
        completion.keys()
    ):
        return (
            jsonify(
                {
                    "error": (
                        "Completion is already submitted. Cannot "
                        "update submitted completion!"
                    )
                }
            ),
            400,
        )

    current_page = int(request.args.get("current_page", 1))
    task_in_db = Tasks.get_task(project_id, task_id)

    completion_in_db = Completions.get_completion(task_in_db.id)
    existing_completion = deepcopy(
        next(
            (
                c
                for c in completion_in_db.completions
                if c.get("id") == completion_id
            ),
            {},
        )
    )

    if project_is_visual_ner(user_project.label_config_line) and isinstance(
        task_in_db.data.get("image"), list
    ):
        completion_in_db = Completions.get_completion(task_in_db.id)
        for c in completion_in_db.completions:
            if c.get("id") == completion_id:
                results = c.get("result", [])
                new_result = []
                for result in results:
                    if current_page != result.get("pageNumber"):
                        new_result.append(result)
                if completion.get("result") or new_result:
                    completion["result"] = (
                        completion.get("result", []) + new_result
                    )

    completion.pop("state", None)  # remove editor state
    completion["id"] = int(completion_id)
    if not completion_data.submitted_at:
        completion["updated_at"] = datetime.now().isoformat() + "Z"
        completion["updated_by"] = request.username
    user_project.save_completion(task_id, completion, request.username)
    if existing_completion.get("result"):
        kwargs = {
            "updated_completion": {
                "old": [existing_completion],
                "new": [new_completion],
            }
        }
    else:
        kwargs = {"new_completion": [new_completion]}
    update_completions_meta_table(project_id, **kwargs)
    logger.debug(f"OUTPUT={request.json}")
    logger.info(f"TASK_ID={task_id} COMPLETION SAVED!")

    # Active learning
    al_automatic_model_training(project_id, project_name)

    # Remove output schema for current project, if exists
    Projectai_project.clear_derived_output_schema(project_name)
    return jsonify({"message": "Completion updated successfully."}), 201
