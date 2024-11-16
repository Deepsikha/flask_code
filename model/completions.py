import operator
from sqlalchemy import func, distinct, or_
from sqlalchemy.orm import load_only
from sqlalchemy.dialects.postgresql import JSON, JSONB
from ai_project.db import db
from ai_project.models import tasks
from ai_project.models import tags as TAGS
from ai_project.models import user_projects
from sqlalchemy import cast, text
from lxml import etree


class Completions(db.Model):
    # primary key of tasks table
    id = db.Column(
        db.ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True
    )
    project_id = db.Column(
        db.ForeignKey("user_projects.project_id", ondelete="CASCADE"),
        nullable=False,
    )
    # completion_id and task_id (but not task.id) is same
    completion_id = db.Column(db.Integer, nullable=False)
    data = db.Column(JSON)
    title = db.Column(db.String(70), default="")
    completions = db.Column(JSON)
    predictions = db.Column(JSON)
    created_at = db.Column(
        db.DateTime(timezone=True), server_default=func.now()
    )
    created_by = db.Column(db.String(100), nullable=False)

    def __init__(
        self,
        completion_id,
        task_id,
        project_id,
        data,
        completions,
        predictions,
        created_by,
    ):
        self.completion_id = completion_id
        self.id = task_id
        self.project_id = project_id
        self.data = data
        self.completions = completions
        self.predictions = predictions
        self.created_by = created_by

    @classmethod
    def get_all_completions(cls):
        return cls.query.all()

    @classmethod
    def get_task_completion(cls, project_id: int, task_ids: list):
        """
        Get completions based on project id and task_ids
        return: [(id, created_ago, updated_at, created_username, updated_by),]
        """
        sub_query = (
            db.session.query(
                Completions.completion_id,
                func.json_array_elements(Completions.completions)
                .op("->>")("created_ago")
                .label("created_ago"),
                func.json_array_elements(Completions.completions)
                .op("->>")("updated_at")
                .label("updated_at"),
                func.json_array_elements(Completions.completions)
                .op("->>")("created_username")
                .label("created_username"),
                func.json_array_elements(Completions.completions)
                .op("->>")("updated_by")
                .label("updated_by"),
            )
            .filter(
                Completions.project_id == project_id,
                Completions.completion_id.in_(task_ids),
            )
            .subquery()
        )

        query = (
            db.session.query(sub_query)
            .with_entities(
                sub_query.c.completion_id,
                func.array_remove(
                    func.array_agg(distinct(sub_query.c.created_username)),
                    None,
                ).label("created_username"),
                func.array_remove(
                    func.array_agg(distinct(sub_query.c.updated_by)), None
                ).label("updated_by"),
                func.max(sub_query.c.created_ago).label("created_ago"),
                func.max(sub_query.c.updated_at).label("updated_at"),
            )
            .group_by(sub_query.c.completion_id)
            .all()
        )

        return query

    @classmethod
    def get_completion_result_detail(cls, project_id: int):
        """
        Get completions result details for output schema.
        :param project_id: Project ID
        return: [(from_name, to_name, type, value),]
        """
        sub_query = (
            db.session.query(
                func.jsonb_array_elements(
                    func.jsonb_array_elements(
                        func.cast(Completions.completions, JSONB)
                    ).op("->")("result")
                )
                .op("->")("from_name")
                .label("from_name"),
                func.jsonb_array_elements(
                    func.jsonb_array_elements(
                        func.cast(Completions.completions, JSONB)
                    ).op("->")("result")
                )
                .op("->")("to_name")
                .label("to_name"),
                func.jsonb_array_elements(
                    func.jsonb_array_elements(
                        func.cast(Completions.completions, JSONB)
                    ).op("->")("result")
                )
                .op("->")("type")
                .label("type"),
                func.jsonb_array_elements(
                    func.jsonb_array_elements(
                        func.cast(Completions.completions, JSONB)
                    ).op("->")("result")
                )
                .op("->")("value")
                .label("value"),
                func.json_array_elements(Completions.completions)
                .op("->>")("deleted_at")
                .label("deleted_at"),
            )
            .filter(
                Completions.project_id == project_id,
            )
            .subquery()
        )

        query = (
            cls.query.with_entities(
                sub_query.c.from_name,
                sub_query.c.to_name,
                sub_query.c.type,
                sub_query.c.value,
            )
            .filter(sub_query.c.deleted_at == None)
            .all()
        )

        return query

    def save(self):
        db.session.add(self)
        db.session.commit()

    @classmethod
    def get_project_completions(cls, project_id):
        return db.session.query(cls).filter_by(project_id=project_id).all()

    @classmethod
    def get_completions_count(cls, project_id):
        return (
            db.session.query(cls)
            .filter(
                cls.project_id == project_id,
                cls.completions.cast(db.String) != "[]",
            )
            .count()
        )

    @classmethod
    def get_completions(
        cls, project_id: int, tags: list, ground_truth: bool, fields: list
    ):
        query = db.session.query(Completions)

        if tags:
            query = query.join(
                tasks.TaggedTasks, Completions.id == tasks.TaggedTasks.task_pk
            ).filter(tasks.TaggedTasks.tag_id.in_(tags))

        if ground_truth:
            completion_subquery = (
                db.session.query(
                    Completions.completion_id,
                    func.json_array_elements(Completions.completions)
                    .op("->>")("honeypot")
                    .label("honeypot"),
                )
                .filter(Completions.project_id == project_id)
                .subquery()
            )
            query = query.filter(
                completion_subquery.c.honeypot == "true",
                completion_subquery.c.completion_id
                == Completions.completion_id,
            )

        return query.filter(Completions.project_id == project_id).all()

    @classmethod
    def get_al_completions_count(
        cls, project_id: int, tags: list, completions_filter: str
    ):
        completions_type = (
            "review_status"
            if completions_filter == "reviewed"
            else "submitted_at"
        )

        completion_subquery = (
            db.session.query(
                Completions.completion_id,
                Completions.id,
                Completions.project_id,
                func.json_array_elements(Completions.completions)
                .op("->>")("honeypot")
                .label("honeypot"),
                func.json_array_elements(Completions.completions)
                .op("->>")(completions_type)
                .label("completions_type"),
            )
            .filter(Completions.project_id == project_id)
            .subquery()
        )

        completion_query = (
            db.session.query(completion_subquery)
            .filter(
                completion_subquery.c.honeypot == "true",
                completion_subquery.c.completions_type != None,
            )
            .with_entities(
                completion_subquery.c.project_id,
                completion_subquery.c.completion_id,
                completion_subquery.c.id,
            )
        )
        if tags:
            subquery = db.session.query(TAGS.Tags.tag_id).filter(
                TAGS.Tags.project_id == project_id,
                TAGS.Tags.tag_name.in_(tags),
            )
            completion_query = completion_query.join(
                tasks.TaggedTasks,
                completion_subquery.c.id == tasks.TaggedTasks.task_pk,
            ).filter(tasks.TaggedTasks.tag_id.in_(subquery))

        return completion_query.filter(
            completion_subquery.c.project_id == project_id
        ).count()

    @classmethod
    def get_completion_owner_submitted_timestamp(
        cls, project_id: int, task_id: int, completion_id: int
    ):
        completion_subquery = (
            db.session.query(
                func.json_array_elements(Completions.completions)
                .op("->>")("id")
                .label("id"),
                func.json_array_elements(Completions.completions)
                .op("->>")("created_username")
                .label("created_username"),
                func.json_array_elements(Completions.completions)
                .op("->>")("submitted_at")
                .label("submitted_at"),
            )
            .filter(
                Completions.project_id == project_id,
                Completions.completion_id == task_id,
            )
            .subquery()
        )

        data = (
            db.session.query(Completions, completion_subquery)
            .filter(
                completion_subquery.c.id == str(completion_id),
            )
            .with_entities(
                completion_subquery.c.created_username,
                completion_subquery.c.submitted_at,
            )
            .first()
        )
        return data

    @classmethod
    def get_completion(cls, task_id):
        return cls.query.filter_by(id=task_id).first()

    @classmethod
    def get_completions_by_task_ids(
        cls, project_id: int, task_ids: list, fields=[]
    ):
        return (
            cls.query.options(load_only(*fields))
            .filter(
                Completions.project_id == project_id,
                Completions.completion_id.in_(task_ids),
            )
            .all()
        )

    @classmethod
    def update_completion(cls, task_id, data):
        db.session.query(cls).filter_by(id=task_id).update(data)
        db.session.commit()

    @classmethod
    def delete_completion(cls, task_id):
        db.session.query(cls).filter_by(id=task_id).delete()
        db.session.commit()

    @classmethod
    def get_completion_review_status(
        cls, project_id: int, task_id: int, completion_id: int
    ):
        """
        Get completion review status
        """
        completion_subquery = (
            db.session.query(
                func.json_array_elements(Completions.completions)
                .op("->>")("id")
                .label("id"),
                func.json_array_elements(Completions.completions)
                .op("->>")("review_status")
                .label("review_status"),
                func.json_array_elements(Completions.completions)
                .op("->>")("submitted_at")
                .label("submitted_at"),
            )
            .filter(
                Completions.project_id == project_id,
                Completions.completion_id == task_id,
            )
            .subquery()
        )

        review_status = (
            db.session.query(Completions, completion_subquery)
            .filter(
                completion_subquery.c.id == str(completion_id),
            )
            .with_entities(
                completion_subquery.c.review_status,
                completion_subquery.c.submitted_at,
            )
            .first()
        )
        return review_status

    @classmethod
    def get_user_completions(
        cls, project_id: int, task_id: int, username: str
    ):
        completion_subquery = (
            db.session.query(
                Completions.completions,
                func.json_array_elements(Completions.completions)
                .op("->>")("created_username")
                .label("created_username"),
                func.json_array_elements(Completions.completions)
                .op("->>")("updated_by")
                .label("updated_by"),
            )
            .filter(
                Completions.project_id == project_id,
                Completions.completion_id == task_id,
            )
            .subquery()
        )

        completions = (
            db.session.query(completion_subquery)
            .filter(
                or_(
                    completion_subquery.c.created_username == username,
                    completion_subquery.c.updated_by == username,
                ),
            )
            .with_entities(completion_subquery.c.completions)
            .first()
        )

        return completions

    #######################
    # Charts related method
    #######################

    @classmethod
    def get_completion_result_by_annotator_vner(
        cls,
        project_id: int,
        completion_ids: list = [],
        username: str = None,
        **kwargs,
    ):
        """
        Get completion result detail by annotator
        :For Visual NER project
        """

        filters = [Completions.project_id == project_id]

        if completion_ids:
            filters.append(Completions.completion_id.in_(list(completion_ids)))
        subquery = (
            Completions.query.with_entities(
                *cls.get_completions_result_fields(
                    is_completions=True, is_visual_ner=True
                )
            )
            .filter(*filters)
            .group_by(
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("deleted_at"),
                text("submitted_at"),
                text("taskid"),
                text("username"),
                text("x"),
                text("y"),
                text("width"),
                text("height"),
            )
            .order_by(text("label"), text("taskid"))
            .subquery()
        )

        filters = [
            subquery.c.deleted_at == None,
            subquery.c.honeypot == "true",
            subquery.c.submitted_at != None,
        ]

        if username:
            filters.append(subquery.c.username == username)
        fields = []
        if not username:
            fields = [subquery.c.username]

        return (
            db.session.query(subquery)
            .with_entities(
                subquery.c.label,
                subquery.c.chunk,
                subquery.c.taskid,
                subquery.c.x,
                subquery.c.y,
                subquery.c.width,
                subquery.c.height,
                *fields,
            )
            .filter(*filters)
            .group_by(
                text("username"),
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("submitted_at"),
                text("deleted_at"),
                text("taskid"),
                text("x"),
                text("y"),
                text("width"),
                text("height"),
            )
            .all()
        )

    @classmethod
    def get_completion_result_by_annotator(
        cls,
        project_id: int,
        completion_ids: list = [],
        username: str = None,
        is_assertion=None,
    ):
        """
        Get completion result detail by annotator
        """

        filters = [Completions.project_id == project_id]

        if completion_ids:
            filters.append(Completions.completion_id.in_(list(completion_ids)))
        subquery = (
            Completions.query.with_entities(
                *cls.get_completions_result_fields(is_completions=True)
            )
            .filter(*filters)
            .group_by(
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("deleted_at"),
                text("submitted_at"),
                text("taskid"),
                text("username"),
                text("start"),
                text("end_index"),
            )
            .order_by(text("label"), text("taskid"))
            .subquery()
        )

        filters = [
            subquery.c.deleted_at == None,
            subquery.c.honeypot == "true",
            subquery.c.submitted_at != None,
        ]

        project = user_projects.UserProjects.get_project_by_project_id(
            project_id, ["label_config"]
        )
        if is_assertion != None:
            condition_operator = operator.eq if is_assertion else operator.ne
            xmlTree = etree.fromstring(project.label_config)
            assertion_label = [
                element.get("value")
                for sh in xmlTree.iter("Labels")
                for element in sh.iter("Label")
                if condition_operator(element.get("assertion"), "true")
            ]
            filters.append(
                cast(subquery.c.label, JSONB).op("->>")(0).in_(assertion_label)
            )

        if username:
            filters.append(subquery.c.username == username)

        fields = [
            subquery.c.label,
            subquery.c.chunk,
            subquery.c.taskid,
            subquery.c.start,
            subquery.c.end_index,
        ]
        if not username:
            fields.append(subquery.c.username)

        return (
            db.session.query(subquery)
            .with_entities(*fields)
            .filter(*filters)
            .group_by(
                text("username"),
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("submitted_at"),
                text("deleted_at"),
                text("taskid"),
                text("start"),
                text("end_index"),
            )
            .all()
        )

    # prediction vs ground truth chart

    @classmethod
    def get_completions_result_fields(
        cls, is_completions: bool = True, is_visual_ner: bool = False
    ):
        """
        get completions result fields to extract from database
        """

        request_type = (
            Completions.completions
            if is_completions
            else Completions.predictions
        )
        values = (
            {
                "x_px": "x",
                "y_px": "y",
                "width_px": "width",
                "height_px": "height",
            }
            if is_visual_ner
            else {"start": "start", "end": "end_index"}
        )

        fields = [
            func.jsonb_array_elements(
                func.jsonb_array_elements(
                    func.jsonb_array_elements(cast(request_type, JSONB)).op(
                        "->"
                    )("result")
                )
                .op("->")("value")
                .op("->")(
                    func.jsonb_array_elements(
                        func.jsonb_array_elements(
                            cast(request_type, JSONB)
                        ).op("->")("result")
                    ).op("->>")("type")
                )
            ).label("label"),
            func.jsonb_array_elements(
                func.jsonb_array_elements(cast(request_type, JSONB)).op("->")(
                    "result"
                )
            )
            .op("->")("value")
            .op("->")("text")
            .op("->>")(0)
            .label("chunk"),
            func.jsonb_array_elements(cast(request_type, JSONB))
            .op("->>")("created_username")
            .label("username"),
            *[
                func.jsonb_array_elements(
                    func.jsonb_array_elements(cast(request_type, JSONB)).op(
                        "->"
                    )("result")
                )
                .op("->")("value")
                .op("->>")(key)
                .label(value)
                for key, value in values.items()
            ],
            Completions.completion_id.label("taskid"),
        ]

        if is_completions:
            fields = [
                func.jsonb_array_elements(cast(Completions.completions, JSONB))
                .op("->")("deleted_at")
                .label("deleted_at"),
                func.jsonb_array_elements(cast(Completions.completions, JSONB))
                .op("->>")("honeypot")
                .label("honeypot"),
                func.jsonb_array_elements(cast(Completions.completions, JSONB))
                .op("->")("submitted_at")
                .label("submitted_at"),
            ] + fields

        return fields

    @classmethod
    def get_completions_for_PVGT_vner(cls, project_id: int, task_ids):
        """
        Get completions for prediction vs ground truth chart
        :For Visual NER Project
        """
        completions_subquery = (
            Completions.query.with_entities(
                *cls.get_completions_result_fields(
                    is_completions=True, is_visual_ner=True
                )
            )
            .filter(
                Completions.project_id == project_id,
                Completions.completion_id.in_(list(task_ids)),
            )
            .group_by(
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("deleted_at"),
                text("submitted_at"),
                text("taskid"),
                text("username"),
                text("x"),
                text("y"),
                text("width"),
                text("height"),
            )
            .order_by(text("label"), text("taskid"))
            .subquery()
        )
        return (
            db.session.query(completions_subquery)
            .with_entities(
                completions_subquery.c.username,
                completions_subquery.c.label,
                completions_subquery.c.chunk,
                completions_subquery.c.taskid,
                completions_subquery.c.x,
                completions_subquery.c.y,
                completions_subquery.c.width,
                completions_subquery.c.height,
            )
            .filter(
                completions_subquery.c.deleted_at == None,
                completions_subquery.c.honeypot == "true",
                completions_subquery.c.submitted_at != None,
            )
            .group_by(
                text("username"),
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("submitted_at"),
                text("deleted_at"),
                text("taskid"),
                text("x"),
                text("y"),
                text("width"),
                text("height"),
            )
            .all()
        )

    @classmethod
    def get_predictions_for_PVGT_vner(cls, project_id):
        """
        Get predictions for prediction vs ground truth chart
        :For Visual NER Project
        """
        predictions_subquery = (
            Completions.query.with_entities(
                *cls.get_completions_result_fields(
                    is_completions=False, is_visual_ner=True
                )
            )
            .filter(Completions.project_id == project_id)
            .group_by(
                text("label"),
                text("chunk"),
                text("taskid"),
                text("x"),
                text("y"),
                text("width"),
                text("height"),
                text("username"),
            )
            .order_by(text("label"), text("taskid"))
            .subquery()
        )

        return (
            db.session.query(predictions_subquery)
            .with_entities(
                predictions_subquery.c.username,
                predictions_subquery.c.label,
                predictions_subquery.c.chunk,
                predictions_subquery.c.taskid,
                predictions_subquery.c.x,
                predictions_subquery.c.y,
                predictions_subquery.c.width,
                predictions_subquery.c.height,
            )
            .filter(Completions.project_id == project_id)
            .group_by(
                text("username"),
                text("label"),
                text("chunk"),
                text("taskid"),
                text("x"),
                text("y"),
                text("width"),
                text("height"),
            )
            .all()
        )

    @classmethod
    def get_completions_for_PVGT(cls, project_id: int, task_ids):
        """
        Get completions for prediction vs ground truth chart
        """
        completions_subquery = (
            Completions.query.with_entities(
                *cls.get_completions_result_fields(is_completions=True)
            )
            .filter(
                Completions.project_id == project_id,
                Completions.completion_id.in_(list(task_ids)),
            )
            .group_by(
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("deleted_at"),
                text("submitted_at"),
                text("taskid"),
                text("username"),
                text("start"),
                text("end_index"),
            )
            .order_by(text("label"), text("taskid"))
            .subquery()
        )
        return (
            db.session.query(completions_subquery)
            .with_entities(
                completions_subquery.c.username,
                completions_subquery.c.label,
                completions_subquery.c.chunk,
                completions_subquery.c.taskid,
                completions_subquery.c.start,
                completions_subquery.c.end_index,
            )
            .filter(
                completions_subquery.c.deleted_at == None,
                completions_subquery.c.honeypot == "true",
                completions_subquery.c.submitted_at != None,
            )
            .group_by(
                text("username"),
                text("label"),
                text("chunk"),
                text("honeypot"),
                text("submitted_at"),
                text("deleted_at"),
                text("taskid"),
                text("start"),
                text("end_index"),
            )
            .all()
        )

    @classmethod
    def get_predictions_for_PVGT(cls, project_id):
        """
        Get predictions for prediction vs ground truth chart
        """
        predictions_subquery = (
            Completions.query.with_entities(
                *cls.get_completions_result_fields(is_completions=False)
            )
            .filter(Completions.project_id == project_id)
            .group_by(
                text("label"),
                text("chunk"),
                text("taskid"),
                text("start"),
                text("end_index"),
                text("username"),
            )
            .order_by(text("label"), text("taskid"))
            .subquery()
        )

        return (
            db.session.query(predictions_subquery)
            .with_entities(
                predictions_subquery.c.username,
                predictions_subquery.c.label,
                predictions_subquery.c.chunk,
                predictions_subquery.c.taskid,
                predictions_subquery.c.start,
                predictions_subquery.c.end_index,
            )
            .filter(Completions.project_id == project_id)
            .group_by(
                text("username"),
                text("label"),
                text("chunk"),
                text("taskid"),
                text("start"),
                text("end_index"),
            )
            .all()
        )

    @classmethod
    def get_completion_for_CEBA(cls, project_id, is_visual_ner: bool = False):
        """
        Get completions for chunk extracted by annotator chart
        For: chunk_extracted_by_label, chunk_extracted_by_annotator chart
        """

        group_by = [
            text("label"),
            text("username"),
            text("chunk"),
            text("honeypot"),
            text("submitted_at"),
            text("deleted_at"),
        ]

        group_by.extend(
            [text("x"), text("y"), text("width"), text("height")]
            if is_visual_ner
            else [text("start"), text("end_index")]
        )

        subquery = (
            Completions.query.with_entities(
                *cls.get_completions_result_fields(
                    is_visual_ner=is_visual_ner
                )[:-1]
            )
            .filter(Completions.project_id == project_id)
            .group_by(*group_by)
            .order_by(text("label"))
            .subquery()
        )

        return (
            db.session.query(subquery)
            .with_entities(
                subquery.c.username,
                subquery.c.chunk,
                subquery.c.label,
            )
            .filter(
                subquery.c.deleted_at == None,
                subquery.c.honeypot == "true",
                subquery.c.submitted_at != None,
            )
            .group_by(*group_by)
            .all()
        )


# Used in ALAB <= v.2.5.0
class CompletionsResultView(db.Model):
    project_id = db.Column(db.Integer, primary_key=True)
    from_name = db.Column(db.String)
    to_name = db.Column(db.String)
    config_type = db.Column(db.String)
    config_value = db.Column(JSONB)

    @classmethod
    def get_completion_result_detail(cls, project_id):
        return (
            db.session.query(cls)
            .with_entities(
                cls.from_name, cls.to_name, cls.config_type, cls.config_value
            )
            .filter(cls.project_id == project_id)
            .all()
        )


# Used in ALAB > v.2.5.0
class CompletionsMeta(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(
        db.ForeignKey("user_projects.project_id", ondelete="CASCADE"),
        nullable=False,
    )
    from_name_to_name_type = db.Column(JSONB, nullable=False)
    used_labels_info = db.Column(JSONB)

    def __init__(self, project_id, from_name_to_name_type):
        self.project_id = project_id
        self.from_name_to_name_type = from_name_to_name_type

    def save(self):
        db.session.add(self)
        db.session.commit()

    @classmethod
    def update(cls, project_id: int, data: dict):
        db.session.query(cls).filter(cls.project_id == project_id).update(data)
        db.session.commit()

    @classmethod
    def read(cls, project_id: int):
        return cls.query.filter_by(project_id=project_id).first()

    @classmethod
    def delete(cls, project_id: int):
        db.session.query(cls).filter(cls.project_id == project_id).delete()
        db.session.commit()
