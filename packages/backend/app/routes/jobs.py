from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required

from ..services.job_monitor import job_monitor

bp = Blueprint("jobs", __name__)


@bp.get("/status")
@jwt_required()
def get_jobs_status():
    return jsonify(job_monitor.summary()), 200


@bp.get("/dead-letter")
@jwt_required()
def list_dead_letter_jobs():
    return jsonify(job_monitor.list_dead_letters()), 200


@bp.post("/dead-letter/<string:job_id>/retry")
@jwt_required()
def retry_dead_letter_job(job_id: str):
    ok, record = job_monitor.retry_dead_letter(job_id)
    if record is None:
        return jsonify(error="job not found"), 404
    status_code = 200 if ok else 502
    return jsonify(retried=ok, job=record.to_dict()), status_code
