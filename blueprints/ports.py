from flask import Blueprint, request, jsonify
from services.port_service import update_port_active

bp_ports = Blueprint("ports_bp", __name__, url_prefix="/projects/<int:pid>/ports")


@bp_ports.route("/<int:port_id>/active", methods=["PATCH"])
def update_active(pid, port_id):
    data = request.get_json(silent=True) or {}
    is_active = data.get("is_active")
    if is_active is None:
        return jsonify({"ok": False, "msg": "is_active required"}), 400
    try:
        update_port_active(pid, port_id, bool(int(is_active)))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
