# blueprints/projects.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from services.project_service import list_projects, get_project, create_project, update_project, delete_project
from services.device_service import list_devices_by_project, create_device_in_project, get_device, update_device_basic,delete_device, get_device
from services.template_service import list_templates


bp_projects = Blueprint("projects_bp", __name__, url_prefix="/projects")

@bp_projects.route("/", methods=["GET"])
def project_list():
    rows = list_projects()
    return render_template("project_list.html", rows=rows)

@bp_projects.route("/new", methods=["GET","POST"])
def project_new():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        remark = (request.form.get("remark") or "").strip()
        try:
            create_project(name, remark)
            flash("已创建项目", "ok")
            return redirect(url_for("projects_bp.project_list"))
        except Exception as e:
            flash(f"创建失败：{e}", "err")
    return render_template("project_form.html")

@bp_projects.route("/<int:pid>", methods=["GET"])
def project_detail(pid):
    p = get_project(pid)
    if not p:
        flash("项目不存在", "err")
        return redirect(url_for("projects_bp.project_list"))
    devices = list_devices_by_project(pid)
    return render_template("project_detail.html", project=p, devices=devices)

@bp_projects.route("/<int:pid>/delete", methods=["POST"])
def project_delete(pid):
    try:
        delete_project(pid)
        flash("已删除项目", "ok")
    except Exception as e:
        flash(f"删除失败：{e}", "err")
    return redirect(url_for("projects_bp.project_list"))

# ========== 项目内设备（迁移设备入口到项目下） ==========
@bp_projects.route("/<int:pid>/devices/new", methods=["GET","POST"])
def device_new_in_project(pid):
    p = get_project(pid)
    if not p:
        flash("项目不存在", "err")
        return redirect(url_for("projects_bp.project_list"))

    templates = list_templates()  # 用于选择模板
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()         # 设备编号
        model_code = (request.form.get("model_code") or "").strip()  # 设备型号
        template_id = request.form.get("template_id", type=int)
        if not name or not model_code or not template_id:
            flash("设备编号、设备型号、模板必填", "err")
        else:
            try:
                did = create_device_in_project(pid, template_id, name, model_code)
                flash("已创建设备", "ok")
                return redirect(url_for("projects_bp.project_detail", pid=pid))
            except Exception as e:
                flash(f"创建失败：{e}", "err")
    return render_template("device_form_in_project.html", project=p, templates=templates)

@bp_projects.route("/<int:pid>/devices/<int:device_id>/edit", methods=["GET","POST"])
def device_edit_in_project(pid, device_id):
    p = get_project(pid)
    if not p:
        flash("项目不存在", "err")
        return redirect(url_for("projects_bp.project_list"))

    device = get_device(device_id)
    if not device or device.get("project_id") != pid:
        flash("设备不存在于该项目", "err")
        return redirect(url_for("projects_bp.project_detail", pid=pid))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        model_code = (request.form.get("model_code") or "").strip()
        try:
            update_device_basic(device_id, name, model_code, None)  # 不在此处变更模板
            flash("已保存修改", "ok")
            return redirect(url_for("projects_bp.project_detail", pid=pid))
        except Exception as e:
            flash(f"保存失败：{e}", "err")

    return render_template("device_form_in_project.html", project=p, device=device, templates=None)


@bp_projects.route("/<int:pid>/devices/<int:device_id>/delete", methods=["POST"])
def device_delete_in_project(pid, device_id):
    dev = get_device(device_id)
    if not dev or dev.get("project_id") != pid:
        flash("设备不存在于该项目", "err")
        return redirect(url_for("projects_bp.project_detail", pid=pid))
    try:
        delete_device(device_id)
        flash("已删除设备", "ok")
    except Exception as e:
        flash(f"删除失败：{e}", "err")
    return redirect(url_for("projects_bp.project_detail", pid=pid))
