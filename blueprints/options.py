# blueprints/options.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from services.attribute_service import get_attribute, list_attributes
from services.option_service import (
    list_options, create_option, get_option, update_option, delete_option,
    ensure_root_option
)

bp_options = Blueprint("options", __name__, url_prefix="/options")

def build_tree_with_root(rows, root_id):
    """
    返回 root 节点（包含完整 children 树）。
    rows: attribute_option 的所有行（同一 attribute_id）
    """
    nodes = {r["id"]: {**r, "children": []} for r in rows}
    # 先将所有节点挂到父节点
    for r in rows:
        pid = r["parent_id"]
        if pid and pid in nodes:
            nodes[pid]["children"].append(nodes[r["id"]])
    # 排序
    def sort_rec(n):
        n["children"].sort(key=lambda x: (x["sort_order"] if x["sort_order"] is not None else 0, x["id"]))
        for c in n["children"]:
            sort_rec(c)
    # 找到 root
    root = nodes.get(root_id)
    if root:
        sort_rec(root)
    return root

@bp_options.route("/select", methods=["GET"])
def select_attribute():
    scope = request.args.get("scope")
    attrs = list_attributes(scope=scope)
    return render_template("options_select.html", attrs=attrs, scope=scope)

@bp_options.route("/<int:attribute_id>", methods=["GET","POST"])
def manage_options(attribute_id):
    attr = get_attribute(attribute_id)
    if not attr:
        flash("属性不存在", "err")
        return redirect(url_for("options.select_attribute"))

    # 确保该属性已有根节点
    root = ensure_root_option(attribute_id, attr_name=attr["name"])

    if request.method == "POST":
        action = request.form.get("action")
        if action == "create":
            name = (request.form.get("name") or "").strip()
            code = (request.form.get("code") or None)
            parent_id = request.form.get("parent_id") or None
            sort_order = int(request.form.get("sort_order") or 0)
            if not name:
                flash("选项名称必填", "err")
            else:
                try:
                    pid = int(parent_id) if parent_id else None
                except:
                    pid = None
                try:
                    create_option(attribute_id, name, code, pid, sort_order)
                    flash("已新增选项", "ok")
                except Exception as e:
                    flash(f"新增失败：{e}", "err")

        elif action == "update":
            opt_id = int(request.form.get("opt_id"))
            name = (request.form.get("name") or "").strip()
            code = (request.form.get("code") or None)
            parent_id = request.form.get("parent_id") or None
            sort_order = int(request.form.get("sort_order") or 0)
            if not name:
                flash("名称必填", "err")
            else:
                try:
                    pid = int(parent_id) if parent_id else None
                except:
                    pid = None
                try:
                    update_option(opt_id, name, code, pid, sort_order)
                    flash("已更新", "ok")
                except Exception as e:
                    flash(f"保存失败：{e}", "err")

        elif action == "delete":
            opt_id = int(request.form.get("opt_id"))
            if opt_id == root["id"]:
                flash("禁止删除属性根节点", "err")
            else:
                try:
                    delete_option(opt_id)
                    flash("已删除（如有子选项亦被删除）", "ok")
                except Exception as e:
                    flash(f"删除失败：{e}", "err")

        return redirect(url_for("options.manage_options", attribute_id=attribute_id))

    # GET: 查询所有行并构建以 root 为根的树
    rows = list_options(attribute_id)
    tree_root = build_tree_with_root(rows, root["id"])
    return render_template("options_manage.html", attr=attr, rows=rows, root=root, tree_root=tree_root)
