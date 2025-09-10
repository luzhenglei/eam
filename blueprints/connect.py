# blueprints/connect.py
# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify
from typing import Optional
from db import get_conn
from services.link_service import (
    create_link, find_candidates, toggle_port,
    get_device_ports_for_connect_ui, delete_link
)


connect_bp = Blueprint('connect_bp', __name__, url_prefix='/connect')

@connect_bp.post('/create')
def api_create_link():
    data = request.get_json(force=True)
    project_id   = int(data.get('project_id'))
    from_port_id = int(data.get('from_port_id'))
    to_port_id   = int(data.get('to_port_id'))
    try:
        link_id = create_link(project_id, from_port_id, to_port_id)
        return jsonify({"ok": True, "link_id": link_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

# blueprints/connect.py 片段
@connect_bp.get('/<int:pid>/candidates')
def api_candidates(pid: Optional[int] = None):
    a_dev = request.args.get('a', type=int)
    b_dev = request.args.get('b', type=int)
    if a_dev and b_dev:
        try:
            with get_conn() as conn, conn.cursor() as cur:
                def fetch_ports(dev_id: int):
                    cur.execute("""
                        SELECT
                            p.id   AS port_id,
                            p.name,
                            p.port_type_id,
                            p.port_template_id,          -- ★ 加上模板ID用于属性匹配
                            tpl.name AS attr_name,       -- ★ 返回属性名
                            p.max_links,
                            p.is_active
                        FROM port p
                        LEFT JOIN port_template tpl ON tpl.id = p.port_template_id
                        WHERE p.device_id=%s
                        ORDER BY p.name
                    """, (dev_id,))
                    rows = cur.fetchall() or []
                    # 计算占用：达到上限 或 已关闭
                    for r in rows:
                        cur.execute("""
                            SELECT COUNT(*) AS c FROM link
                            WHERE a_port_id=%s OR b_port_id=%s
                        """, (r['port_id'], r['port_id']))
                        c = (cur.fetchone() or {}).get('c', 0)
                        occupied = (int(c) >= int(r.get('max_links') or 0)) or (int(r.get('is_active') or 1) == 0)
                        r['occupied'] = bool(occupied)
                    return rows

                left  = fetch_ports(a_dev)
                right = fetch_ports(b_dev)
            return jsonify({"ok": True, "data": {"left": left, "right": right}})
        except Exception as e:
            return jsonify({"ok": False, "err": str(e)}), 500

    # ……新模式分支保持不变（如果你在用旧模板，就不会走这里）
    ...
  
@connect_bp.post('/<int:pid>/make_link')
def api_make_link(pid: int):
    """
    旧模板 POST：a_port_id, b_port_id (form-urlencoded)
    """
    a_port_id = request.form.get('a_port_id', type=int)
    b_port_id = request.form.get('b_port_id', type=int)
    if not a_port_id or not b_port_id:
        return jsonify({"ok": False, "err": "缺少端口参数"}), 400
    try:
        link_id = create_link(pid, a_port_id, b_port_id)
        return jsonify({"ok": True, "link_id": link_id})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)}), 400

@connect_bp.get('/<int:pid>/devices/<int:did>/ports')
def api_device_ports(pid: int, did: int):
    """
    返回该设备的端口与当前连接：
    - 字段包含：port_id, name, is_active, max_links, conn_count, links（数组）
    - 为兼容旧模板：若 links 只有 1 条，则补充 link_id / target_device_name / target_port_name 便于旧逻辑直接显示
    """
    try:
        rows = get_device_ports_for_connect_ui(pid, did)  # 来自 services.link_service
        # 兼容旧模板的简化字段
        for r in rows:
            if r.get('links'):
                L0 = r['links'][0]
                r['link_id'] = L0.get('link_id') or L0.get('id')
                r['target_device_name'] = L0.get('other_device_name')
                r['target_port_name']   = L0.get('other_port_name')
        return jsonify({"ok": True, "data": rows})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)}), 500

@connect_bp.post('/<int:pid>/links/<int:link_id>/delete')
def api_delete_link(pid: int, link_id: int):
    try:
        n = delete_link(link_id)
        if n <= 0:
            return jsonify({"ok": False, "err": "link 不存在"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)}), 400
    

@connect_bp.post('/ports/<int:port_id>/toggle')
def api_toggle_port(port_id: int):
    try:
        new_val = toggle_port(port_id)
        return jsonify({"ok": True, "is_active": new_val})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400
   
    
@connect_bp.get('/<int:pid>/connect')
def connect_page(pid: int):
    """连接配置页面：供 url_for('connect_bp.connect_page', pid=...) 使用"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, name FROM project WHERE id=%s", (pid,))
        project = cur.fetchone()
    return render_template('connect_config.html', project=project)

@connect_bp.get('/<int:pid>/search_devices')
def api_search_devices(pid: int):
    """
    GET 参数:
      - q: 关键字，可为空；按设备名称模糊匹配
    返回:
      { "ok": True, "data": [ { "id": ..., "name": ..., "model_code": Optional[str] }, ... ] }
    说明:
      - 模板/前端会用到 d.name 与 (可选) d.model_code；我们即使不提供 model_code，前端也会用 '' 兜底
    """
    q = (request.args.get('q') or '').strip()
    like = f"%{q}%"
    rows = []
    try:
        with get_conn() as conn, conn.cursor() as cur:
            if q:
                # 只查 id/name，避免你的库里没有 model_code 字段时报错
                cur.execute("""
                    SELECT id, name
                    FROM device
                    WHERE project_id=%s AND name LIKE %s
                    ORDER BY name
                    LIMIT 200
                """, (pid, like))
            else:
                cur.execute("""
                    SELECT id, name
                    FROM device
                    WHERE project_id=%s
                    ORDER BY name
                    LIMIT 200
                """, (pid,))
            rows = cur.fetchall() or []

        # 兼容前端模板里的 d.model_code || '' 写法：我们不强制返回该字段
        return jsonify({"ok": True, "data": rows})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)}), 500