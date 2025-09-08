# services/link_service.py
from typing import Any, Dict, List, Set

from db import get_conn  # 数据库连接工具


# ================== 工具函数 ==================

def _is_port_occupied(project_id: int, port_id: int) -> bool:
    """判断端口在该项目下是否已被占用。"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM link
            WHERE project_id=%s AND status='CONNECTED'
              AND (a_port_id=%s OR b_port_id=%s)
            """,
            (project_id, port_id, port_id),
        )
        c = (cur.fetchone() or {}).get("c", 0)
        return int(c) > 0


# ================== 端口基础操作 ==================

def list_ports_for_device(project_id: int, device_id: int) -> List[Dict[str, Any]]:
    """返回设备下所有端口及其占用/属性信息，供前端分组折叠。"""
    sql = """
        SELECT p.id AS port_id, p.name, p.port_type_id, p.is_active,
               pt.name AS attr_name, t.name AS port_type_name
        FROM port p
        LEFT JOIN port_template pt ON pt.id = p.port_template_id
        LEFT JOIN port_type t ON t.id = p.port_type_id
        JOIN device d ON d.id = p.device_id
        WHERE d.project_id=%s AND d.id=%s
        ORDER BY t.name, pt.name, p.index_no, p.id
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (project_id, device_id))
        rows = cur.fetchall() or []

    items: List[Dict[str, Any]] = []
    for r in rows:
        pid = int(r["port_id"])
        items.append({**r, "occupied": _is_port_occupied(project_id, pid)})
    return items


def find_matching_ports(project_id: int, src_port_id: int, target_device_id: int) -> List[Dict[str, Any]]:
    """给定源端口和目标设备，返回可连接的目标端口列表。"""
    with get_conn() as conn, conn.cursor() as cur:
        # 读取源端口的类型及属性
        cur.execute(
            """
            SELECT p.port_type_id, pt.name AS attr_name
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            WHERE p.id=%s
            """,
            (src_port_id,),
        )
        src = cur.fetchone()
        if not src:
            return []

        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, p.is_active,
                   pt.name AS attr_name, t.name AS port_type_name
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            LEFT JOIN port_type t ON t.id = p.port_type_id
            JOIN device d ON d.id = p.device_id
            WHERE d.project_id=%s AND d.id=%s AND p.is_active=1
                  AND p.port_type_id=%s AND COALESCE(pt.name,'')=COALESCE(%s,'')
            ORDER BY p.index_no, p.id
            """,
            (project_id, target_device_id, src["port_type_id"], src.get("attr_name")),
        )
        rows = cur.fetchall() or []

    result: List[Dict[str, Any]] = []
    for r in rows:
        pid = int(r["port_id"])
        if _is_port_occupied(project_id, pid):
            continue
        result.append(r)
    return result


def update_port_active(project_id: int, port_id: int, is_active: bool) -> bool:
    """切换端口开关状态。若端口已连线且要关闭则报错。"""
    if not is_active and _is_port_occupied(project_id, port_id):
        raise ValueError("端口已连线，无法关闭")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE port SET is_active=%s WHERE id=%s",
            (1 if is_active else 0, port_id),
        )
        conn.commit()
        return cur.rowcount > 0


# ================== 候选端口 ==================

def find_candidates(project_id: int, device_a_id: int, device_b_id: int) -> Dict[str, List[Dict[str, Any]]]:
    """查找两台设备可配对的候选端口。"""
    if device_a_id == device_b_id:
        return {"left": [], "right": []}

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, p.is_active,
                   pt.name AS attr_name, tpt.name AS port_type_name
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            LEFT JOIN port_type tpt ON tpt.id = p.port_type_id
            JOIN device d ON d.id = p.device_id
            WHERE d.project_id=%s AND d.id=%s AND p.is_active=1
            """,
            (project_id, device_a_id),
        )
        left_rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, p.is_active,
                   pt.name AS attr_name, tpt.name AS port_type_name
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            LEFT JOIN port_type tpt ON tpt.id = p.port_type_id
            JOIN device d ON d.id = p.device_id
            WHERE d.project_id=%s AND d.id=%s AND p.is_active=1
            """,
            (project_id, device_b_id),
        )
        right_rows = cur.fetchall() or []

    def enrich(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows:
            pid = int(r["port_id"])
            out.append({**r, "occupied": _is_port_occupied(project_id, pid)})
        return out

    left = enrich(left_rows)
    right = enrich(right_rows)

    index_right: Dict[tuple, List[Dict[str, Any]]] = {}
    for r in right:
        if r["occupied"]:
            continue
        key = (r["port_type_id"], r.get("attr_name") or "")
        index_right.setdefault(key, []).append(r)

    left_filtered: List[Dict[str, Any]] = []
    right_allowed: Set[int] = set()
    for L in left:
        if L["occupied"]:
            continue
        keyL = (L["port_type_id"], L.get("attr_name") or "")
        if keyL in index_right and index_right[keyL]:
            left_filtered.append(L)
            for R in index_right[keyL]:
                right_allowed.add(int(R["port_id"]))

    right_filtered = [r for r in right if int(r["port_id"]) in right_allowed]
    return {"left": left_filtered, "right": right_filtered}


# ================== 建立/删除连接 ==================

def create_link(project_id: int, a_port_id: int, b_port_id: int, status: str = "CONNECTED") -> int:
    """建立连接，校验类型/属性一致且端口可用。"""
    if a_port_id == b_port_id:
        raise ValueError("不能将同一端口两端相连")

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, p.is_active, pt.name AS rule_attr_name,
                   d.id AS device_id, d.project_id
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            JOIN device d ON d.id = p.device_id
            WHERE p.id=%s
            """,
            (a_port_id,),
        )
        a = cur.fetchone()
        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, p.is_active, pt.name AS rule_attr_name,
                   d.id AS device_id, d.project_id
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            JOIN device d ON d.id = p.device_id
            WHERE p.id=%s
            """,
            (b_port_id,),
        )
        b = cur.fetchone()

    if not a or not b:
        raise ValueError("端口不存在")
    if not a.get("is_active") or not b.get("is_active"):
        raise ValueError("端口已关闭，不能连线")
    if int(a["device_id"]) == int(b["device_id"]):
        raise ValueError("不能连接同一台设备上的两个端口")
    if int(a["project_id"]) != project_id or int(b["project_id"]) != project_id:
        raise ValueError("项目不匹配")
    if int(a["port_type_id"] or 0) != int(b["port_type_id"] or 0):
        raise ValueError("端口类型不匹配")
    if (a.get("rule_attr_name") or "") != (b.get("rule_attr_name") or ""):
        raise ValueError("端口属性不匹配")
    if _is_port_occupied(project_id, a_port_id) or _is_port_occupied(project_id, b_port_id):
        raise ValueError("端口已被占用")

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO link (project_id, a_port_id, b_port_id, a_device_id, b_device_id, status, created_at)
            VALUES (%s,%s,%s,
                    (SELECT device_id FROM port WHERE id=%s),
                    (SELECT device_id FROM port WHERE id=%s),
                    %s, NOW())
            """,
            (project_id, a_port_id, b_port_id, a_port_id, b_port_id, status),
        )
        conn.commit()
        return int(cur.lastrowid)


def delete_link(project_id: int, link_id: int) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM link WHERE id=%s AND project_id=%s", (link_id, project_id))
        conn.commit()
        return cur.rowcount > 0


# ================== 查询 ==================

def list_links_in_project(project_id: int) -> List[Dict[str, Any]]:
    """返回项目中已建立的连接列表。"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT l.id, l.status, l.remark, l.created_at,
                   la.id AS a_port_id, la.name AS a_port_name,
                   da.id AS a_device_id, da.name AS a_device_name,
                   lb.id AS b_port_id, lb.name AS b_port_name,
                   db.id AS b_device_id, db.name AS b_device_name
            FROM link l
            JOIN port la ON la.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            JOIN port lb ON lb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            WHERE l.project_id=%s
            ORDER BY l.id DESC
            """,
            (project_id,),
        )
        return cur.fetchall() or []


def list_cables_paginated(project_id: int, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
    """分页返回线缆清册。"""
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 50), 200))
    offset = (page - 1) * page_size

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM link WHERE project_id=%s AND status='CONNECTED'",
            (project_id,),
        )
        total = (cur.fetchone() or {}).get("c", 0)

        cur.execute(
            """
            SELECT l.id AS link_id, l.status, l.printed, l.printed_at,
                   da.id AS a_device_id, da.name AS a_device_name,
                   pa.id AS a_port_id, pa.name AS a_port_name,
                   ta.id AS a_port_type_id, ta.name AS a_port_type_name,
                   db.id AS b_device_id, db.name AS b_device_name,
                   pb.id AS b_port_id, pb.name AS b_port_name,
                   tb.id AS b_port_type_id, tb.name AS b_port_type_name
            FROM link l
            JOIN port pa ON pa.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            LEFT JOIN port_type ta ON ta.id = pa.port_type_id
            JOIN port pb ON pb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            LEFT JOIN port_type tb ON tb.id = pb.port_type_id
            WHERE l.project_id=%s AND l.status='CONNECTED'
            ORDER BY da.name ASC, ta.name ASC, pa.name ASC
            LIMIT %s OFFSET %s
            """,
            (project_id, page_size, offset),
        )
        rows = cur.fetchall() or []

    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append({**r, "a_dir": "", "b_dir": ""})
    return {"total": int(total or 0), "page": page, "page_size": page_size, "items": items}


def fetch_cables_by_ids(project_id: int, link_ids: List[int]) -> List[Dict[str, Any]]:
    if not link_ids:
        return []
    placeholders = ",".join(["%s"] * len(link_ids))
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT l.id AS link_id, l.status, l.printed, l.printed_at,
                   da.name AS a_device_name, pa.name AS a_port_name, ta.name AS a_port_type_name,
                   db.name AS b_device_name, pb.name AS b_port_name, tb.name AS b_port_type_name
            FROM link l
            JOIN port pa ON pa.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            LEFT JOIN port_type ta ON ta.id = pa.port_type_id
            JOIN port pb ON pb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            LEFT JOIN port_type tb ON tb.id = pb.port_type_id
            WHERE l.project_id=%s AND l.status='CONNECTED' AND l.id IN ({placeholders})
            """,
            [project_id] + link_ids,
        )
        rows = cur.fetchall() or []
    for r in rows:
        r["a_dir"] = ""
        r["b_dir"] = ""
    return rows


def fetch_all_cables(project_id: int) -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT l.id AS link_id, l.status, l.printed, l.printed_at,
                   da.name AS a_device_name, pa.name AS a_port_name, ta.name AS a_port_type_name,
                   db.name AS b_device_name, pb.name AS b_port_name, tb.name AS b_port_type_name
            FROM link l
            JOIN port pa ON pa.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            LEFT JOIN port_type ta ON ta.id = pa.port_type_id
            JOIN port pb ON pb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            LEFT JOIN port_type tb ON tb.id = pb.port_type_id
            WHERE l.project_id=%s AND l.status='CONNECTED'
            ORDER BY da.name ASC, ta.name ASC, pa.name ASC
            """,
            (project_id,),
        )
        rows = cur.fetchall() or []
    for r in rows:
        r["a_dir"] = ""
        r["b_dir"] = ""
    return rows


def mark_links_printed(project_id: int, link_ids: List[int]) -> int:
    if not link_ids:
        return 0
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE link SET printed=1, printed_at=NOW() WHERE project_id=%s AND id IN ({','.join(['%s']*len(link_ids))})",
            [project_id] + link_ids,
        )
        conn.commit()
        return int(cur.rowcount or 0)
