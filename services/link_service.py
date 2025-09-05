# services/link_service.py
from typing import Any, Dict, List, Optional, Tuple, Set

from db import get_conn  # 你的数据库连接工具

VALID_DIRECTIONS = {"FROM", "TO"}


# ============== 基础工具 ==============

def _get_direction_attr_id() -> Optional[int]:
    """获取端口方向属性ID（scope='port', code='PORT_DIRECTION'）。没有则返回 None。"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM attribute_def WHERE scope='port' AND code='PORT_DIRECTION' LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row["id"])


def _get_port_direction(port_id: int) -> Optional[str]:
    """
    读取某端口的方向（FROM/TO）。若未配置或不存在则返回 None。
    同时支持“枚举(option_id→attribute_option.name)”或“文本(value_text)”。
    """
    attr_id = _get_direction_attr_id()
    if not attr_id:
        return None
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT UPPER(COALESCE(ao.name, pav.value_text)) AS v
            FROM port_attr_value pav
            LEFT JOIN attribute_option ao ON ao.id = pav.option_id
            WHERE pav.port_id=%s AND pav.attribute_id=%s
            LIMIT 1
            """,
            (port_id, attr_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        v = (row["v"] or "").strip().upper()
        return v if v in VALID_DIRECTIONS else None


def _is_port_occupied(project_id: int, port_id: int) -> bool:
    """端口是否已被占用（该项目下是否在 link 表中作为 a/b 端出现）。"""
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


def _direction_required() -> bool:
    """库里有 PORT_DIRECTION 定义才启用方向规则；否则不检查方向。"""
    return _get_direction_attr_id() is not None


# ============== 候选端口 / 建立/删除连接 / 列表 ==============

def find_candidates(project_id: int, device_a_id: int, device_b_id: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    查找 A/B 设备可配对的候选端口。
    匹配条件：
      - 不同设备，且同项目
      - 端口类型相同（port_type_id）
      - 端口规则的“属性名”相同（通过 port_template_id -> port_template.name 拿到）
      - 若定义了方向属性，则方向互补（FROM<->TO）
      - 两侧端口均未被占用
    返回：{"left": [...], "right":[...]}
    """
    if device_a_id == device_b_id:
        return {"left": [], "right": []}

    with get_conn() as conn, conn.cursor() as cur:
        # A 侧端口
        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, pt.name AS attr_name, tpt.name AS port_type_name
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            LEFT JOIN port_type tpt ON tpt.id = p.port_type_id
            JOIN device d ON d.id = p.device_id
            WHERE d.project_id=%s AND d.id=%s
            """,
            (project_id, device_a_id),
        )
        left_rows = cur.fetchall() or []

        # B 侧端口
        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, pt.name AS attr_name, tpt.name AS port_type_name
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            LEFT JOIN port_type tpt ON tpt.id = p.port_type_id
            JOIN device d ON d.id = p.device_id
            WHERE d.project_id=%s AND d.id=%s
            """,
            (project_id, device_b_id),
        )
        right_rows = cur.fetchall() or []

    need_dir = _direction_required()

    def enrich(port_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in port_rows:
            pid = int(r["port_id"])
            dirv = _get_port_direction(pid) if need_dir else None
            occ = _is_port_occupied(project_id, pid)
            out.append(
                {
                    **r,
                    "direction": dirv,
                    "occupied": occ,
                }
            )
        return out

    left = enrich(left_rows)
    right = enrich(right_rows)

    # 右侧索引
    index_right: Dict[Tuple[Any, Any, Optional[str]], List[Dict[str, Any]]] = {}
    for r in right:
        if r["occupied"]:
            continue
        key: Tuple[Any, Any, Optional[str]] = (r["port_type_id"], r.get("attr_name") or "", None)
        if need_dir:
            if not r["direction"]:
                continue
            key = (r["port_type_id"], r.get("attr_name") or "", r["direction"])
        index_right.setdefault(key, []).append(r)

    # 左侧过滤并收集右侧允许集合
    left_filtered: List[Dict[str, Any]] = []
    right_allowed_ids: Set[int] = set()
    for L in left:
        if L["occupied"]:
            continue
        if need_dir:
            dirL = L.get("direction")
            if dirL not in VALID_DIRECTIONS:
                continue
            dir_need = "TO" if dirL == "FROM" else "FROM"
            keyL = (L["port_type_id"], L.get("attr_name") or "", dir_need)
        else:
            keyL = (L["port_type_id"], L.get("attr_name") or "", None)
        if keyL in index_right and index_right[keyL]:
            left_filtered.append(L)
            for R in index_right[keyL]:
                right_allowed_ids.add(int(R["port_id"]))

    right_filtered = [r for r in right if int(r["port_id"]) in right_allowed_ids]
    return {"left": left_filtered, "right": right_filtered}


def create_link(project_id: int, a_port_id: int, b_port_id: int, status: str = "CONNECTED") -> int:
    """
    建立连接：校验类型/属性（规则名）一致；若启用方向则互补；端口未占用。
    返回新建 link.id
    """
    if a_port_id == b_port_id:
        raise ValueError("不能将同一端口两端相连")

    # 取端口与设备/类型/规则名
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id, pt.name AS rule_attr_name,
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
            SELECT p.id AS port_id, p.name, p.port_type_id, pt.name AS rule_attr_name,
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
    if int(a["device_id"]) == int(b["device_id"]):
        raise ValueError("不能连接同一台设备上的两个端口")
    if int(a["project_id"]) != project_id or int(b["project_id"]) != project_id:
        raise ValueError("项目不匹配")
    if int(a["port_type_id"] or 0) != int(b["port_type_id"] or 0):
        raise ValueError("端口类型不匹配")
    if (a.get("rule_attr_name") or "") != (b.get("rule_attr_name") or ""):
        raise ValueError("端口属性不匹配")

    # 方向（可选）
    if _direction_required():
        da = _get_port_direction(a_port_id)
        db = _get_port_direction(b_port_id)
        if da not in VALID_DIRECTIONS or db not in VALID_DIRECTIONS:
            raise ValueError("端口方向未配置或非法（需 FROM/TO）")
        if da == db:
            raise ValueError("端口方向不互补（FROM↔TO）")

    # 占用校验
    if _is_port_occupied(project_id, a_port_id) or _is_port_occupied(project_id, b_port_id):
        raise ValueError("端口已被占用")

    # 写入
    with get_conn() as conn, conn.cursor() as cur:
        # 保存 a/b 所属设备ID，便于列表展示和完整性
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


def list_links_in_project(project_id: int) -> List[Dict[str, Any]]:
    """已连接列表，带设备/端口名，供前端展示“已连接”区块。"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              l.id, l.status, l.remark, l.created_at,
              la.id AS a_port_id, la.name AS a_port_name,
              da.id AS a_device_id, da.name AS a_device_name,
              lb.id AS b_port_id, lb.name AS b_port_name,
              db.id AS b_device_id, db.name AS b_device_name
            FROM link l
            JOIN port la ON la.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            JOIN port lb ON lb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            WHERE l.project_id = %s
            ORDER BY l.id DESC
            """,
            (project_id,),
        )
        return cur.fetchall() or []


# ============== 线缆清册 / 导出 / 打印标记 ==============

def list_cables_paginated(project_id: int, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
    """
    返回分页的线缆清册（仅 CONNECTED），附带左右两端设备/端口/类型、方向、printed。
    排序：A设备编号 -> 端口类型 -> A端口名
    """
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 50), 200))
    offset = (page - 1) * page_size

    dir_attr_id = _get_direction_attr_id()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM link WHERE project_id=%s AND status='CONNECTED'",
            (project_id,),
        )
        total = (cur.fetchone() or {}).get("c", 0)

        sql = f"""
        SELECT
          l.id AS link_id, l.status, l.printed, l.printed_at,
          -- A 端
          da.id  AS a_device_id, da.name AS a_device_name,
          pa.id  AS a_port_id,   pa.name AS a_port_name,
          ta.id  AS a_port_type_id, ta.name AS a_port_type_name,
          -- B 端
          db.id  AS b_device_id, db.name AS b_device_name,
          pb.id  AS b_port_id,   pb.name AS b_port_name,
          tb.id  AS b_port_type_id, tb.name AS b_port_type_name,
          -- 方向（优先枚举名，其次文本）
          COALESCE(UPPER(ao_a.name), UPPER(pav_a.value_text)) AS a_dir_raw,
          COALESCE(UPPER(ao_b.name), UPPER(pav_b.value_text)) AS b_dir_raw
        FROM link l
        JOIN port pa ON pa.id = l.a_port_id
        JOIN device da ON da.id = l.a_device_id
        LEFT JOIN port_type ta ON ta.id = pa.port_type_id

        JOIN port pb ON pb.id = l.b_port_id
        JOIN device db ON db.id = l.b_device_id
        LEFT JOIN port_type tb ON tb.id = pb.port_type_id

        LEFT JOIN port_attr_value pav_a
          ON pav_a.port_id = pa.id AND {"pav_a.attribute_id=%s" if dir_attr_id else "1=0"}
        LEFT JOIN attribute_option ao_a ON ao_a.id = pav_a.option_id

        LEFT JOIN port_attr_value pav_b
          ON pav_b.port_id = pb.id AND {"pav_b.attribute_id=%s" if dir_attr_id else "1=0"}
        LEFT JOIN attribute_option ao_b ON ao_b.id = pav_b.option_id

        WHERE l.project_id=%s AND l.status='CONNECTED'
        ORDER BY da.name ASC, ta.name ASC, pa.name ASC
        LIMIT %s OFFSET %s
        """
        params: List[Any] = ([dir_attr_id, dir_attr_id] if dir_attr_id else []) + [project_id, page_size, offset]
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    items: List[Dict[str, Any]] = []
    for r in rows:
        a_dir = (r.get("a_dir_raw") or "").strip().upper() if r.get("a_dir_raw") else ""
        b_dir = (r.get("b_dir_raw") or "").strip().upper() if r.get("b_dir_raw") else ""
        items.append(
            {
                **r,
                "a_dir": a_dir if a_dir in VALID_DIRECTIONS else "",
                "b_dir": b_dir if b_dir in VALID_DIRECTIONS else "",
            }
        )
    return {"total": int(total or 0), "page": page, "page_size": page_size, "items": items}


def fetch_cables_by_ids(project_id: int, link_ids: List[int]) -> List[Dict[str, Any]]:
    """按ID集合获取线缆清册行（仅 CONNECTED）。"""
    if not link_ids:
        return []
    dir_attr_id = _get_direction_attr_id()
    placeholders = ",".join(["%s"] * len(link_ids))
    with get_conn() as conn, conn.cursor() as cur:
        sql = f"""
        SELECT
          l.id AS link_id, l.status, l.printed, l.printed_at,
          da.name AS a_device_name, pa.name AS a_port_name, ta.name AS a_port_type_name,
          db.name AS b_device_name, pb.name AS b_port_name, tb.name AS b_port_type_name,
          COALESCE(UPPER(ao_a.name), UPPER(pav_a.value_text)) AS a_dir_raw,
          COALESCE(UPPER(ao_b.name), UPPER(pav_b.value_text)) AS b_dir_raw
        FROM link l
        JOIN port pa ON pa.id = l.a_port_id
        JOIN device da ON da.id = l.a_device_id
        LEFT JOIN port_type ta ON ta.id = pa.port_type_id
        JOIN port pb ON pb.id = l.b_port_id
        JOIN device db ON db.id = l.b_device_id
        LEFT JOIN port_type tb ON tb.id = pb.port_type_id

        LEFT JOIN port_attr_value pav_a
          ON pav_a.port_id = pa.id AND {"pav_a.attribute_id=%s" if dir_attr_id else "1=0"}
        LEFT JOIN attribute_option ao_a ON ao_a.id = pav_a.option_id

        LEFT JOIN port_attr_value pav_b
          ON pav_b.port_id = pb.id AND {"pav_b.attribute_id=%s" if dir_attr_id else "1=0"}
        LEFT JOIN attribute_option ao_b ON ao_b.id = pav_b.option_id

        WHERE l.project_id=%s AND l.status='CONNECTED' AND l.id IN ({placeholders})
        """
        params: List[Any] = ([dir_attr_id, dir_attr_id] if dir_attr_id else []) + [project_id] + link_ids
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    for r in rows:
        a_dir = (r.get("a_dir_raw") or "").strip().upper() if r.get("a_dir_raw") else ""
        b_dir = (r.get("b_dir_raw") or "").strip().upper() if r.get("b_dir_raw") else ""
        r["a_dir"] = a_dir if a_dir in VALID_DIRECTIONS else ""
        r["b_dir"] = b_dir if b_dir in VALID_DIRECTIONS else ""
    return rows


def fetch_all_cables(project_id: int) -> List[Dict[str, Any]]:
    """获取该项目全部已连接的连接记录（无分页）。"""
    dir_attr_id = _get_direction_attr_id()
    with get_conn() as conn, conn.cursor() as cur:
        sql = f"""
        SELECT
          l.id AS link_id, l.status, l.printed, l.printed_at,
          da.name AS a_device_name, pa.name AS a_port_name, ta.name AS a_port_type_name,
          db.name AS b_device_name, pb.name AS b_port_name, tb.name AS b_port_type_name,
          COALESCE(UPPER(ao_a.name), UPPER(pav_a.value_text)) AS a_dir_raw,
          COALESCE(UPPER(ao_b.name), UPPER(pav_b.value_text)) AS b_dir_raw
        FROM link l
        JOIN port pa ON pa.id = l.a_port_id
        JOIN device da ON da.id = l.a_device_id
        LEFT JOIN port_type ta ON ta.id = pa.port_type_id
        JOIN port pb ON pb.id = l.b_port_id
        JOIN device db ON db.id = l.b_device_id
        LEFT JOIN port_type tb ON tb.id = pb.port_type_id

        LEFT JOIN port_attr_value pav_a
          ON pav_a.port_id = pa.id AND {"pav_a.attribute_id=%s" if dir_attr_id else "1=0"}
        LEFT JOIN attribute_option ao_a ON ao_a.id = pav_a.option_id

        LEFT JOIN port_attr_value pav_b
          ON pav_b.port_id = pb.id AND {"pav_b.attribute_id=%s" if dir_attr_id else "1=0"}
        LEFT JOIN attribute_option ao_b ON ao_b.id = pav_b.option_id

        WHERE l.project_id=%s AND l.status='CONNECTED'
        ORDER BY da.name ASC, ta.name ASC, pa.name ASC
        """
        params: List[Any] = ([dir_attr_id, dir_attr_id] if dir_attr_id else []) + [project_id]
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    for r in rows:
        a_dir = (r.get("a_dir_raw") or "").strip().upper() if r.get("a_dir_raw") else ""
        b_dir = (r.get("b_dir_raw") or "").strip().upper() if r.get("b_dir_raw") else ""
        r["a_dir"] = a_dir if a_dir in VALID_DIRECTIONS else ""
        r["b_dir"] = b_dir if b_dir in VALID_DIRECTIONS else ""
    return rows


def mark_links_printed(project_id: int, link_ids: List[int]) -> int:
    """批量标记为已打印，返回受影响行数。"""
    if not link_ids:
        return 0
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE link SET printed=1, printed_at=NOW() WHERE project_id=%s AND id IN ({','.join(['%s']*len(link_ids))})",
            [project_id] + link_ids,
        )
        conn.commit()
        return int(cur.rowcount or 0)
