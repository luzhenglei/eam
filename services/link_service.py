import re
from typing import Any, Dict, List, Optional, Tuple,Set
from db import get_conn  # 数据库连接工具


# ================== 工具函数 ==================

def _is_port_occupied(project_id: int, port_id: int) -> bool:
    """判断端口在该项目下是否连接数已达上限。"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.max_links,
                   (
                     SELECT COUNT(*) FROM link
                     WHERE project_id=%s AND status='CONNECTED'
                       AND (a_port_id=%s OR b_port_id=%s)
                   ) AS c
            FROM port p
            WHERE p.id=%s
            """,
            (project_id, port_id, port_id, port_id),
        )
        row = cur.fetchone()
        if not row:
            return True
        return int(row.get("c", 0)) >= int(row.get("max_links") or 1)


# ================== 单设备端口列表 ==================

def list_ports_with_links(project_id: int, device_id: int) -> List[Dict[str, Any]]:
    """返回设备的所有端口及其连接信息。"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id AS port_id, p.name, p.port_type_id,
                   pt.name AS attr_name, tpt.name AS port_type_name
            FROM port p
            LEFT JOIN port_template pt ON pt.id = p.port_template_id
            LEFT JOIN port_type tpt ON tpt.id = p.port_type_id
            JOIN device d ON d.id = p.device_id
            WHERE d.project_id=%s AND d.id=%s AND p.is_active=1
            ORDER BY p.id ASC
            """,
            (project_id, device_id),
        )
        ports = cur.fetchall() or []
        if not ports:
            return []

        port_ids = [int(p["port_id"]) for p in ports]
        placeholders = ",".join(["%s"] * len(port_ids))
        cur.execute(
            f"""
            SELECT l.id AS link_id, l.a_port_id, l.b_port_id,
                   da.id AS a_device_id, da.name AS a_device_name, la.name AS a_port_name,
                   db.id AS b_device_id, db.name AS b_device_name, lb.name AS b_port_name
            FROM link l
            JOIN port la ON la.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            JOIN port lb ON lb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            WHERE l.project_id=%s AND l.status='CONNECTED'
              AND (l.a_port_id IN ({placeholders}) OR l.b_port_id IN ({placeholders}))
            """,
            [project_id] + port_ids + port_ids,
        )
        link_rows = cur.fetchall() or []

    link_map: Dict[int, Dict[str, Any]] = {}
    for r in link_rows:
        a_pid = int(r["a_port_id"])
        b_pid = int(r["b_port_id"])
        lid = int(r["link_id"])
        link_map[a_pid] = {
            "link_id": lid,
            "target_device_id": r["b_device_id"],
            "target_device_name": r["b_device_name"],
            "target_port_id": b_pid,
            "target_port_name": r["b_port_name"],
        }
        link_map[b_pid] = {
            "link_id": lid,
            "target_device_id": r["a_device_id"],
            "target_device_name": r["a_device_name"],
            "target_port_id": a_pid,
            "target_port_name": r["a_port_name"],
        }

    out: List[Dict[str, Any]] = []
    for p in ports:
        pid = int(p["port_id"])
        info = link_map.get(pid) or {}
        out.append({**p, **info, "occupied": bool(info)})
    return out


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

def _has_children(conn, port_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM port WHERE parent_port_id=%s LIMIT 1", (port_id,))
        return cur.fetchone() is not None

def create_link(project_id: int, from_port_id: int, to_port_id: int) -> int:
    if from_port_id == to_port_id:
        raise ValueError("不能把端口与自身相连")
    with get_conn() as conn, conn.cursor() as cur:
        fr = _get_port_row(conn, from_port_id, for_update=True)
        to = _get_port_row(conn, to_port_id,   for_update=True)
        _ensure_same_project(project_id, fr, to)
        _ensure_not_same_device(fr, to)

        # ★ 新增：父端口不可连线（有子端口的端口视为“父端口”）
        if _has_children(conn, from_port_id) or _has_children(conn, to_port_id):
            raise ValueError("存在子端口的端口不可直接连线，请改连子端口")

        if _already_linked(conn, from_port_id, to_port_id):
            raise ValueError("这两个端口已建立连线")

        ok, msg = check_port_available(conn, from_port_id)
        if not ok: raise ValueError(msg)
        ok, msg = check_port_available(conn, to_port_id)
        if not ok: raise ValueError(msg)

        cur.execute("""
            INSERT INTO link (project_id, a_device_id, a_port_id, b_device_id, b_port_id, status, created_at)
            VALUES (%s, %s, %s, %s, %s, 'CONNECTED', NOW())
        """, (project_id, fr['device_id'], from_port_id, to['device_id'], to_port_id))
        return cur.lastrowid

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
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 50), 200))
    offset = (page - 1) * page_size

    with get_conn() as conn, conn.cursor() as cur:
        # 统计
        cur.execute("""
            SELECT COUNT(*) AS c
            FROM link l
            WHERE l.project_id=%s AND l.status='CONNECTED'
        """, (project_id,))
        total = (cur.fetchone() or {}).get("c", 0)

        # 列表：带端口类型名
        cur.execute(f"""
            SELECT
                l.id AS link_id,
                l.status,
                l.printed,
                l.printed_at,

                da.id   AS a_device_id,
                da.name AS a_device_name,
                pa.id   AS a_port_id,
                pa.name AS a_port_name,
                ta.name AS a_port_type_name,

                db.id   AS b_device_id,
                db.name AS b_device_name,
                pb.id   AS b_port_id,
                pb.name AS b_port_name,
                tb.name AS b_port_type_name

            FROM link l
            JOIN port   pa ON pa.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            LEFT JOIN port_type ta ON ta.id = pa.port_type_id

            JOIN port   pb ON pb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            LEFT JOIN port_type tb ON tb.id = pb.port_type_id

            WHERE l.project_id=%s AND l.status='CONNECTED'
            ORDER BY da.name ASC, ta.name ASC, pa.name ASC, l.id ASC
            LIMIT %s OFFSET %s
        """, (project_id, page_size, offset))
        rows = cur.fetchall() or []

    # 兼容旧前端：补充方向占位
    for r in rows:
        r["a_dir"] = r.get("a_dir") or ""
        r["b_dir"] = r.get("b_dir") or ""

    return {
        "total": int(total or 0),
        "page": page,
        "page_size": page_size,
        "items": rows,
    }


def fetch_cables_by_ids(project_id: int, link_ids: List[int]) -> List[Dict[str, Any]]:
    if not link_ids:
        return []
    placeholders = ",".join(["%s"] * len(link_ids))
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                l.id AS link_id,
                l.status,
                l.printed,
                l.printed_at,

                da.name AS a_device_name,
                pa.name AS a_port_name,
                ta.name AS a_port_type_name,

                db.name AS b_device_name,
                pb.name AS b_port_name,
                tb.name AS b_port_type_name

            FROM link l
            JOIN port   pa ON pa.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            LEFT JOIN port_type ta ON ta.id = pa.port_type_id

            JOIN port   pb ON pb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            LEFT JOIN port_type tb ON tb.id = pb.port_type_id

            WHERE l.project_id=%s AND l.status='CONNECTED'
              AND l.id IN ({placeholders})
            ORDER BY l.id ASC
        """, [project_id] + link_ids)
        rows = cur.fetchall() or []

    for r in rows:
        r["a_dir"] = ""
        r["b_dir"] = ""
    return rows


def fetch_all_cables(project_id: int) -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                l.id AS link_id,
                l.status,
                l.printed,
                l.printed_at,

                da.name AS a_device_name,
                pa.name AS a_port_name,
                ta.name AS a_port_type_name,

                db.name AS b_device_name,
                pb.name AS b_port_name,
                tb.name AS b_port_type_name

            FROM link l
            JOIN port   pa ON pa.id = l.a_port_id
            JOIN device da ON da.id = l.a_device_id
            LEFT JOIN port_type ta ON ta.id = pa.port_type_id

            JOIN port   pb ON pb.id = l.b_port_id
            JOIN device db ON db.id = l.b_device_id
            LEFT JOIN port_type tb ON tb.id = pb.port_type_id

            WHERE l.project_id=%s AND l.status='CONNECTED'
            ORDER BY da.name ASC, ta.name ASC, pa.name ASC, l.id ASC
        """, (project_id,))
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


# services/link_service.py
# -*- coding: utf-8 -*-
from typing import List, Optional, Tuple, Dict, Any
from db import get_conn  # ✅ 使用你项目已有的连接函数

# ----------------- 工具函数 -----------------
def _to_int(v, d=0) -> int:
    try:
        return int(v)
    except Exception:
        return d

def _get_port_row(conn, port_id: int, for_update: bool = True) -> Dict:
    sql = """
        SELECT p.id, p.device_id, p.name, p.is_active, p.max_links,
               d.project_id, d.name AS device_name
          FROM port p
          JOIN device d ON d.id = p.device_id
         WHERE p.id=%s
    """
    if for_update:
        sql += " FOR UPDATE"
    with conn.cursor() as cur:
        cur.execute(sql, (port_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"端口不存在: {port_id}")
        return row

def _get_port_row_nolock(conn, port_id: int) -> Dict:
    return _get_port_row(conn, port_id, for_update=False)

def _link_count_of_port(conn, port_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT (SELECT COUNT(*) FROM link WHERE a_port_id=%s OR b_port_id=%s) AS c
        """, (port_id, port_id))
        r = cur.fetchone()
        return _to_int(r['c'], 0)

def _already_linked(conn, a_port_id: int, b_port_id: int) -> bool:
    if a_port_id == b_port_id:
        return True
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM link
             WHERE (a_port_id=%s AND b_port_id=%s)
                OR (a_port_id=%s AND b_port_id=%s)
             LIMIT 1
        """, (a_port_id, b_port_id, b_port_id, a_port_id))
        return cur.fetchone() is not None

def _ensure_not_same_device(fr: Dict, to: Dict):
    if fr['device_id'] == to['device_id']:
        raise ValueError("不能连接同一设备的端口")

def _ensure_same_project(project_id: int, fr: Dict, to: Dict):
    if fr['project_id'] != project_id or to['project_id'] != project_id:
        raise ValueError("端口不属于该项目")

# ----------------- 业务校验 -----------------
def check_port_available(conn, port_id: int) -> Tuple[bool, str]:
    row = _get_port_row(conn, port_id, for_update=True)
    if _to_int(row['is_active'], 1) == 0:
        return False, "端口已关闭"
    max_links = _to_int(row['max_links'], 0)
    if max_links <= 0:
        return False, "端口最大连接数为 0"
    cnt = _link_count_of_port(conn, port_id)
    if cnt >= max_links:
        return False, f"端口已达上限 {cnt}/{max_links}"
    return True, "OK"

# ----------------- 创建 / 删除连线 -----------------
def create_link(project_id: int, from_port_id: int, to_port_id: int) -> int:
    if from_port_id == to_port_id:
        raise ValueError("不能把端口与自身相连")
    with get_conn() as conn, conn.cursor() as cur:
        fr = _get_port_row(conn, from_port_id, for_update=True)
        to = _get_port_row(conn, to_port_id,   for_update=True)
        _ensure_same_project(project_id, fr, to)
        _ensure_not_same_device(fr, to)

        if _already_linked(conn, from_port_id, to_port_id):
            raise ValueError("这两个端口已建立连线")

        ok, msg = check_port_available(conn, from_port_id)
        if not ok: raise ValueError(msg)
        ok, msg = check_port_available(conn, to_port_id)
        if not ok: raise ValueError(msg)

        cur.execute("""
            INSERT INTO link (project_id, a_device_id, a_port_id, b_device_id, b_port_id, status, created_at)
            VALUES (%s, %s, %s, %s, %s, 'CONNECTED', NOW())
        """, (project_id, fr['device_id'], from_port_id, to['device_id'], to_port_id))
        return cur.lastrowid

def delete_link(link_id: int) -> int:
    """删除连线，返回受影响行数"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM link WHERE id=%s", (link_id,))
        return cur.rowcount

# ----------------- 候选端口搜索 -----------------
def find_candidates(project_id: int, source_port_id: int, q: Optional[str], limit: int = 20) -> List[dict]:
    q_like = f"%{q.strip()}%" if q else "%"
    with get_conn() as conn, conn.cursor() as cur:
        src = _get_port_row_nolock(conn, source_port_id)

        # 已与 source 建连的对端 port 集合
        cur.execute("""
            SELECT CASE WHEN a_port_id=%s THEN b_port_id ELSE a_port_id END AS other_port_id
              FROM link
             WHERE a_port_id=%s OR b_port_id=%s
        """, (source_port_id, source_port_id, source_port_id))
        already = { _to_int(r['other_port_id']) for r in cur.fetchall() }

        # 初筛：同项目、不同设备、端口开启、名称匹配（设备或端口）
        cur.execute(f"""
            SELECT p.id AS port_id,
                   p.name AS port_name,
                   p.max_links,
                   p.is_active,
                   d.id AS device_id,
                   d.name AS device_name
              FROM port p
              JOIN device d ON d.id=p.device_id
             WHERE d.project_id=%s
               AND d.id <> %s
               AND p.is_active=1
               AND CONCAT(d.name, ' ', IFNULL(p.name,'')) LIKE %s
             ORDER BY d.name, p.name
             LIMIT {int(limit * 2)}
        """, (project_id, src['device_id'], q_like))
        rows = cur.fetchall()

        result: List[dict] = []
        for r in rows:
            pid = _to_int(r['port_id'])
            if pid in already:
                continue
            if _link_count_of_port(conn, pid) >= _to_int(r['max_links'], 0):
                continue
            result.append({
                "port_id": pid,
                "port_name": r['port_name'],
                "device_id": _to_int(r['device_id']),
                "device_name": r['device_name'],
            })
            if len(result) >= limit:
                break
        return result

# ----------------- 端口开关 -----------------
def toggle_port(port_id: int) -> int:
    """
    切换端口 is_active：1->0 或 0->1，返回新值
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT is_active FROM port WHERE id=%s FOR UPDATE", (port_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError("端口不存在")
        new_val = 0 if _to_int(row['is_active'], 0) == 1 else 1
        cur.execute("UPDATE port SET is_active=%s WHERE id=%s", (new_val, port_id))
        return new_val

# ----------------- 列表与分页（兼容旧接口） -----------------
def list_links_in_project(project_id: int) -> List[Dict[str, Any]]:
    """列出项目内所有连线"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT l.id, l.project_id,
                   l.a_device_id, ad.name AS a_device_name,
                   l.a_port_id,   ap.name AS a_port_name,
                   l.b_device_id, bd.name AS b_device_name,
                   l.b_port_id,   bp.name AS b_port_name,
                   l.status, l.created_at
              FROM link l
              JOIN device ad ON ad.id = l.a_device_id
              JOIN device bd ON bd.id = l.b_device_id
              JOIN port   ap ON ap.id = l.a_port_id
              JOIN port   bp ON bp.id = l.b_port_id
             WHERE l.project_id=%s
             ORDER BY l.created_at DESC, l.id DESC
        """, (project_id,))
        return cur.fetchall()

def list_cables_paginated(project_id: int,
                          page: int = 1,
                          page_size: int = 20,
                          q: Optional[str] = None,
                          device_id: Optional[int] = None) -> Dict[str, Any]:
    """
    兼容旧引用：分页返回连线列表
    - 支持 q 模糊搜索（设备/端口名）
    - 可按 device_id 过滤（a 或 b 任一）
    """
    page = max(1, _to_int(page, 1))
    page_size = max(1, _to_int(page_size, 20))
    offset = (page - 1) * page_size
    like = f"%{q.strip()}%" if q else None

    where = ["l.project_id=%s"]
    args: List[Any] = [project_id]

    if like:
        where.append("(ad.name LIKE %s OR bd.name LIKE %s OR ap.name LIKE %s OR bp.name LIKE %s)")
        args += [like, like, like, like]

    if device_id:
        where.append("(l.a_device_id=%s OR l.b_device_id=%s)")
        args += [device_id, device_id]

    where_sql = " AND ".join(where)

    with get_conn() as conn, conn.cursor() as cur:
        # total
        cur.execute(f"""
            SELECT COUNT(*) AS c
              FROM link l
              JOIN device ad ON ad.id = l.a_device_id
              JOIN device bd ON bd.id = l.b_device_id
              JOIN port   ap ON ap.id = l.a_port_id
              JOIN port   bp ON bp.id = l.b_port_id
             WHERE {where_sql}
        """, args)
        total = _to_int(cur.fetchone()['c'], 0)

        # items
        cur.execute(f"""
            SELECT l.id, l.project_id,
                   l.a_device_id, ad.name AS a_device_name,
                   l.a_port_id,   ap.name AS a_port_name,
                   l.b_device_id, bd.name AS b_device_name,
                   l.b_port_id,   bp.name AS b_port_name,
                   l.status, l.created_at
              FROM link l
              JOIN device ad ON ad.id = l.a_device_id
              JOIN device bd ON bd.id = l.b_device_id
              JOIN port   ap ON ap.id = l.a_port_id
              JOIN port   bp ON bp.id = l.b_port_id
             WHERE {where_sql}
             ORDER BY l.created_at DESC, l.id DESC
             LIMIT %s OFFSET %s
        """, args + [page_size, offset])
        items = cur.fetchall()

    return {"total": total, "items": items, "page": page, "page_size": page_size}

def _natural_key(s: str) -> Tuple:
    """端口名自然排序：GE1/0/10 比 GE1/0/2 大"""
    if s is None:
        return ()
    parts = re.split(r'(\d+)', str(s))
    key = []
    for p in parts:
        if p.isdigit():
            key.append(int(p))
        else:
            key.append(p.lower())
    return tuple(key)

def list_connected_port_types(project_id: int) -> List[Dict[str, Any]]:
    """返回当前项目“已建立连接的端口类型”去重列表，用于筛选下拉。"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT t.id, t.name
            FROM link l
            JOIN port pa ON pa.id = l.a_port_id
            LEFT JOIN port_type t ON t.id = pa.port_type_id
            WHERE l.project_id=%s AND l.status='CONNECTED' AND pa.port_type_id IS NOT NULL
            UNION
            SELECT DISTINCT t.id, t.name
            FROM link l
            JOIN port pb ON pb.id = l.b_port_id
            LEFT JOIN port_type t ON t.id = pb.port_type_id
            WHERE l.project_id=%s AND l.status='CONNECTED' AND pb.port_type_id IS NOT NULL
            ORDER BY name ASC
        """, (project_id, project_id))
        return cur.fetchall() or []

def list_cables_paginated(project_id: int,
                          page: int = 1,
                          page_size: int = 50,
                          type_id: Optional[int] = None) -> Dict[str, Any]:
    """
    分页返回线缆清册：
    - 支持按端口类型ID筛选（任一端的端口类型命中即可）
    - 字段包含 a_port_type_name / b_port_type_name，为空时返回空字符串
    - 排序：A设备名 → A端口类型名 → A端口名（自然排序）
    """
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 50), 200))
    offset = (page - 1) * page_size

    with get_conn() as conn, conn.cursor() as cur:
        # 统计
        if type_id:
            cur.execute("""
                SELECT COUNT(*) AS c
                FROM link l
                LEFT JOIN port pa ON pa.id = l.a_port_id
                LEFT JOIN port pb ON pb.id = l.b_port_id
                WHERE l.project_id=%s AND l.status='CONNECTED'
                  AND (pa.port_type_id=%s OR pb.port_type_id=%s)
            """, (project_id, type_id, type_id))
        else:
            cur.execute("""
                SELECT COUNT(*) AS c
                FROM link l
                WHERE l.project_id=%s AND l.status='CONNECTED'
            """, (project_id,))
        total = int((cur.fetchone() or {}).get("c", 0))

        # 明细
        if type_id:
            cur.execute("""
                SELECT
                    l.id AS link_id, l.status, l.printed, l.printed_at,
                    da.id   AS a_device_id, da.name AS a_device_name,
                    pa.id   AS a_port_id,   pa.name AS a_port_name,
                    ta.name AS a_port_type_name,
                    db.id   AS b_device_id, db.name AS b_device_name,
                    pb.id   AS b_port_id,   pb.name AS b_port_name,
                    tb.name AS b_port_type_name
                FROM link l
                JOIN device da ON da.id = l.a_device_id
                JOIN port   pa ON pa.id = l.a_port_id
                LEFT JOIN port_type ta ON ta.id = pa.port_type_id
                JOIN device db ON db.id = l.b_device_id
                JOIN port   pb ON pb.id = l.b_port_id
                LEFT JOIN port_type tb ON tb.id = pb.port_type_id
                WHERE l.project_id=%s AND l.status='CONNECTED'
                  AND (pa.port_type_id=%s OR pb.port_type_id=%s)
                -- 先粗排，最终用 Python 再自然排序
                ORDER BY da.name ASC, ta.name ASC, pa.name ASC, l.id ASC
                LIMIT %s OFFSET %s
            """, (project_id, type_id, type_id, page_size, offset))
        else:
            cur.execute("""
                SELECT
                    l.id AS link_id, l.status, l.printed, l.printed_at,
                    da.id   AS a_device_id, da.name AS a_device_name,
                    pa.id   AS a_port_id,   pa.name AS a_port_name,
                    ta.name AS a_port_type_name,
                    db.id   AS b_device_id, db.name AS b_device_name,
                    pb.id   AS b_port_id,   pb.name AS b_port_name,
                    tb.name AS b_port_type_name
                FROM link l
                JOIN device da ON da.id = l.a_device_id
                JOIN port   pa ON pa.id = l.a_port_id
                LEFT JOIN port_type ta ON ta.id = pa.port_type_id
                JOIN device db ON db.id = l.b_device_id
                JOIN port   pb ON pb.id = l.b_port_id
                LEFT JOIN port_type tb ON tb.id = pb.port_type_id
                WHERE l.project_id=%s AND l.status='CONNECTED'
                ORDER BY da.name ASC, ta.name ASC, pa.name ASC, l.id ASC
                LIMIT %s OFFSET %s
            """, (project_id, page_size, offset))
        rows = cur.fetchall() or []

    # 兜底空字符串 & 自然排序
    for r in rows:
        r["a_port_type_name"] = r.get("a_port_type_name") or ""
        r["b_port_type_name"] = r.get("b_port_type_name") or ""
        r["a_dir"] = r.get("a_dir") or ""
        r["b_dir"] = r.get("b_dir") or ""
    rows.sort(key=lambda r: (
        (r.get("a_device_name") or "").lower(),
        (r.get("a_port_type_name") or "").lower(),
        _natural_key(r.get("a_port_name") or "")
    ))

    return {"total": total, "page": page, "page_size": page_size, "items": rows}


# ----------------- 设备端口（供 connect_config 页面调用） -----------------
def _links_of_port(conn, port_id: int) -> List[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.id,
                   CASE WHEN l.a_port_id=%s THEN l.b_port_id ELSE l.a_port_id END AS other_port_id
              FROM link l
             WHERE l.a_port_id=%s OR l.b_port_id=%s
        """, (port_id, port_id, port_id))
        rows = cur.fetchall()

        result: List[dict] = []
        for r in rows:
            opid = _to_int(r['other_port_id'])
            cur.execute("""
                SELECT p.id AS port_id, p.name AS port_name, d.id AS device_id, d.name AS device_name
                  FROM port p JOIN device d ON d.id=p.device_id
                 WHERE p.id=%s
            """, (opid,))
            dst = cur.fetchone()
            if dst:
                result.append({
                    "link_id": _to_int(r['id']),
                    "other_port_id": opid,
                    "other_port_name": dst['port_name'],
                    "other_device_id": _to_int(dst['device_id']),
                    "other_device_name": dst['device_name'],
                })
        return result

def get_device_ports_for_connect_ui(project_id: int, device_id: int) -> List[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
              p.id   AS port_id,
              p.name AS name,
              p.is_active,
              p.max_links,
              p.parent_port_id,
              pt.name AS port_type_name,
              tpl.name AS attr_name
            FROM port p
            JOIN device d        ON d.id = p.device_id
            LEFT JOIN port_type pt   ON pt.id = p.port_type_id
            LEFT JOIN port_template tpl ON tpl.id = p.port_template_id
            WHERE d.project_id=%s AND p.device_id=%s
            ORDER BY p.name
        """, (project_id, device_id))
        rows = cur.fetchall() or []

        # 连接计数与 has_children
        id_set = [r["port_id"] for r in rows]
        child_set = set()
        if id_set:
            cur.execute(f"SELECT parent_port_id pid, COUNT(*) c FROM port WHERE parent_port_id IN ({','.join(['%s']*len(id_set))}) GROUP BY parent_port_id", id_set)
            for rr in cur.fetchall() or []:
                if rr["pid"]:
                    child_set.add(int(rr["pid"]))

        for r in rows:
            # 计算连接数
            cur.execute("SELECT COUNT(*) c FROM link WHERE a_port_id=%s OR b_port_id=%s", (r["port_id"], r["port_id"]))
            r["conn_count"]  = int((cur.fetchone() or {}).get("c", 0))
            r["has_children"] = r["port_id"] in child_set

        # links 数组（可选：按需保留）
        for r in rows:
            cur.execute("""
                SELECT l.id,
                       CASE WHEN l.a_port_id=%s THEN l.b_port_id ELSE l.a_port_id END AS other_port_id
                FROM link l
                WHERE l.a_port_id=%s OR l.b_port_id=%s
            """, (r["port_id"], r["port_id"], r["port_id"]))
            link_rows = cur.fetchall() or []
            r["links"] = []
            for lr in link_rows:
                cur.execute("""
                    SELECT p.name AS other_port_name, d.name AS other_device_name
                    FROM port p JOIN device d ON d.id = p.device_id
                    WHERE p.id=%s
                """, (lr["other_port_id"],))
                dst = cur.fetchone()
                if dst:
                    r["links"].append({
                        "id": lr["id"],
                        "other_port_name": dst["other_port_name"],
                        "other_device_name": dst["other_device_name"],
                    })
        return rows
