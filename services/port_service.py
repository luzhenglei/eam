# services/port_service.py
from typing import List
from db import get_conn

def _descendant_port_ids(conn, root_id: int) -> List[int]:
    """
    递归（BFS）获取某端口的全部子孙端口 id（不包含自身）。
    """
    ids: List[int] = []
    queue = [root_id]
    with conn.cursor() as cur:
        while queue:
            pid = queue.pop(0)
            cur.execute("SELECT id FROM port WHERE parent_port_id=%s", (pid,))
            children = [int(r["id"]) for r in (cur.fetchall() or [])]
            if children:
                ids.extend(children)
                queue.extend(children)
    return ids

def _ensure_port_in_project(conn, project_id: int, port_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
              FROM port p
              JOIN device d ON d.id = p.device_id
             WHERE p.id=%s AND d.project_id=%s
             LIMIT 1
        """, (port_id, project_id))
        if cur.fetchone() is None:
            raise ValueError("端口不属于该项目")

def update_port_active(project_id: int, port_id: int, is_active: bool) -> int:
    """
    设置端口启用状态：
      - 关闭(is_active=False)：级联关闭该端口的所有子孙端口
      - 开启(is_active=True) ：仅开启该端口自身（不级联打开子孙）
    返回受影响的行数
    """
    new_val = 1 if is_active else 0
    with get_conn() as conn, conn.cursor() as cur:
        # 校验端口归属项目
        _ensure_port_in_project(conn, project_id, port_id)

        # 开关策略
        if new_val == 0:
            # 关闭：自身 + 全部子孙
            desc_ids = _descendant_port_ids(conn, port_id)
            all_ids = [port_id] + desc_ids
            placeholders = ",".join(["%s"] * len(all_ids))
            cur.execute(
                f"UPDATE port SET is_active=0 WHERE id IN ({placeholders})",
                all_ids
            )
            affected = int(cur.rowcount or 0)
        else:
            # 开启：仅自身
            cur.execute("UPDATE port SET is_active=1 WHERE id=%s", (port_id,))
            affected = int(cur.rowcount or 0)

        return affected
