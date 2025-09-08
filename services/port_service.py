# services/port_service.py
from db import get_conn


def update_port_active(project_id: int, port_id: int, is_active: bool) -> None:
    """Enable or disable a port. When disabling, ensure it has no active links."""
    with get_conn() as conn, conn.cursor() as cur:
        # ensure port exists under project
        cur.execute(
            """
            SELECT p.id
            FROM port p
            JOIN device d ON d.id = p.device_id
            WHERE p.id=%s AND d.project_id=%s
            """,
            (port_id, project_id),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("port not found")

        if not is_active:
            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM link
                WHERE project_id=%s AND status='CONNECTED' AND (a_port_id=%s OR b_port_id=%s)
                """,
                (project_id, port_id, port_id),
            )
            cnt = cur.fetchone().get("c", 0)
            if cnt:
                raise ValueError("端口已被占用，无法禁用")

        cur.execute("UPDATE port SET is_active=%s WHERE id=%s", (1 if is_active else 0, port_id))
        conn.commit()
