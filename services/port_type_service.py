# services/port_type_service.py
from db import get_conn

def list_port_types():
    sql = "SELECT id, code, name FROM port_type ORDER BY id DESC"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def get_port_type(pt_id: int):
    sql = "SELECT id, code, name FROM port_type WHERE id=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (pt_id,))
        return cur.fetchone()

def create_port_type(code: str, name: str):
    if not code or not name:
        raise ValueError("code 与 name 必填")
    sql = "INSERT INTO port_type(code, name) VALUES (%s, %s)"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (code.strip(), name.strip()))
        return cur.lastrowid

def update_port_type(pt_id: int, code: str, name: str):
    if not code or not name:
        raise ValueError("code 与 name 必填")
    sql = "UPDATE port_type SET code=%s, name=%s WHERE id=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (code.strip(), name.strip(), pt_id))
        return True

def delete_port_type(pt_id: int):
    # 若有外键引用（port.port_type_id / port_template.port_type_id），数据库会 RESTRICT 或 SET NULL
    # 这里直接尝试删除，失败抛出异常由上层 flash
    sql = "DELETE FROM port_type WHERE id=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (pt_id,))
        return True
