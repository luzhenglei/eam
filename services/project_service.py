# services/project_service.py
from db import get_conn

def list_projects():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, name, remark, created_at FROM project ORDER BY id DESC")
        return cur.fetchall()

def get_project(pid: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, name, remark, created_at FROM project WHERE id=%s", (pid,))
        return cur.fetchone()

def create_project(name: str, remark: str = None):
    if not name or not name.strip():
        raise ValueError("项目名称必填")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO project(name, remark) VALUES(%s,%s)", (name.strip(), remark))
        return cur.lastrowid

def update_project(pid: int, name: str, remark: str = None):
    if not name or not name.strip():
        raise ValueError("项目名称必填")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("UPDATE project SET name=%s, remark=%s WHERE id=%s", (name.strip(), remark, pid))
        return True

def delete_project(pid: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM project WHERE id=%s", (pid,))
        return True
