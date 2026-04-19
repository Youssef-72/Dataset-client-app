import os
import pymysql
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Government Dataset Client App")
app.mount("/static", StaticFiles(directory="static"), name="static")

def get_connection():
    return pymysql.connect(
        host="mysql-3d66d3a9-database-project1-shehata.e.aivencloud.com",
        port=21312,
        user="avnadmin",
        password=os.environ.get("DB_PASSWORD", ""),
        database="defaultdb",
        ssl={"ssl_disabled": False},
        cursorclass=pymysql.cursors.DictCursor
    )

@app.get("/")
def root():
    return FileResponse("static/index.html")

class UserRegister(BaseModel):
    email: str
    username: str
    gender: str
    birthdate: str
    country: str

class UsageAdd(BaseModel):
    user_id: int
    dataset_identifier: str
    project_name: str
    project_category: str

@app.post("/users/register")
def register_user(user: UserRegister):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Users WHERE email = %s", (user.email,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Email already registered")
            cur.execute(
                "INSERT INTO Users (email, username, gender, birthdate, country) VALUES (%s, %s, %s, %s, %s)",
                (user.email, user.username, user.gender, user.birthdate, user.country)
            )
            conn.commit()
            return {"message": "User registered successfully", "user_id": cur.lastrowid}
    finally:
        conn.close()

@app.post("/usage/add")
def add_usage(usage: UsageAdd):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Users WHERE user_id = %s", (usage.user_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="User not found")
            cur.execute("SELECT * FROM Dataset WHERE identifier = %s", (usage.dataset_identifier,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Dataset not found")
            cur.execute(
                "INSERT INTO DatasetUsage (user_id, dataset_identifier, project_name, project_category, usage_date) VALUES (%s, %s, %s, %s, %s)",
                (usage.user_id, usage.dataset_identifier, usage.project_name, usage.project_category, datetime.now())
            )
            conn.commit()
            return {"message": "Usage added successfully"}
    finally:
        conn.close()

@app.get("/usage/user/{user_id}")
def get_user_usage(user_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Users WHERE user_id = %s", (user_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="User not found")
            cur.execute("""
                SELECT du.usage_id, du.dataset_identifier, d.name, du.project_name, du.project_category, du.usage_date
                FROM DatasetUsage du
                JOIN Dataset d ON du.dataset_identifier = d.identifier
                WHERE du.user_id = %s
                ORDER BY du.usage_date DESC
            """, (user_id,))
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/datasets/by-org-type")
def datasets_by_org_type(org_type: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.identifier, d.name, d.access_level, o.name as org_name, o.org_type
                FROM Dataset d JOIN Organization o ON d.org_id = o.org_id
                WHERE o.org_type LIKE %s LIMIT 100
            """, (f"%{org_type}%",))
            results = cur.fetchall()
            if not results:
                raise HTTPException(status_code=404, detail=f"No datasets found for org type: {org_type}")
            return results
    finally:
        conn.close()

@app.get("/organizations/top5")
def top5_organizations():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.name, o.org_type, COUNT(d.identifier) as dataset_count
                FROM Organization o JOIN Dataset d ON o.org_id = d.org_id
                GROUP BY o.org_id, o.name, o.org_type
                ORDER BY dataset_count DESC LIMIT 5
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/datasets/by-format")
def datasets_by_format(format: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT d.identifier, d.name, d.access_level, r.file_format
                FROM Dataset d JOIN Resource r ON d.identifier = r.dataset_identifier
                WHERE r.file_format LIKE %s LIMIT 100
            """, (f"%{format}%",))
            results = cur.fetchall()
            if not results:
                raise HTTPException(status_code=404, detail=f"No datasets found for format: {format}")
            return results
    finally:
        conn.close()

@app.get("/datasets/by-tag")
def datasets_by_tag(tag: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT d.identifier, d.name, d.access_level, dt.tag_name
                FROM Dataset d JOIN Dataset_Tag dt ON d.identifier = dt.dataset_identifier
                WHERE dt.tag_name LIKE %s LIMIT 100
            """, (f"%{tag}%",))
            results = cur.fetchall()
            if not results:
                raise HTTPException(status_code=404, detail=f"No datasets found for tag: {tag}")
            return results
    finally:
        conn.close()

@app.get("/stats/totals")
def dataset_totals():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.name as organization, COUNT(d.identifier) as total
                FROM Organization o JOIN Dataset d ON o.org_id = d.org_id
                GROUP BY o.name ORDER BY total DESC LIMIT 10
            """)
            by_org = cur.fetchall()
            cur.execute("""
                SELECT dtp.topic_name, COUNT(dtp.dataset_identifier) as total
                FROM Dataset_Topic dtp
                GROUP BY dtp.topic_name ORDER BY total DESC LIMIT 10
            """)
            by_topic = cur.fetchall()
            cur.execute("""
                SELECT file_format, COUNT(*) as total
                FROM Resource WHERE file_format != ''
                GROUP BY file_format ORDER BY total DESC LIMIT 10
            """)
            by_format = cur.fetchall()
            cur.execute("""
                SELECT o.org_type, COUNT(d.identifier) as total
                FROM Organization o JOIN Dataset d ON o.org_id = d.org_id
                WHERE o.org_type IS NOT NULL AND o.org_type != ''
                GROUP BY o.org_type ORDER BY total DESC
            """)
            by_org_type = cur.fetchall()
            return {"by_organization": by_org, "by_topic": by_topic, "by_format": by_format, "by_org_type": by_org_type}
    finally:
        conn.close()

@app.get("/datasets/top5-by-users")
def top5_datasets_by_users():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.identifier, d.name, COUNT(du.user_id) as user_count
                FROM Dataset d JOIN DatasetUsage du ON d.identifier = du.dataset_identifier
                GROUP BY d.identifier, d.name
                ORDER BY user_count DESC LIMIT 5
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/usage/by-project-type")
def usage_by_project_type():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT project_category, COUNT(*) as total
                FROM DatasetUsage GROUP BY project_category ORDER BY total DESC
            """)
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/tags/top10-by-project-type")
def top10_tags_by_project_type():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT du.project_category, dt.tag_name, COUNT(*) as tag_count
                FROM DatasetUsage du JOIN Dataset_Tag dt ON du.dataset_identifier = dt.dataset_identifier
                GROUP BY du.project_category, dt.tag_name
                ORDER BY du.project_category, tag_count DESC
            """)
            rows = cur.fetchall()
            result = {}
            for row in rows:
                cat = row["project_category"]
                if cat not in result:
                    result[cat] = []
                if len(result[cat]) < 10:
                    result[cat].append({"tag": row["tag_name"], "count": row["tag_count"]})
            return result
    finally:
        conn.close()
