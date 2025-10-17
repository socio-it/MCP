import psycopg2
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from typing import Any, Dict, List, Optional, Sequence, Union
import os

def get_db_connection():
    """Establece conexión con la base de datos PostgreSQL"""
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        port=int(os.environ.get("DB_PORT")),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("DB_DATABASE"),
        cursor_factory=RealDictCursor
    )
    return conn