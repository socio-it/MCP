import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from fastmcp import FastMCP
from agent import RetellIAGraph
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()
app = FastMCP("company-db-sever")

def get_db_connection():
    conn = psycopg2.connect(
        host = os.environ.get("DB_HOST"),
        port = int(os.environ.get("DB_PORT")),
        user = os.environ.get("DB_USER"),
        password = os.environ.get("DB_PASSWORD"),
        database = os.environ.get("DB_DATABASE"),
        cursor_factory= RealDictCursor
    ) 
    return conn

@app.tool
def list_employees(limit: int = 5) -> List[Dict[str, Any]]:
    """Listar los empleados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, name, position, department, salary, hire_date
            FROM employees 
            ORDER BY id 
            LIMIT %s
        """, (limit,))

        rows = cursor.fetchall()
        employees = []

        for row in rows:
            employees.append({
                "id": row['id'],
                "name": row['name'],
                "position": row['position'],
                "department": row['department'],
                "salary": float(row['salary']),
                "hire_date": str(row['hire_date'])
            })
        cursor.close()
        conn.close()

        return employees
    except Exception as e:
        return {
            'error': f'Error al obtener los empleados {str(e)}'
        }

@app.tool
def add_employee(
    name: str,
    position: str,
    department: str,
    salary: float,
    hire_date: Optional[str] = None
):
    """Agrega un nuevo empleado"""
    try:
        if not name.strip():
            return {"error": "El nombre es requerido"}
        
        if salary <= 0:
            return {"error": "El salario debe ser mayor a 0"}

        if not hire_date:
            hire_date = datetime.now().strftime('%Y-%m-%d')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO employees (name, position, department, salary, hire_date)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, name, position, department, salary, hire_date
        """, (name.strip(), position.strip(), department.strip(), salary, hire_date))
        
        new_employee = cursor.fetchone()
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "employee": {
                "id": new_employee['id'],
                "name": new_employee['name'],
                "position": new_employee['position'],
                "department": new_employee['department'],
                "salary": float(new_employee['salary']),
                "hire_date": str(new_employee['hire_date'])
            }
        }
        
    except Exception as e:
        return {"error": f"Error al agregar empleado: {str(e)}"}
    
@app.tool
def get_resumen(messages: str) -> Dict[str, Any]:
    """Genera un resumen y análisis inteligente de consultas sobre empleados"""
    try:
        import json
        
        # Convertir el string JSON a una lista de diccionarios
        if isinstance(messages, str):
            try:
                messages_list = json.loads(messages)
            except json.JSONDecodeError:
                # Si no es JSON válido, crear una estructura básica
                messages_list = [{"role": "user", "content": messages}]
        else:
            messages_list = messages
            
        # Verificar que tenemos la API key de OpenAI
        if not os.environ.get("OPENAI_API_KEY"):
            return {
                "error": "OPENAI_API_KEY no está configurada en las variables de entorno"
            }
        
        # Usar el nuevo agente mejorado
        engine = RetellIAGraph()
        result = engine.run(messages_list)
        
        # El nuevo agente devuelve un diccionario completo con más información
        return {
            "success": True,
            "analysis": result,
            "is_ambiguous": result.get("is_ambiguous", False),
            "insufficient_data": result.get("insufficient_data", False),
            "clarification_needed": result.get("clarification_needed"),
            "requires_multiple_queries": result.get("requires_multiple_queries", False),
            "sql_queries": result.get("sql_queries", []),
            "all_sql_results": result.get("all_sql_results", []),
            "summary": result.get("summary", "No se pudo generar resumen"),
            "sql_query": result.get("sql_query"),
            "data_results": result.get("sql_results")
        }
        
    except Exception as e:
        return {
            "error": f"Error al generar resumen: {str(e)}"
        }

@app.tool
def query_employees_ai(query: str) -> Dict[str, Any]:
    """Consulta inteligente de empleados usando IA - acepta preguntas en lenguaje natural"""
    try:
        # Verificar API key
        if not os.environ.get("OPENAI_API_KEY"):
            return {
                "error": "OPENAI_API_KEY no está configurada en las variables de entorno"
            }
        
        # Crear mensaje para el agente
        messages = [{"role": "user", "content": query}]
        
        # Usar el agente para procesar la consulta
        engine = RetellIAGraph()
        result = engine.run(messages)
        
        return {
            "success": True,
            "query": query,
            "is_ambiguous": result.get("is_ambiguous", False),
            "insufficient_data": result.get("insufficient_data", False),
            "clarification_needed": result.get("clarification_needed"),
            "requires_multiple_queries": result.get("requires_multiple_queries", False),
            "sql_queries": result.get("sql_queries", []),
            "all_sql_results": result.get("all_sql_results", []),
            "analysis": result.get("data_analysis"),
            "sql_executed": result.get("sql_query"),
            "results": result.get("sql_results"),
            "summary": result.get("summary", "No se pudo procesar la consulta")
        }
        
    except Exception as e:
        return {
            "error": f"Error al procesar consulta: {str(e)}"
        }

@app.tool
def get_query_details(query: str) -> Dict[str, Any]:
    """Obtiene detalles completos de la ejecución de consultas incluyendo todas las queries ejecutadas y datos limitados a 50 filas"""
    try:
        # Verificar API key
        if not os.environ.get("OPENAI_API_KEY"):
            return {
                "error": "OPENAI_API_KEY no está configurada en las variables de entorno"
            }
        
        # Crear mensaje para el agente
        messages = [{"role": "user", "content": query}]
        
        # Usar el agente para procesar la consulta
        engine = RetellIAGraph()
        result = engine.run(messages)
        
        # Extraer información detallada de las queries
        response = {
            "success": True,
            "original_query": query,
            "requires_multiple_queries": result.get("requires_multiple_queries", False),
            "analysis": result.get("data_analysis"),
            "summary": result.get("summary", "No se pudo procesar la consulta")
        }
        
        # Agregar detalles de queries simples o múltiples
        if result.get("requires_multiple_queries", False):
            response.update({
                "query_type": "multiple",
                "total_queries": len(result.get("sql_queries", [])),
                "queries_executed": result.get("sql_queries", []),
                "execution_details": result.get("all_sql_results", []),
                "summary_stats": {
                    "total_queries": result.get("sql_results", {}).get("total_queries", 0),
                    "successful_queries": result.get("sql_results", {}).get("successful_queries", 0),
                    "total_rows_found": result.get("sql_results", {}).get("total_rows_found", 0),
                    "total_rows_returned": result.get("sql_results", {}).get("total_rows_returned", 0)
                }
            })
        else:
            # Query simple
            sql_results = result.get("sql_results", {})
            if isinstance(sql_results, dict):
                response.update({
                    "query_type": "single",
                    "query_executed": sql_results.get("query"),
                    "total_rows_found": sql_results.get("total_rows", 0),
                    "rows_returned": sql_results.get("returned_rows", 0),
                    "data_truncated": sql_results.get("truncated", False),
                    "data": sql_results.get("data", []),
                    "error": sql_results.get("error")
                })
            else:
                # Formato legacy
                response.update({
                    "query_type": "single",
                    "query_executed": result.get("sql_query"),
                    "data": sql_results,
                    "rows_returned": len(sql_results) if sql_results else 0
                })
        
        return response
        
    except Exception as e:
        return {
            "error": f"Error al obtener detalles de consulta: {str(e)}"
        }
    
if __name__ == "__main__":
    app.run(transport="sse", host="0.0.0.0", port=3000)