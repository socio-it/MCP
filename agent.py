import inspect
import json
import logging
import os
from typing import Any, Dict, List, Optional, Sequence, Union

import psycopg2
from langgraph.graph import END, START, StateGraph
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

from client import mllOpenIA


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


class FlowState(BaseModel):
    """Estado del flujo de procesamiento de consultas"""
    input: List[str] = []
    messages: List[Dict[str, str]] = []
    
    # Estados de procesamiento
    agent_analysis: Optional[str] = None
    sql_query: Optional[str] = None
    sql_queries: List[str] = []  # Para múltiples queries
    sql_results: Optional[Any] = None  # Puede ser dict o list dependiendo del contexto
    all_sql_results: List[Any] = []  # Lista de diccionarios con resultados
    query_evaluation: Optional[Dict[str, Any]] = None
    data_analysis: Optional[str] = None
    
    # Control de flujo
    is_sql_valid: bool = False
    needs_retry: bool = False
    is_ambiguous: bool = False
    clarification_needed: Optional[str] = None
    insufficient_data: bool = False
    requires_multiple_queries: bool = False
    current_query_index: int = 0
    
    # Métricas de tokens (para futura implementación)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def __getitem__(self, k):
        return getattr(self, k)

    def __setitem__(self, k, v):
        setattr(self, k, v)

    def get(self, k, d=None):
        return getattr(self, k, d)

    def setdefault(self, k, d):
        if not hasattr(self, k):
            setattr(self, k, d)
        return getattr(self, k)
    



# ---------------------------------------------------------------------------
# Retell AI Graph - Refactored
# ---------------------------------------------------------------------------
class RetellIAGraph:
    """
    Agente mejorado para procesar consultas de base de datos con múltiples especialistas
    """
    
    def __init__(self):
        # Configurar el modelo de lenguaje
        self.llm = mllOpenIA('gpt-4o')
        
        # Crear el grafo de estados
        sg = StateGraph(FlowState)

        # Definir nodos
        sg.add_node('ingest', self.ingest)
        sg.add_node('agent_coordinator', self.agent_coordinator)
        sg.add_node('ambiguity_detector', self.ambiguity_detector)
        sg.add_node('sql_agent', self.sql_agent)
        sg.add_node('sql_process', self.sql_process)
        sg.add_node('multi_query_processor', self.multi_query_processor)
        sg.add_node('sql_evaluator', self.sql_evaluator)
        sg.add_node('data_analyst', self.data_analyst)
        sg.add_node('clarification_handler', self.clarification_handler)

        # Definir edges
        sg.add_edge(START, 'ingest')
        sg.add_edge('ingest', 'agent_coordinator')
        sg.add_edge('agent_coordinator', 'ambiguity_detector')
        
        # Edge condicional para detectar ambigüedades
        sg.add_conditional_edges(
            'ambiguity_detector',
            lambda st: 'clarification_handler' if st.is_ambiguous or st.insufficient_data else 'sql_agent',
        )
        
        # Decidir si usar procesamiento simple o múltiple
        sg.add_conditional_edges(
            'sql_agent',
            lambda st: 'multi_query_processor' if st.requires_multiple_queries else 'sql_process',
        )
        
        sg.add_edge('sql_process', 'sql_evaluator')
        sg.add_edge('multi_query_processor', 'sql_evaluator')
        
        # Edge condicional basado en validación SQL
        sg.add_conditional_edges(
            'sql_evaluator',
            lambda st: 'data_analyst' if st.is_sql_valid else 'data_analyst',  # Siempre continuar para evitar bucles
        )
        sg.add_edge('data_analyst', END)
        sg.add_edge('clarification_handler', END)

        self.graph = sg.compile()

    # ----------------------------- Nodos -----------------------------------
    
    def ingest(self, state: FlowState) -> FlowState:
        """Inicializa el estado y prepara los datos de entrada"""
        # Limpiar estados previos
        state.agent_analysis = None
        state.sql_query = None
        state.sql_queries = []
        state.sql_results = None
        state.all_sql_results = []
        state.query_evaluation = None
        state.data_analysis = None
        state.is_sql_valid = False
        state.needs_retry = False
        state.is_ambiguous = False
        state.clarification_needed = None
        state.insufficient_data = False
        state.requires_multiple_queries = False
        state.current_query_index = 0
        
        # Convertir input a messages si es necesario
        if state.input and not state.messages:
            if isinstance(state.input[0], str):
                state.messages = [{"role": "user", "content": " ".join(state.input)}]
            else:
                state.messages = state.input
                
        return state

    def agent_coordinator(self, state: FlowState) -> FlowState:
        """Agente coordinador que analiza la intención del usuario"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        prompt = f"""
        Eres el agente coordinador principal. Analiza la siguiente consulta del usuario y:
        
        1. Identifica la intención principal (consulta de datos, análisis, reporte, etc.)
        2. Extrae las entidades clave (tablas, campos, condiciones)
        3. Determina qué tipo de análisis SQL se requiere
        4. Proporciona contexto estructurado para los agentes especializados
        5 estos son los campos de la tabla "employee":
                "id": new_employee['id'],
                "name": new_employee['name'],
                "position": new_employee['position'],
                "department": new_employee['department'],
                "salary": float(new_employee['salary']),
                "hire_date": str(new_employee['hire_date'])
            
        Consulta del usuario:
        {messages_content}
        
        Responde con un análisis claro y estructurado de la solicitud.
        """
        
        try:
            response = self.llm.invoke(prompt).content
            state.agent_analysis = response
        except Exception as e:
            logging.error(f"Error en agent_coordinator: {str(e)}")
            state.agent_analysis = f"Error en análisis: {str(e)}"
            
        return state

    def ambiguity_detector(self, state: FlowState) -> FlowState:
        """Detecta si la consulta es ambigua o falta información"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        prompt = f"""
        Analiza la siguiente consulta de usuario para determinar si es ambigua o falta información importante.
        
        Consulta del usuario: {messages_content}
        Análisis previo: {state.agent_analysis}
        
        La base de datos tiene una tabla 'employees' con estos campos:
        - id (integer)
        - name (varchar) 
        - position (varchar)
        - department (varchar)
        - salary (decimal)
        - hire_date (date)
        
        Evalúa si:
        1. La consulta es demasiado ambigua para generar una respuesta precisa
        2. Falta información específica necesaria para responder
        3. La consulta hace referencia a datos que NO están disponibles en la tabla
        4. Se requieren aclaraciones del usuario
        
        Responde ÚNICAMENTE con uno de estos formatos:
        
        Si la consulta está CLARA y se puede responder:
        CLEAR
        
        Si es AMBIGUA y necesita aclaración:
        AMBIGUOUS: [pregunta específica para aclarar]
        
        Si faltan DATOS que no están en la tabla:
        INSUFFICIENT_DATA: [explicación de qué datos faltan]
        
        Ejemplos:
        - "Muestra empleados" → AMBIGUOUS: ¿Quieres ver todos los empleados o empleados de un departamento específico?
        - "Empleados con mejor rendimiento" → INSUFFICIENT_DATA: No tengo datos de rendimiento, solo información básica como salario, puesto y departamento.
        - "Lista empleados del departamento ventas" → CLEAR
        """
        
        try:
            response = self.llm.invoke(prompt).content.strip()
            
            if response.startswith("CLEAR"):
                state.is_ambiguous = False
                state.insufficient_data = False
                state.clarification_needed = None
                
            elif response.startswith("AMBIGUOUS:"):
                state.is_ambiguous = True
                state.insufficient_data = False
                state.clarification_needed = response.replace("AMBIGUOUS:", "").strip()
                
            elif response.startswith("INSUFFICIENT_DATA:"):
                state.is_ambiguous = False
                state.insufficient_data = True
                state.clarification_needed = response.replace("INSUFFICIENT_DATA:", "").strip()
                
            else:
                # Fallback: asumir que está claro si no reconoce el formato
                state.is_ambiguous = False
                state.insufficient_data = False
                state.clarification_needed = None
                
        except Exception as e:
            logging.error(f"Error en ambiguity_detector: {str(e)}")
            # En caso de error, asumir que está claro para continuar
            state.is_ambiguous = False
            state.insufficient_data = False
            state.clarification_needed = None
            
        return state

    def sql_agent(self, state: FlowState) -> FlowState:
        """Agente especializado en generar consultas SQL (simple o múltiples)"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        # Primero, determinar si se necesitan múltiples queries
        complexity_prompt = f"""
        Analiza si la siguiente consulta requiere múltiples queries SQL para responder completamente:
        
        Consulta: {messages_content}
        Análisis previo: {state.agent_analysis}
        
        Tabla disponible 'employees' con campos: id, name, position, department, salary, hire_date
        
        Responde ÚNICAMENTE con:
        - SINGLE: si se puede responder con una sola query
        - MULTIPLE: si necesita múltiples queries para análisis completo
        
        Casos que requieren MULTIPLE:
        - Comparaciones complejas entre departamentos
        - Análisis que requieren cálculos estadísticos y listados
        - Consultas que necesitan datos agregados Y detalles específicos
        - Análisis temporales que requieren múltiples perspectivas
        
        Ejemplos:
        - "Lista empleados" → SINGLE
        - "Análisis completo de salarios por departamento con empleados mejor pagados" → MULTIPLE
        - "Estadísticas de contratación por año y departamento" → MULTIPLE
        """
        
        try:
            complexity_response = self.llm.invoke(complexity_prompt).content.strip()
            
            if complexity_response.startswith("MULTIPLE"):
                state.requires_multiple_queries = True
                return self._generate_multiple_queries(state, messages_content)
            else:
                state.requires_multiple_queries = False
                return self._generate_single_query(state, messages_content)
                
        except Exception as e:
            logging.error(f"Error en sql_agent: {str(e)}")
            state.sql_query = f"ERROR: {str(e)}"
            state.requires_multiple_queries = False
            
        return state

    def _generate_single_query(self, state: FlowState, messages_content: str) -> FlowState:
        """Genera una sola consulta SQL"""
        
        prompt = f"""
        Genera una consulta SQL PostgreSQL para responder a:
        
        Consulta: {messages_content}
        Análisis previo: {state.agent_analysis}
        
        Tabla 'employees' con campos: id, name, position, department, salary, hire_date
        
        IMPORTANTE: 
        - Solo genera consultas SELECT
        - NO incluyas ```sql ni ``` ni ningún markdown
        - NO incluyas comentarios ni explicaciones
        - Responde ÚNICAMENTE con la consulta SQL pura
        - Si no es posible generar SQL, responde: "NO_SQL_NEEDED"
        """
        
        try:
            response = self.llm.invoke(prompt).content.strip()
            response = self._clean_sql_response(response)
            state.sql_query = response
        except Exception as e:
            logging.error(f"Error generando query simple: {str(e)}")
            state.sql_query = f"ERROR: {str(e)}"
            
        return state

    def _generate_multiple_queries(self, state: FlowState, messages_content: str) -> FlowState:
        """Genera múltiples consultas SQL para análisis complejo"""
        
        prompt = f"""
        Genera una lista de consultas SQL PostgreSQL para un análisis completo de:
        
        Consulta: {messages_content}
        Análisis previo: {state.agent_analysis}
        
        Tabla 'employees' con campos: id, name, position, department, salary, hire_date
        
        Genera de 2 a 4 consultas que cubran diferentes aspectos del análisis:
        1. Datos agregados/estadísticas
        2. Datos específicos/listados
        3. Comparaciones/rankings
        4. Análisis temporal (si aplica)
        
        FORMATO DE RESPUESTA:
        QUERY_1: [consulta SQL 1]
        QUERY_2: [consulta SQL 2]
        QUERY_3: [consulta SQL 3]
        QUERY_4: [consulta SQL 4]
        
        IMPORTANTE:
        - Solo consultas SELECT
        - NO incluyas ```sql ni ``` ni markdown
        - Una consulta por línea con formato QUERY_N:
        - Si no necesitas todas las 4 queries, usa solo las necesarias
        
        Ejemplo:
        QUERY_1: SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department
        QUERY_2: SELECT * FROM employees WHERE salary > 50000 ORDER BY salary DESC
        """
        
        try:
            response = self.llm.invoke(prompt).content.strip()
            queries = self._parse_multiple_queries(response)
            
            if queries:
                state.sql_queries = queries
                state.sql_query = f"MULTIPLE_QUERIES: {len(queries)} queries generated"
            else:
                # Fallback a query simple
                state.requires_multiple_queries = False
                return self._generate_single_query(state, messages_content)
                
        except Exception as e:
            logging.error(f"Error generando queries múltiples: {str(e)}")
            state.sql_query = f"ERROR: {str(e)}"
            state.requires_multiple_queries = False
            
        return state

    def sql_process(self, state: FlowState) -> FlowState:
        """Ejecuta la consulta SQL generada"""
        
        if not state.sql_query or state.sql_query.startswith("ERROR") or state.sql_query == "NO_SQL_NEEDED":
            state.sql_results = {
                "query": state.sql_query or "NO_QUERY",
                "total_rows": 0,
                "returned_rows": 0,
                "data": [],
                "truncated": False
            }
            return state
            
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Ejecutar la consulta
            cursor.execute(state.sql_query)
            rows = cursor.fetchall()
            
            # Convertir a lista de diccionarios para serialización
            all_results = [dict(row) for row in rows] if rows else []
            
            # Limitar a máximo 50 filas para retorno, pero mantener info completa
            limited_results = all_results[:50] if len(all_results) > 50 else all_results
            
            # Crear estructura consistente
            state.sql_results = {
                "query": state.sql_query,
                "total_rows": len(all_results),
                "returned_rows": len(limited_results),
                "data": limited_results,
                "truncated": len(all_results) > 50
            }
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"Error ejecutando SQL: {str(e)}")
            state.sql_results = {
                "query": state.sql_query,
                "error": f"Error al ejecutar consulta: {str(e)}",
                "total_rows": 0,
                "returned_rows": 0,
                "data": [],
                "truncated": False
            }
            
        return state

    def multi_query_processor(self, state: FlowState) -> FlowState:
        """Procesa múltiples consultas SQL secuencialmente"""
        
        if not state.sql_queries:
            state.sql_results = {
                "query": "NO_QUERIES",
                "total_queries": 0,
                "successful_queries": 0,
                "total_rows_found": 0,
                "total_rows_returned": 0,
                "queries_detail": [],
                "summary": "No se generaron consultas"
            }
            state.all_sql_results = []
            return state
            
        all_results = []
        total_rows_across_queries = 0
        
        for i, query in enumerate(state.sql_queries):
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # Limpiar la query individual
                clean_query = self._clean_sql_response(query)
                
                if clean_query and not clean_query.startswith("ERROR"):
                    cursor.execute(clean_query)
                    rows = cursor.fetchall()
                    all_query_results = [dict(row) for row in rows] if rows else []
                    
                    # Limitar a máximo 50 filas por query
                    limited_results = all_query_results[:50] if len(all_query_results) > 50 else all_query_results
                    total_rows_across_queries += len(all_query_results)
                    
                    all_results.append({
                        "query_index": i + 1,
                        "query": clean_query,
                        "total_rows": len(all_query_results),
                        "returned_rows": len(limited_results),
                        "data": limited_results,
                        "truncated": len(all_query_results) > 50,
                        "success": True
                    })
                else:
                    all_results.append({
                        "query_index": i + 1,
                        "query": query,
                        "error": f"Query inválida: {clean_query}",
                        "total_rows": 0,
                        "returned_rows": 0,
                        "data": [],
                        "truncated": False,
                        "success": False
                    })
                
                cursor.close()
                conn.close()
                
            except Exception as e:
                logging.error(f"Error ejecutando query {i+1}: {str(e)}")
                all_results.append({
                    "query_index": i + 1,
                    "query": query,
                    "error": f"Error ejecutando consulta: {str(e)}",
                    "total_rows": 0,
                    "returned_rows": 0,
                    "data": [],
                    "truncated": False,
                    "success": False
                })
        
        # Consolidar todos los resultados
        state.all_sql_results = all_results
        
        # Para compatibilidad, poner el resumen en sql_results
        successful_queries = sum(1 for r in all_results if r.get("success", False))
        total_returned_rows = sum(r.get("returned_rows", 0) for r in all_results)
        
        state.sql_results = {
            "query": f"MULTIPLE_QUERIES: {len(all_results)} queries",
            "total_queries": len(all_results),
            "successful_queries": successful_queries,
            "total_rows_found": total_rows_across_queries,
            "total_rows_returned": total_returned_rows,
            "queries_detail": all_results,
            "summary": f"Ejecutadas {len(all_results)} consultas ({successful_queries} exitosas), {total_rows_across_queries} filas encontradas, {total_returned_rows} filas retornadas"
        }
        
        return state

    def sql_evaluator(self, state: FlowState) -> FlowState:
        """Evalúa la calidad y validez de la consulta SQL"""
        
        if not state.sql_query or state.sql_query.startswith("ERROR"):
            state.is_sql_valid = False
            state.query_evaluation = {"valid": False, "reason": "Consulta SQL inválida o con errores"}
            return state
            
        if state.sql_query == "NO_SQL_NEEDED":
            state.is_sql_valid = True
            state.query_evaluation = {"valid": True, "reason": "No se requiere consulta SQL"}
            return state
            
        # Evaluar si hay errores en los resultados
        if isinstance(state.sql_results, dict):
            if "error" in state.sql_results:
                # IMPORTANTE: No reintentar si ya hay un error - evitar bucles
                state.is_sql_valid = True  # Marcar como válido para seguir al siguiente paso
                state.query_evaluation = {
                    "valid": False, 
                    "reason": f"Error en ejecución: {state.sql_results['error']}",
                    "continue_anyway": True
                }
            else:
                state.is_sql_valid = True
                if state.requires_multiple_queries:
                    successful_queries = state.sql_results.get("successful_queries", 0)
                    total_queries = state.sql_results.get("total_queries", 0)
                    state.query_evaluation = {
                        "valid": True,
                        "reason": f"Múltiples consultas ejecutadas: {successful_queries}/{total_queries} exitosas",
                        "total_rows_found": state.sql_results.get("total_rows_found", 0),
                        "total_rows_returned": state.sql_results.get("total_rows_returned", 0)
                    }
                else:
                    state.query_evaluation = {
                        "valid": True,
                        "reason": "Consulta ejecutada exitosamente",
                        "total_rows": state.sql_results.get("total_rows", 0),
                        "returned_rows": state.sql_results.get("returned_rows", 0),
                        "truncated": state.sql_results.get("truncated", False)
                    }
        else:
            # Formato legacy - lista
            if state.sql_results and isinstance(state.sql_results[0], dict) and "error" in state.sql_results[0]:
                state.is_sql_valid = True
                state.query_evaluation = {
                    "valid": False, 
                    "reason": f"Error en ejecución: {state.sql_results[0]['error']}",
                    "continue_anyway": True
                }
            else:
                state.is_sql_valid = True
                state.query_evaluation = {
                    "valid": True, 
                    "reason": "Consulta ejecutada exitosamente",
                    "rows_returned": len(state.sql_results) if state.sql_results else 0
                }
            
        return state

    def data_analyst(self, state: FlowState) -> FlowState:
        """Analiza los resultados y genera insights"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        if state.sql_query == "NO_SQL_NEEDED":
            prompt = f"""
            La consulta del usuario no requiere acceso a base de datos. 
            Consulta original: {messages_content}
            Análisis previo: {state.agent_analysis}
            
            Proporciona una respuesta directa y útil al usuario.
            """
        elif state.requires_multiple_queries and state.all_sql_results:
            # Análisis de múltiples queries
            prompt = f"""
            Analiza los resultados de múltiples consultas SQL y genera un análisis integral:
            
            Consulta original: {messages_content}
            Número de consultas ejecutadas: {len(state.all_sql_results)}
            
            Resultados detallados:
            {self._format_multiple_results_for_analysis(state.all_sql_results)}
            
            Proporciona:
            1. **Resumen ejecutivo** de todos los hallazgos
            2. **Análisis integrado** combinando datos de todas las consultas
            3. **Insights clave** y patrones identificados
            4. **Conclusiones** y respuesta directa a la pregunta original
            5. **Recomendaciones** basadas en el análisis completo
            
            Estructura tu respuesta de manera clara y profesional, destacando los puntos más importantes.
            """
        else:
            # Análisis de query simple
            prompt = f"""
            Analiza los siguientes resultados de la consulta SQL y genera insights útiles:
            
            Consulta original: {messages_content}
            Consulta SQL ejecutada: {state.sql_query}
            Resultados: {state.sql_results}
            
            Proporciona:
            1. Un resumen claro de los resultados
            2. Insights relevantes y patrones identificados
            3. Respuesta directa a la pregunta del usuario
            4. Recomendaciones si aplica
            
            Responde de manera clara y profesional.
            """
        
        try:
            response = self.llm.invoke(prompt).content
            state.data_analysis = response
        except Exception as e:
            logging.error(f"Error en data_analyst: {str(e)}")
            state.data_analysis = f"Error en análisis: {str(e)}"
            
        return state

    def clarification_handler(self, state: FlowState) -> FlowState:
        """Maneja casos donde se necesita aclaración o faltan datos"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        if state.is_ambiguous:
            # Consulta ambigua - pedir aclaración
            response = f"""
            Tu consulta necesita más detalles para poder ayudarte mejor.
            
            📝 Consulta original: {messages_content}
            
            ❓ **Aclaración necesaria**: {state.clarification_needed}
            
            💡 **Información disponible**: Tengo acceso a datos de empleados incluyendo:
            - Nombre, puesto, departamento
            - Salario y fecha de contratación
            - Identificador único de cada empleado
            
            Por favor, proporciona más detalles específicos para poder generar la consulta exacta que necesitas.
            """
            
        elif state.insufficient_data:
            # Datos insuficientes - explicar limitaciones
            response = f"""
            No puedo responder completamente a tu consulta debido a limitaciones en los datos disponibles.
            
            📝 Consulta original: {messages_content}
            
            ❌ **Limitación identificada**: {state.clarification_needed}
            
            📊 **Datos disponibles**: La base de datos contiene únicamente información básica de empleados:
            - **Identificación**: ID, nombre
            - **Puesto**: posición, departamento  
            - **Compensación**: salario
            - **Temporal**: fecha de contratación
            
            💡 **Sugerencia**: Puedo ayudarte con consultas relacionadas con esta información disponible. 
            ¿Te gustaría reformular tu pregunta basándote en estos datos?
            
            **Ejemplos de lo que SÍ puedo hacer**:
            - Listar empleados por departamento
            - Calcular estadísticas salariales
            - Analizar antigüedad de empleados
            - Comparar departamentos por tamaño o salarios
            """
            
        else:
            # Fallback
            response = f"""
            Necesito más información para procesar tu consulta correctamente.
            
            📝 Consulta original: {messages_content}
            
            Por favor, proporciona más detalles específicos sobre lo que necesitas.
            """
        
        state.data_analysis = response
        return state

    # ----------------------------- Métodos de utilidad -----------------------------------
    
    def _extract_content_from_messages(self, messages: List[Dict[str, str]]) -> str:
        """Extrae el contenido de los mensajes para procesamiento"""
        if not messages:
            return ""
        
        content_parts = []
        for msg in messages:
            if isinstance(msg, dict) and "content" in msg:
                content_parts.append(msg["content"])
            elif isinstance(msg, str):
                content_parts.append(msg)
                
        return " ".join(content_parts)

    def _clean_sql_response(self, response: str) -> str:
        """Limpia la respuesta SQL eliminando markdown y formato"""
        if not response:
            return response
            
        # Eliminar bloques de código markdown
        response = response.replace("```sql", "").replace("```", "")
        
        # Eliminar líneas en blanco al inicio y final
        response = response.strip()
        
        # Si contiene múltiples líneas, tomar solo la consulta SQL válida
        lines = [line.strip() for line in response.split('\n') if line.strip()]
        
        # Filtrar líneas que no sean SQL válido
        sql_lines = []
        for line in lines:
            # Saltear comentarios y explicaciones
            if not line.startswith('#') and not line.startswith('--') and not line.lower().startswith('esta consulta'):
                sql_lines.append(line)
        
        # Unir las líneas SQL válidas
        cleaned_sql = ' '.join(sql_lines)
        
        return cleaned_sql

    def _parse_multiple_queries(self, response: str) -> List[str]:
        """Parsea la respuesta que contiene múltiples queries"""
        queries = []
        
        lines = response.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('QUERY_'):
                # Extraer la parte después de ':'
                parts = line.split(':', 1)
                if len(parts) == 2:
                    query = parts[1].strip()
                    # Limpiar la query
                    clean_query = self._clean_sql_response(query)
                    if clean_query and not clean_query.lower().startswith('no_sql_needed'):
                        queries.append(clean_query)
        
        return queries

    def _format_multiple_results_for_analysis(self, all_results: List[Dict[str, Any]]) -> str:
        """Formatea los resultados de múltiples queries para análisis"""
        formatted = []
        
        for result in all_results:
            query_info = f"Query {result['query_index']}: {result['query']}"
            
            if not result.get('success', False) or 'error' in result:
                error_msg = result.get('error', 'Error desconocido')
                formatted.append(f"{query_info}\nEstado: Error\nDetalle: {error_msg}\n")
            else:
                total_rows = result.get('total_rows', 0)
                returned_rows = result.get('returned_rows', 0)
                truncated = result.get('truncated', False)
                data_sample = result.get('data', [])
                
                # Información de estado
                status_info = f"Estado: Exitoso\nFilas encontradas: {total_rows}\nFilas retornadas: {returned_rows}"
                if truncated:
                    status_info += " (datos truncados a 50 filas)"
                
                # Muestra de datos
                if data_sample:
                    if len(data_sample) <= 3:
                        preview = f"Datos completos: {data_sample}"
                    else:
                        preview = f"Muestra de datos (primeros 3 de {len(data_sample)}): {data_sample[:3]}"
                else:
                    preview = "Sin datos encontrados"
                
                formatted.append(f"{query_info}\n{status_info}\n{preview}\n")
        
        return "\n".join(formatted)

    @staticmethod
    def _serialise(obj: Any) -> Any:
        """Serializa objetos BaseModel y secuencias"""
        if isinstance(obj, BaseModel):
            return obj.dict() if hasattr(obj, 'dict') else obj.model_dump()
        if isinstance(obj, Sequence) and not isinstance(obj, str):
            return [RetellIAGraph._serialise(o) for o in obj]
        return obj

    # ---------------------------- API ------------------------------
    
    def run(self, segments: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Ejecuta el grafo completo para procesar la consulta del usuario
        
        Args:
            segments: Lista de mensajes/diccionarios de la conversación
            
        Returns:
            Diccionario con el análisis completo y resultados
        """
        try:
            # Preparar estado inicial
            if isinstance(segments, str):
                initial_state = FlowState(input=[segments])
            elif isinstance(segments, list) and all(isinstance(s, str) for s in segments):
                initial_state = FlowState(input=segments)
            else:
                initial_state = FlowState(messages=segments)
            
            # Ejecutar el grafo
            final_state = self.graph.invoke(
                initial_state, config={'recursion_limit': 200}
            )
            
            # El final_state es un diccionario, no un objeto FlowState
            # Acceder a los valores usando claves de diccionario
            result = {
                'agent_analysis': final_state.get('agent_analysis'),
                'is_ambiguous': final_state.get('is_ambiguous', False),
                'insufficient_data': final_state.get('insufficient_data', False),
                'clarification_needed': final_state.get('clarification_needed'),
                'requires_multiple_queries': final_state.get('requires_multiple_queries', False),
                'sql_query': final_state.get('sql_query'),
                'sql_queries': final_state.get('sql_queries', []),
                'sql_results': self._serialise(final_state.get('sql_results')),
                'all_sql_results': self._serialise(final_state.get('all_sql_results', [])),
                'query_evaluation': final_state.get('query_evaluation'),
                'data_analysis': final_state.get('data_analysis'),
                'summary': final_state.get('data_analysis') or final_state.get('agent_analysis') or "No se pudo generar resumen"
            }
            
            return result
            
        except Exception as e:
            logging.error(f"Error en RetellIAGraph.run: {str(e)}")
            return {
                'error': f"Error procesando consulta: {str(e)}",
                'summary': f"Error: {str(e)}"
            }