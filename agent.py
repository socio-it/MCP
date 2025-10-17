import json
import logging
import decimal
from typing import Any, Dict, List, Optional, Sequence, Union

from pydantic import BaseModel
from langgraph.graph import END, START, StateGraph


from client import mllOpenIA
from utils import get_db_connection
from prompts import prompt_multi_query, prompt_single_query

dict_tables = {
  "tables": [
    {
      "name": "raw_input_metrics",
      "columns": [
        { "name": "country", "type": "text" },
        { "name": "city", "type": "text" },
        { "name": "zone", "type": "text" },
        { "name": "zone_type", "type": "text" },
        { "name": "zone_prioritization", "type": "text" },
        { "name": "metric", "type": "text" },
        { "name": "l8w_roll", "type": "double precision" },
        { "name": "l7w_roll", "type": "double precision" },
        { "name": "l6w_roll", "type": "double precision" },
        { "name": "l5w_roll", "type": "double precision" },
        { "name": "l4w_roll", "type": "double precision" },
        { "name": "l3w_roll", "type": "double precision" },
        { "name": "l2w_roll", "type": "double precision" },
        { "name": "l1w_roll", "type": "double precision" },
        { "name": "l0w_roll", "type": "double precision" }
      ]
    },
    {
      "name": "raw_orders",
      "columns": [
        { "name": "country", "type": "text" },
        { "name": "city", "type": "text" },
        { "name": "zone", "type": "text" },
        { "name": "metric", "type": "text" },
        { "name": "l8w", "type": "integer" },
        { "name": "l7w", "type": "integer" },
        { "name": "l6w", "type": "integer" },
        { "name": "l5w", "type": "integer" },
        { "name": "l4w", "type": "integer" },
        { "name": "l3w", "type": "integer" },
        { "name": "l2w", "type": "integer" },
        { "name": "l1w", "type": "integer" },
        { "name": "l0w", "type": "integer" }
      ]
    }
  ]
}

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
    
    # Validación de tablas
    validated_tables: Dict[str, Dict[str, Any]] = {}  # Almacena muestras y metadatos de tablas validadas
    table_samples: Dict[str, List[Dict[str, Any]]] = {}  # Muestras de datos de cada tabla
    table_validation_errors: List[str] = []  # Errores encontrados durante la validación
    
    # Control de flujo
    is_sql_valid: bool = False
    needs_retry: bool = False
    is_ambiguous: bool = False
    clarification_needed: Optional[str] = None
    insufficient_data: bool = False
    requires_multiple_queries: bool = False
    current_query_index: int = 0
    
    # Control de reintentos
    retry_count: int = 0
    max_retries: int = 2
    error_messages: List[str] = []  # Para almacenar mensajes de error de intentos anteriores
    
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
    


class AnalystIAGraph:
    """
    Agente mejorado para procesar consultas de base de datos con múltiples especialistas
    """
    
    def __init__(self, agent_prompt):
        self.agent_prompt = agent_prompt
        # Configurar el modelo de lenguaje
        self.llm = mllOpenIA('gpt-4.1-nano')
        sg = StateGraph(FlowState)

        # Definir nodos
        sg.add_node('ingest', self.ingest)
        sg.add_node('agent_coordinator', self.agent_coordinator)
        sg.add_node('ambiguity_detector', self.ambiguity_detector)
        sg.add_node('clarification_handler', self.clarification_handler)
        sg.add_node('table_validator', self.table_validator)
        sg.add_node('sql_agent', self.sql_agent)
        sg.add_node('sql_process', self.sql_process)
        sg.add_node('multi_query_processor', self.multi_query_processor)
        sg.add_node('sql_evaluator', self.sql_evaluator)
        sg.add_node('data_analyst', self.data_analyst)
        

        # Definir edges
        sg.add_edge(START, 'ingest')
        sg.add_edge('ingest', 'agent_coordinator')
        sg.add_edge('agent_coordinator', 'ambiguity_detector')
        
        # Edge ambigüedades
        sg.add_conditional_edges(
            'ambiguity_detector',
            lambda st: 'clarification_handler' if st.is_ambiguous or st.insufficient_data else 'table_validator',
        )
        
        # Conectar el validador de tablas al agente SQL
        sg.add_edge('table_validator', 'sql_agent')
        
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
            lambda st: 'sql_agent' if st.needs_retry and st.retry_count < st.max_retries else 'data_analyst'
        )
        sg.add_edge('data_analyst', END)
        sg.add_edge('clarification_handler', END)

        self.graph = sg.compile()

    # ----------------------------- Nodos -----------------------------------
    
    def ingest(self, state: FlowState) -> FlowState:
        """Inicializa el estado y prepara los datos de entrada"""
        # resetear
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
        state.validated_tables = {}
        state.table_samples = {}
        state.table_validation_errors = []
        state.retry_count = 0
        state.error_messages = []
        
        #Convertir input a messages
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
        Eres el agente coordinador principal. Analiza la siguiente consulta del usuario:
        
        {self.agent_prompt}
            
        Consulta del usuario:
        {messages_content}
        
        La base de datos tiene esta estructura:
        {json.dumps(dict_tables)}

        Responde con un análisis claro y estructurado de la solicitud.
        """
        
        try:
            response = self.llm.invoke(prompt).content
            state.agent_analysis = response
        except Exception as e:
            logging.error(f"Error en agent_coordinator: {str(e)}")
            state.agent_analysis = f"Error en análisis: {str(e)}"
            
        return state
        
    def table_validator(self, state: FlowState) -> FlowState:
        """
        Validador de tablas que ejecuta queries de muestra para verificar la estructura y datos
        antes de generar las consultas SQL principales.
        """
        messages_content = self._extract_content_from_messages(state.messages)
        
        # Primero, analizar qué tablas podrían ser relevantes para la consulta
        prompt = f"""
        Identifica las tablas que podrían ser relevantes para responder la siguiente consulta:
        
        Consulta del usuario: {messages_content}
        Análisis previo: {state.agent_analysis}
        
        La base de datos tiene estas tablas:
        {json.dumps(dict_tables)}
        
        Responde con un objeto JSON que contenga un array de nombres de tablas:
        {{
            "tables": ["nombre_tabla1", "nombre_tabla2", ...]
        }}
        """
        
        try:
            response = self.llm.invoke(prompt).content.strip()
            
            # Extraer lista de tablas del JSON
            tables_to_validate = []
            
            # Buscar y extraer el JSON
            import re
            json_match = re.search(r'\{[\s\S]*\}', response)
            if json_match:
                try:
                    tables_json = json.loads(json_match.group(0))
                    if isinstance(tables_json, dict) and "tables" in tables_json:
                        tables_to_validate = tables_json["tables"]
                except json.JSONDecodeError:
                    # Fallback: buscar nombres de tablas en la respuesta
                    for table_info in dict_tables.get("tables", []):
                        table_name = table_info.get("name")
                        if table_name and table_name in response:
                            tables_to_validate.append(table_name)
            
            # Si no se encontraron tablas, usar todas las disponibles
            if not tables_to_validate:
                tables_to_validate = [table_info.get("name") for table_info in dict_tables.get("tables", [])]
                
            # Validar cada tabla identificada
            for table_name in tables_to_validate:
                self._validate_single_table(state, table_name)
                
            # Si no se pudieron validar todas las tablas, registrar el error
            if state.table_validation_errors:
                logging.warning(f"Errores en validación de tablas: {state.table_validation_errors}")
                
        except Exception as e:
            logging.error(f"Error en table_validator: {str(e)}")
            state.table_validation_errors.append(f"Error general: {str(e)}")
            
        return state
        
    def _validate_single_table(self, state: FlowState, table_name: str) -> None:
        """
        Valida una tabla específica ejecutando consultas de muestra
        y almacenando los resultados en el estado.
        """
        # Verificar si la tabla está definida en dict_tables
        table_definition = None
        for table_info in dict_tables.get("tables", []):
            if table_info.get("name") == table_name:
                table_definition = table_info
                break
                
        if not table_definition:
            state.table_validation_errors.append(f"Tabla '{table_name}' no encontrada en la definición")
            return
            
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Consulta 1: Obtener muestra de datos (máximo 10 filas)
            sample_query = f"SELECT * FROM {table_name} LIMIT 10"
            cursor.execute(sample_query)
            sample_rows = cursor.fetchall()
            
            # Convertir a lista de diccionarios
            sample_data = [dict(row) for row in sample_rows]
            
            # Consulta 2: Contar registros totales
            count_query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
            cursor.execute(count_query)
            count_result = cursor.fetchone()
            total_rows = count_result["total_rows"] if count_result else 0
            
            # Consulta 3: Para cada columna numérica, obtener estadísticas básicas
            column_stats = {}
            for column_info in table_definition.get("columns", []):
                column_name = column_info.get("name")
                column_type = column_info.get("type", "").lower()
                
                if column_type in ["integer", "double precision", "numeric", "decimal", "float"]:
                    try:
                        stats_query = f"""
                        SELECT 
                            COUNT(*) AS count,
                            COUNT(*) FILTER(WHERE {column_name} IS NULL) AS null_count,
                            AVG({column_name}) AS avg,
                            MIN({column_name}) AS min,
                            MAX({column_name}) AS max
                        FROM {table_name}
                        """
                        cursor.execute(stats_query)
                        stats = cursor.fetchone()
                        if stats:
                            column_stats[column_name] = dict(stats)
                    except Exception as e:
                        logging.warning(f"Error al obtener estadísticas para {column_name}: {str(e)}")
                        column_stats[column_name] = {"error": str(e)}
                
            # Guardar toda la información validada
            state.validated_tables[table_name] = {
                "exists": True,
                "definition": table_definition,
                "total_rows": total_rows,
                "column_stats": column_stats
            }
            
            state.table_samples[table_name] = sample_data
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            error_msg = f"Error validando tabla '{table_name}': {str(e)}"
            state.table_validation_errors.append(error_msg)
            state.validated_tables[table_name] = {
                "exists": False,
                "error": str(e),
                "definition": table_definition
            }
            logging.error(error_msg)

    def ambiguity_detector(self, state: FlowState) -> FlowState:
        """Detecta si la consulta es ambigua o falta información"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        prompt = f"""
        Analiza la siguiente consulta de usuario para determinar si es ambigua o falta información importante.
        
        Consulta del usuario: {messages_content}
        Análisis previo: {state.agent_analysis}
        
        La base de datos tiene esta estructura:
        {json.dumps(dict_tables)}
        
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
        - "Muestra datos" → AMBIGUOUS: ¿Quieres ver todos los registros o aplicar algún filtro?
        - "Registros con valor máximo" → INSUFFICIENT_DATA: No se especifica la columna a evaluar.
        - "Lista registros del campo categoría 'A'" → CLEAR"""
        
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
    
    def sql_agent(self, state: FlowState) -> FlowState:
        """Agente especializado en generar consultas SQL (simple o múltiples)"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        # Primero, determinar si se necesitan múltiples queries
        complexity_prompt= f"""
        fAnaliza si la siguiente consulta requiere múltiples queries SQL para responder completamente.
        Entrada:
        - Consulta: {messages_content}
        - Análisis previo: {state.agent_analysis}

        Responde ÚNICAMENTE con:
        - SINGLE: si se puede responder con una sola query
        - MULTIPLE: si necesita múltiples queries para análisis completo

        Reglas para MULTIPLE (si se cumple UNA, responde MULTIPLE):
        1) Comparaciones entre grupos/departamentos o múltiples cortes.
        2) Mezcla de agregados y listados/detalles.
        3) Métricas derivadas que se reutilizan (medianas/percentiles/imputaciones) o submuestras intermedias.
        4) Temporal con más de un grano o varias ventanas.
        5) Validaciones previas necesarias (existencia de datos, null-rate, dominios canónicos, esquema incierto).
        6) Unión de múltiples fuentes/tablas con lógicas distintas o claves dudosas.
        7) Sensibilidad/QA: antes-después, A/B, outliers, sesgos.
        8) Performance que recomienda etapas (CTEs pesadas, materializaciones).

        Reglas para SINGLE:
        A) Una sola tabla o joins triviales.
        B) Un solo grano.
        C) Solo agregación o solo detalle.
        D) Sin dependencias de cálculos previos ni validaciones críticas.

        Política de reconocimiento (solo si eliges MULTIPLE):
        - Por cada tabla involucrada, generar primero:
        1) SELECT * FROM <schema>.<tabla> LIMIT 10;
        2) SELECT DISTINCT <campos_de_filtro_principales> FROM <schema>.<tabla>;
        3) SELECT COUNT(*) AS n, COUNT(*) FILTER(WHERE <col_num> IS NULL) AS n_null,
            MIN(<col_fecha>) AS min_date, MAX(<col_fecha>) AS max_date FROM <schema>.<tabla>;

        Política de filtrado y normalización (guía para la fase de generación):
        - Estandariza texto: usar UPPER(TRIM(col)) para comparar con literales canónicos.
        - Relajación progresiva si 0 filas: 1) quitar topes secundarios, 2) expandir dominios, 3) aflojar fechas, 4) revisar casing/espacios.
        - Evitar ORDER BY en subconsultas salvo necesario. Solo ordenar en el resultado final.
        - Dialecto por defecto: PostgreSQL. Si una función no existe, usar alternativa ANSI.

        Criterio de incertidumbre:
        - Si hay ambigüedad material sobre datos, esquemas o filtros, elige MULTIPLE.

        Ejemplos:
        - “Lista empleados” → SINGLE
        - “Análisis completo de salarios por departamento con empleados mejor pagados” → MULTIPLE
        - “Estadísticas de contratación por año y departamento” → MULTIPLE
        - “Calcular promedio de ‘Perfect Order’ imputando mediana por zona y compararlo entre ‘Wealthy’ y ‘Non Wealthy’” → MULTIPLE
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
        
        # Preparar información de tablas validadas
        validated_tables_info = {}
        for table_name, validation_data in state.validated_tables.items():
            if validation_data.get("exists", False):
                sample_data = state.table_samples.get(table_name, [])
                sample_snippet = sample_data[:3] if sample_data else []
                
                validated_tables_info[table_name] = {
                    "definition": validation_data.get("definition", {}),
                    "total_rows": validation_data.get("total_rows", 0),
                    "sample_data": sample_snippet,
                    "column_stats": validation_data.get("column_stats", {})
                }
        
        # Construir la sección de errores anteriores si existen
        previous_errors = ""
        if state.retry_count > 0 and state.error_messages:
            previous_errors = "\nERRORES PREVIOS (debes corregirlos):\n"
            for i, error in enumerate(state.error_messages):
                previous_errors += f"{i+1}. {error}\n"
                
            # Si hay una consulta previa que falló, incluirla
            if state.sql_query and not state.sql_query.startswith("ERROR") and not state.sql_query == "NO_SQL_NEEDED":
                previous_errors += f"\nConsulta anterior que falló:\n{state.sql_query}\n"
        
        prompt = f"""
        Consulta: {messages_content}
        Análisis previo: {state.agent_analysis}
        Intento: {state.retry_count + 1} de {state.max_retries}
        
        INFORMACIÓN DE TABLAS VALIDADAS:
        {json.dumps(validated_tables_info, indent=2)}
        {previous_errors}
        {prompt_single_query}
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
        
        # Preparar información de tablas validadas
        validated_tables_info = {}
        for table_name, validation_data in state.validated_tables.items():
            if validation_data.get("exists", False):
                sample_data = state.table_samples.get(table_name, [])
                sample_snippet = sample_data[:3] if sample_data else []
                
                validated_tables_info[table_name] = {
                    "definition": validation_data.get("definition", {}),
                    "total_rows": validation_data.get("total_rows", 0),
                    "sample_data": sample_snippet,
                    "column_stats": validation_data.get("column_stats", {})
                }
        
        # Construir la sección de errores anteriores si existen
        previous_errors = ""
        if state.retry_count > 0 and state.error_messages:
            previous_errors = "\nERRORES PREVIOS (debes corregirlos):\n"
            for i, error in enumerate(state.error_messages):
                previous_errors += f"{i+1}. {error}\n"
                
            # Si hay consultas previas que fallaron, incluirlas
            if state.sql_queries:
                previous_errors += f"\nConsultas anteriores que fallaron:\n"
                for i, query in enumerate(state.sql_queries):
                    previous_errors += f"QUERY_{i+1}: {query}\n"
        
        prompt = f"""
        Consulta: {messages_content}
        Análisis previo: {state.agent_analysis}
        Intento: {state.retry_count + 1} de {state.max_retries}
        
        INFORMACIÓN DE TABLAS VALIDADAS:
        {json.dumps(validated_tables_info, indent=2)}
        {previous_errors}
        {prompt_multi_query}
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
            
            # Limitar a máximo 300 filas para retorno, pero mantener info completa
            limited_results = all_results[:300] if len(all_results) > 60 else all_results
            
            # Crear estructura consistente
            state.sql_results = {
                "query": state.sql_query,
                "total_rows": len(all_results),
                "returned_rows": len(limited_results),
                "data": limited_results,
                "truncated": len(all_results) > 60
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
                        "truncated": len(all_query_results) > 60,
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
            state.needs_retry = True
            state.query_evaluation = {"valid": False, "reason": "Consulta SQL inválida o con errores"}
            state.error_messages.append("La consulta SQL es inválida o contiene errores de sintaxis.")
            return state
            
        if state.sql_query == "NO_SQL_NEEDED":
            state.is_sql_valid = True
            state.needs_retry = False
            state.query_evaluation = {"valid": True, "reason": "No se requiere consulta SQL"}
            return state
            
        # Evaluar si hay errores en los resultados
        if isinstance(state.sql_results, dict):
            if "error" in state.sql_results:
                error_msg = state.sql_results.get("error", "Error desconocido")
                # Incrementar contador de reintentos y verificar límite
                state.retry_count += 1
                state.error_messages.append(f"Intento {state.retry_count}: {error_msg}")
                
                if state.retry_count >= state.max_retries:
                    # Alcanzó límite de reintentos - continuar al siguiente paso pero marcar como fallido
                    state.is_sql_valid = False
                    state.needs_retry = False
                    state.query_evaluation = {
                        "valid": False, 
                        "reason": f"Error después de {state.retry_count} intentos: {error_msg}",
                        "errors": state.error_messages,
                        "continue_anyway": True  # Continuar al siguiente paso
                    }
                else:
                    # Todavía puede reintentar
                    state.is_sql_valid = False
                    state.needs_retry = True
                    state.query_evaluation = {
                        "valid": False, 
                        "reason": f"Error en ejecución: {error_msg}",
                        "errors": state.error_messages,
                        "attempt": state.retry_count,
                        "max_attempts": state.max_retries
                    }
            else:
                # Consulta exitosa
                state.is_sql_valid = True
                state.needs_retry = False
                if state.requires_multiple_queries:
                    successful_queries = state.sql_results.get("successful_queries", 0)
                    total_queries = state.sql_results.get("total_queries", 0)
                    
                    # Verificar si hay suficientes consultas exitosas
                    if successful_queries < total_queries * 0.5:  # Si menos del 50% fueron exitosas
                        state.retry_count += 1
                        state.error_messages.append(f"Intento {state.retry_count}: Solo {successful_queries} de {total_queries} consultas fueron exitosas")
                        
                        if state.retry_count >= state.max_retries:
                            state.is_sql_valid = False
                            state.needs_retry = False
                            state.query_evaluation = {
                                "valid": False,
                                "reason": f"Demasiadas consultas fallaron después de {state.retry_count} intentos",
                                "errors": state.error_messages,
                                "continue_anyway": True
                            }
                        else:
                            state.is_sql_valid = False
                            state.needs_retry = True
                            state.query_evaluation = {
                                "valid": False,
                                "reason": f"Solo {successful_queries} de {total_queries} consultas fueron exitosas. Reintentando...",
                                "errors": state.error_messages,
                                "attempt": state.retry_count,
                                "max_attempts": state.max_retries
                            }
                    else:
                        # Suficientes consultas exitosas
                        state.query_evaluation = {
                            "valid": True,
                            "reason": f"Múltiples consultas ejecutadas: {successful_queries}/{total_queries} exitosas",
                            "total_rows_found": state.sql_results.get("total_rows_found", 0),
                            "total_rows_returned": state.sql_results.get("total_rows_returned", 0)
                        }
                else:
                    # Consulta simple exitosa
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
                error_msg = state.sql_results[0].get("error", "Error desconocido")
                # Incrementar contador de reintentos
                state.retry_count += 1
                state.error_messages.append(f"Intento {state.retry_count}: {error_msg}")
                
                if state.retry_count >= state.max_retries:
                    state.is_sql_valid = False
                    state.needs_retry = False
                    state.query_evaluation = {
                        "valid": False, 
                        "reason": f"Error después de {state.retry_count} intentos: {error_msg}",
                        "errors": state.error_messages,
                        "continue_anyway": True
                    }
                else:
                    state.is_sql_valid = False
                    state.needs_retry = True
                    state.query_evaluation = {
                        "valid": False, 
                        "reason": f"Error en ejecución: {error_msg}",
                        "errors": state.error_messages,
                        "attempt": state.retry_count,
                        "max_attempts": state.max_retries
                    }
            else:
                # Resultado exitoso en formato legacy
                state.is_sql_valid = True
                state.needs_retry = False
                state.query_evaluation = {
                    "valid": True, 
                    "reason": "Consulta ejecutada exitosamente",
                    "rows_returned": len(state.sql_results) if state.sql_results else 0
                }
            
        return state

    def data_analyst(self, state: FlowState) -> FlowState:
        """Analiza los resultados y genera insights"""
        
        messages_content = self._extract_content_from_messages(state.messages)
        
        # Preparar información de tablas validadas para el análisis
        validated_tables_summary = {}
        for table_name, validation_data in state.validated_tables.items():
            if validation_data.get("exists", False):
                validated_tables_summary[table_name] = {
                    "total_rows": validation_data.get("total_rows", 0),
                    "columns": [col.get("name") for col in validation_data.get("definition", {}).get("columns", [])]
                }
        
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
            
            Información de tablas validadas:
            {json.dumps(validated_tables_summary)}
            
            Resultados detallados:
            {self._format_multiple_results_for_analysis(state.all_sql_results)}
            
            Proporciona:
            1. **Resumen ejecutivo** de todos los hallazgos
            2. **Análisis integrado** combinando datos de todas las consultas
            3. **Insights clave** y patrones identificados
            4. **Conclusiones** y respuesta directa a la pregunta original
            5. **Recomendaciones** basadas en el análisis completo
            
            Estructura tu respuesta de manera clara y profesional, destacando los puntos más importantes.
            Utiliza la información de la estructura y datos de muestra de las tablas para enriquecer tu análisis.
            """
        else:
            # Análisis de query simple
            prompt = f"""
            Analiza los siguientes resultados de la consulta SQL y genera insights útiles:
            
            Consulta original: {messages_content}
            Consulta SQL ejecutada: {state.sql_query}
            
            Información de tablas validadas:
            {json.dumps(validated_tables_summary)}
            
            Resultados: {state.sql_results}
            
            Proporciona:
            1. Un resumen claro de los resultados
            2. Insights relevantes y patrones identificados
            3. Respuesta directa a la pregunta del usuario
            4. Recomendaciones si aplica
            
            Responde de manera clara y profesional.
            Utiliza la información de la estructura y datos de muestra de las tablas para enriquecer tu análisis.
            """
        
        try:
            response = self.llm.invoke(prompt).content
            state.data_analysis = response
        except Exception as e:
            logging.error(f"Error en data_analyst: {str(e)}")
            state.data_analysis = f"Error en análisis: {str(e)}"
            
        return state

    
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
        import decimal 
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, BaseModel):
            return obj.dict() if hasattr(obj, 'dict') else obj.model_dump()
        if isinstance(obj, Sequence) and not isinstance(obj, str):
            return [AnalystIAGraph._serialise(o) for o in obj]
        if isinstance(obj, dict):
            return {k: AnalystIAGraph._serialise(v) for k, v in obj.items()}
        return obj
        
    
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
                'validated_tables': self._serialise(final_state.get('validated_tables', {})),
                'table_validation_errors': final_state.get('table_validation_errors', []),
                'retry_count': final_state.get('retry_count', 0),
                'error_messages': final_state.get('error_messages', []),
                'needs_retry': final_state.get('needs_retry', False),
                'summary': final_state.get('data_analysis') or final_state.get('agent_analysis') or "No se pudo generar resumen"
            }
            
            return result
            
        except Exception as e:
            logging.error(f"Error en RetellIAGraph.run: {str(e)}")
            return {
                'error': f"Error procesando consulta: {str(e)}",
                'summary': f"Error: {str(e)}"
            }