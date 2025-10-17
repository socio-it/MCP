import os
from typing import Dict, Any
from fastmcp import FastMCP
from dotenv import load_dotenv

from agent import AnalystIAGraph
from prompts import prompt_comparador, prompt_cronista_temporal, prompt_curador_de_metricas, prompt_orquestador_de_agregacion, prompt_trade_offs

load_dotenv()
app = FastMCP("company-db-sever")

def get_analystIAGraph(messages: str, prompt: str) -> Dict[str, Any]:
    """Generico: Genera un resumen y análisis inteligente de consultas sobre empleados"""
    import os, json
    try:
        if isinstance(messages, str):
            try:
                messages_list = json.loads(messages)
            except json.JSONDecodeError:
                messages_list = [{"role": "user", "content": messages}]
        else:
            messages_list = messages
        if not os.environ.get("OPENAI_API_KEY"):
            return {"error": "OPENAI_API_KEY no está configurada"}
        engine = AnalystIAGraph(agent_prompt=prompt)
        result = engine.run(messages_list)

        safe_result = json.dumps(result, ensure_ascii=False, indent=2)

        return {
            "success": True,
            "summary": result.get("summary", "No se pudo generar resumen"),
            "analysis": safe_result,  # todo el resultado en texto plano JSON
            "sql_query": result.get("sql_query"),
            "sql_queries": result.get("sql_queries", []),
            "clarification_needed": result.get("clarification_needed"),
            "is_ambiguous": result.get("is_ambiguous", False),
            "insufficient_data": result.get("insufficient_data", False),
            "requires_multiple_queries": result.get("requires_multiple_queries", False),
            # estos dos campos se devuelven como texto plano para evitar conflicto
            "data_results": json.dumps(result.get("sql_results", None), ensure_ascii=False),
            "all_sql_results": json.dumps(result.get("all_sql_results", []), ensure_ascii=False)
        }

    except Exception as e:
        return {"error": f"Error al generar resumen: {str(e)}"}
    

@app.tool
def curador_de_metricas(messages: str) -> Dict[str, Any]:
   """
   Analista multiagente con conexion a data que hace:
   consultas de filtrado/ranking de KPIs. Entrega una especificación lista para el generador SQL.
   Objetivo: 
   Definir KPI, nivel, tiempo, filtros, orden, límite, baselines y criterios de calidad."""

   return get_analystIAGraph(messages, prompt_curador_de_metricas)

@app.tool
def comparador(messages: str) -> Dict[str, Any]:
   """
   Analista multiagente con conexion a data que hace:
   Desmenuza la consulta de comparación A vs B y entrega una especificación.
   Objetivo:
   Definir KPI de comparación, cohortes A/B, controles de mezcla, tiempo, filtros, diferenciales (abs, %) y campos requeridos en la salida."""

   return get_analystIAGraph(messages, prompt_comparador)

@app.tool
def cronista_temporal(messages: str) -> Dict[str, Any]:
   """
   Analista multiagente con conexion a data que hace:
   Analista para Cronista Temporal. Desmenuza consultas de evolución en el tiempo y entrega una especificación lista.
   Objetivo:
   Definir KPI temporal, granularidad, rango, comparativos entre periodos, detección de quiebres, nivel de análisis, filtros y criterios de calidad."""
   return get_analystIAGraph(messages, prompt_cronista_temporal)

@app.tool
def orquestador_de_agregacion(messages: str) -> Dict[str, Any]:
   """
   Analista multiagente con conexion a data que hace:
   Analista para Orquestador de Agregaciones. Desmenuza resúmenes por jerarquías y entrega una especificación lista.                
   Objetivo:
   Definir KPI agregado (directo o ponderado), nivel jerárquico, ponderador, cobertura, reconciliación padre–hijo, filtros y criterios de calidad."""
   return get_analystIAGraph(messages, prompt_orquestador_de_agregacion)

@app.tool
def trade_offs(messages: str) -> Dict[str, Any]:
   """
   Analista multiagente con conexion a data que hace:
   Analista para Buscador de Trade-offs. Desmenuza cruces “alto X / bajo Y” y entrega una especificación lista.
   Objetivo:
   Definir X y Y, nivel de análisis, umbrales alto/bajo, score de priorización, tiempo, filtros y criterios de calidad."""
   return get_analystIAGraph(messages, prompt_trade_offs)

    
if __name__ == "__main__":
    app.run(transport="sse", host="0.0.0.0", port=3000)