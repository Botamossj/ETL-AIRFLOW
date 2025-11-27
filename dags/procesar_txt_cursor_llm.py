"""
DAG que procesa archivos TXT de contratos usando IA (LLM) para extracción completa de datos.

Flujo optimizado:
lectura -> extracción de código -> verificación en BD -> pre-filtro -> LLM (optimizado) -> inserción -> cursor SQL.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import google.generativeai as genai
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("TXT_SOURCE_DIR", "/opt/airflow/data/txt_convertidos"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID", "contratos_postgres")
SYNC_CONTRATOS_TABLE = os.environ.get("SYNC_CONTRATOS_TABLE", "sync_contratos")
TXT_CONTROL_TABLE = os.environ.get("TXT_CONTROL_TABLE", "txt_control")
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")  # IMPORTANTE: Definir en .env para evitar exposición
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "60"))

# Campos esperados del contrato
CAMPOS_CONTRATO = ["titulo", "proveedor", "monto", "moneda", "estado", "fecha_publicacion", "resumen"]


def _get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def _hash_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _ensure_data_dir() -> None:
    if not DATA_DIR.exists():
        logger.warning("El directorio %s no existe. Creándolo.", DATA_DIR)
        DATA_DIR.mkdir(parents=True, exist_ok=True)


def _extract_codigo_proceso(text: str, fallback: str) -> str:
    patrones = [
        r"(?:codigo|c[oó]d)\s*(?:del\s+)?proceso[:\s-]*([A-Z0-9\-_/]{6,})",
        r"(?:proc(?:eso)?\s*no\.?|expediente)[:\s-]*([A-Z0-9\-_/]{6,})",
    ]
    for patron in patrones:
        match = re.search(patron, text, re.IGNORECASE)
        if match:
            return match.group(1).strip().upper()
    return fallback.upper()


def _parse_fecha_publicacion(fecha_str: Any) -> Any:
    """
    Parsea fecha_publicacion a formato date o None.
    
    Args:
        fecha_str: String de fecha o None
        
    Returns:
        datetime.date o None
    """
    if not fecha_str:
        return None
    
    if isinstance(fecha_str, datetime):
        return fecha_str.date()
    
    if not isinstance(fecha_str, str):
        return None
    
    # Limpiar string
    fecha_str = fecha_str.strip()
    
    # Formatos comunes
    formatos = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%d-%m-%Y",
        "%Y/%m/%d",
    ]
    
    for fmt in formatos:
        try:
            # Intentar parsear solo la parte de fecha (antes de espacios)
            fecha_limpia = fecha_str.split()[0] if " " in fecha_str else fecha_str
            return datetime.strptime(fecha_limpia, fmt).date()
        except ValueError:
            continue
    
    logger.warning("No se pudo parsear fecha_publicacion: %s", fecha_str)
    return None


def _call_llm_optimizado(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Llama al LLM (Gemini-Pro) con prompt optimizado para extraer datos del contrato.
    Siempre devuelve JSON válido con todos los campos esperados.
    
    Args:
        payload: Diccionario con codigo_proceso, texto_filtrado, file_name
        
    Returns:
        Diccionario con datos extraídos del contrato
        
    Raises:
        AirflowSkipException: Si el LLM no está disponible o falla
    """
    if not LLM_API_KEY:
        logger.warning("LLM_API_KEY no definido. Saltando procesamiento con IA.")
        raise AirflowSkipException("LLM_API_KEY no configurado")
    
    try:
        # Configurar Gemini
        genai.configure(api_key=LLM_API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        
        codigo_proceso = payload.get("codigo_proceso", "N/A")
        texto_filtrado = payload.get("texto_filtrado", "")
        file_name = payload.get("file_name", "N/A")
        
        # Prompt optimizado
        prompt = f"""Eres un modelo experto en análisis de contratos públicos del Ecuador.

Analiza el siguiente texto de un contrato público y extrae la información relevante.

Código de proceso: {codigo_proceso}
Archivo: {file_name}

Texto del contrato:
{texto_filtrado[:30000]}  # Limitar tamaño

Extrae y devuelve SIEMPRE un JSON válido con la siguiente información:
- titulo: Título o objeto del contrato (string o null)
- proveedor: Proveedor o contratista (string o null)
- monto: Monto o valor del contrato (número o null)
- moneda: Moneda del contrato, ej: USD, EUR (string o null)
- estado: Estado del contrato, ej: Vigente, Finalizado (string o null)
- fecha_publicacion: Fecha de publicación en formato YYYY-MM-DD (string o null)
- resumen: Resumen breve del contrato (string o null)

Reglas:
- Si un dato no existe, usa null.
- No inventes información.
- La fecha debe estar en formato YYYY-MM-DD o null.
- El monto debe ser un número (sin símbolos de moneda).
- Limpia y normaliza los textos.

Ejemplo de respuesta esperada:
{{
  "titulo": "Contrato de servicios de consultoría",
  "proveedor": "EMPRESA EJEMPLO S.A.",
  "monto": 50000.00,
  "moneda": "USD",
  "estado": "Vigente",
  "fecha_publicacion": "2024-01-15",
  "resumen": "Contrato para servicios de consultoría técnica..."
}}

IMPORTANTE: Responde SOLO con el JSON válido, sin texto adicional antes o después."""
        
        # Llamar a Gemini
        response = model.generate_content(prompt)
        texto_respuesta = response.text.strip()
        
        # Limpiar la respuesta: remover markdown code blocks si existen
        texto_respuesta = re.sub(r'```json\s*', '', texto_respuesta)
        texto_respuesta = re.sub(r'```\s*', '', texto_respuesta)
        texto_respuesta = texto_respuesta.strip()
        
        # Intentar parsear JSON
        try:
            # Buscar JSON en la respuesta (puede haber texto antes/después)
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', texto_respuesta, re.DOTALL)
            if json_match:
                datos_extraidos = json.loads(json_match.group())
            else:
                # Intentar parsear directamente
                datos_extraidos = json.loads(texto_respuesta)
        except json.JSONDecodeError as e:
            logger.error("Error al parsear JSON del LLM: %s. Respuesta: %s", e, texto_respuesta[:500])
            raise AirflowSkipException(f"LLM no devolvió JSON válido: {e}")
        
        # Validar y completar campos esperados
        resultado = {}
        for campo in CAMPOS_CONTRATO:
            valor = datos_extraidos.get(campo)
            
            if campo == "fecha_publicacion":
                # Parsear fecha
                resultado[campo] = _parse_fecha_publicacion(valor)
            elif campo == "monto":
                # Convertir monto a float si es posible
                if valor is not None:
                    try:
                        if isinstance(valor, str):
                            # Limpiar string: remover símbolos de moneda y espacios
                            valor_limpio = re.sub(r'[^\d.,]', '', valor.replace(',', '.'))
                            resultado[campo] = float(valor_limpio) if valor_limpio else None
                        else:
                            resultado[campo] = float(valor)
                    except (ValueError, TypeError):
                        resultado[campo] = None
                else:
                    resultado[campo] = None
            elif campo == "resumen":
                # Resumen: usar texto completo o truncar
                if valor:
                    resultado[campo] = str(valor)[:2000]  # Limitar longitud
                else:
                    resultado[campo] = texto_filtrado[:1024] if texto_filtrado else None
            else:
                # Campos de texto: limpiar y validar
                if valor is not None and valor != "":
                    valor_str = str(valor).strip()
                    if valor_str.lower() in ["null", "none", "n/a", "na"]:
                        resultado[campo] = None
                    else:
                        resultado[campo] = valor_str
                else:
                    resultado[campo] = None
        
        logger.info(
            "Datos extraídos con IA para %s (proceso: %s). Campos encontrados: %s",
            file_name,
            codigo_proceso,
            [k for k, v in resultado.items() if v is not None],
        )
        
        return {
            "status": "success",
            "titulo": resultado.get("titulo"),
            "proveedor": resultado.get("proveedor"),
            "monto": resultado.get("monto"),
            "moneda": resultado.get("moneda"),
            "estado": resultado.get("estado"),
            "fecha_publicacion": resultado.get("fecha_publicacion"),
            "resumen": resultado.get("resumen"),
            "metadata": {
                "model": "gemini-pro",
                "codigo_proceso": codigo_proceso,
                "file_name": file_name,
            },
        }
        
    except AirflowSkipException:
        raise
    except Exception as exc:
        logger.exception("Error al llamar a Gemini API")
        raise AirflowSkipException(f"Error en LLM: {exc}") from exc


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="procesar_txt_cursor_llm",
    description="Procesa archivos TXT de contratos usando IA (LLM optimizado) para extracción completa de datos",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["txt", "contratos", "llm"],
)
def procesar_txt_cursor_llm():
    @task
    def listar_archivos_pendientes() -> List[str]:
        _ensure_data_dir()
        # Buscar recursivamente en subdirectorios
        archivos = sorted(DATA_DIR.rglob("*.txt"))
        if not archivos:
            logger.info("No se encontraron archivos en %s.", DATA_DIR)
            return []

        hook = _get_postgres_hook()
        procesados: set[str] = set()
        try:
            registros = hook.get_records(
                f"SELECT file_name FROM {TXT_CONTROL_TABLE} WHERE procesado = TRUE"
            )
            procesados = {row[0] for row in registros}
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "No fue posible cargar el historial de txt_control (%s). Se procesarán sin filtro.",
                exc,
            )

        pendientes: List[str] = []
        for archivo in archivos:
            if archivo.name in procesados:
                continue
            pendientes.append(str(archivo))
            if len(pendientes) >= BATCH_SIZE:
                break

        logger.info("Archivos pendientes detectados: %d", len(pendientes))
        return pendientes

    @task
    def lectura(file_path: str) -> Dict[str, Any]:
        ruta = Path(file_path)
        if not ruta.exists():
            raise FileNotFoundError(f"No se encontró el archivo {file_path}")

        contenido = ruta.read_text(encoding="utf-8", errors="ignore")
        logger.debug("Lectura completada de %s", file_path)
        return {
            "file_path": file_path,
            "file_name": ruta.name,
            "contenido": contenido,
            "hash": _hash_text(contenido),
        }

    @task
    def extraccion_codigo(data: Dict[str, Any]) -> Dict[str, Any]:
        codigo = _extract_codigo_proceso(data["contenido"], fallback=Path(data["file_path"]).stem)
        data["codigo_proceso"] = codigo
        logger.debug("Código de proceso extraído para %s: %s", data["file_name"], codigo)
        return data

    @task
    def verificacion_bd(data: Dict[str, Any]) -> Dict[str, Any]:
        hook = _get_postgres_hook()
        conn = hook.get_conn()
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT codigo_proceso, hash_contenido, fecha_actualizacion
                FROM {SYNC_CONTRATOS_TABLE}
                WHERE codigo_proceso = %s
                """,
                (data["codigo_proceso"],),
            )
            row = cursor.fetchone()

        if row:
            _, hash_existente, fecha_actualizacion = row
            if hash_existente == data["hash"]:
                data["skip"] = True
                data["motivo_skip"] = "Hash coincide; registro ya actualizado."
                logger.info(
                    "El archivo %s ya se encontraba procesado (actualizado %s).",
                    data["file_name"],
                    fecha_actualizacion,
                )
            else:
                data["skip"] = False
                data["registro_existente"] = True
        else:
            data["skip"] = False
            data["registro_existente"] = False

        return data

    @task
    def pre_filtro(data: Dict[str, Any]) -> Dict[str, Any]:
        if data.get("skip"):
            raise AirflowSkipException(data.get("motivo_skip", "Registro ya tratado."))

        contenido = data["contenido"]
        if len(contenido) < 200:
            raise AirflowSkipException("Contenido insuficiente para análisis.")

        # Se conserva solo texto relevante por heurística simple
        lineas_clave = [
            linea
            for linea in contenido.splitlines()
            if any(palabra in linea.lower() for palabra in ("contrato", "proveedor", "monto", "objeto"))
        ]
        texto_filtrado = "\n".join(lineas_clave) or contenido[:2000]
        data["texto_filtrado"] = texto_filtrado
        data["pre_filtro_timestamp"] = datetime.utcnow().isoformat()
        return data

    @task
    def llm(data: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "codigo_proceso": data["codigo_proceso"],
            "texto_filtrado": data["texto_filtrado"],
            "file_name": data["file_name"],
        }
        try:
            resultado = _call_llm_optimizado(payload)
        except AirflowSkipException:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Fallo al invocar el LLM para %s", data["file_name"])
            raise AirflowSkipException(f"LLM no disponible: {exc}") from exc

        data["llm_output"] = resultado
        return data

    @task
    def insercion(data: Dict[str, Any]) -> Dict[str, Any]:
        hook = _get_postgres_hook()
        conn = hook.get_conn()
        conn.autocommit = True
        metadata = {
            "llm_status": data["llm_output"].get("status"),
            "llm_metadata": data["llm_output"].get("metadata"),
            "pre_filtro_timestamp": data.get("pre_filtro_timestamp"),
        }
        
        # Extraer campos del LLM (ya validados y parseados)
        llm_output = data["llm_output"]
        titulo = llm_output.get("titulo")
        proveedor = llm_output.get("proveedor")
        monto = llm_output.get("monto")
        moneda = llm_output.get("moneda")
        estado = llm_output.get("estado")
        fecha_publicacion = llm_output.get("fecha_publicacion")  # Ya parseada como date o None
        resumen = llm_output.get("resumen") or data["texto_filtrado"][:1024]
        
        upsert_sql = f"""
            INSERT INTO {SYNC_CONTRATOS_TABLE} (
                codigo_proceso,
                titulo,
                proveedor,
                monto,
                moneda,
                estado,
                fecha_publicacion,
                resumen,
                hash_contenido,
                fuente_archivo,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (codigo_proceso)
            DO UPDATE SET
                titulo = COALESCE(EXCLUDED.titulo, {SYNC_CONTRATOS_TABLE}.titulo),
                proveedor = COALESCE(EXCLUDED.proveedor, {SYNC_CONTRATOS_TABLE}.proveedor),
                monto = COALESCE(EXCLUDED.monto, {SYNC_CONTRATOS_TABLE}.monto),
                moneda = COALESCE(EXCLUDED.moneda, {SYNC_CONTRATOS_TABLE}.moneda),
                estado = COALESCE(EXCLUDED.estado, {SYNC_CONTRATOS_TABLE}.estado),
                fecha_publicacion = COALESCE(EXCLUDED.fecha_publicacion, {SYNC_CONTRATOS_TABLE}.fecha_publicacion),
                resumen = EXCLUDED.resumen,
                hash_contenido = EXCLUDED.hash_contenido,
                fuente_archivo = EXCLUDED.fuente_archivo,
                metadata = COALESCE({SYNC_CONTRATOS_TABLE}.metadata, '{{}}'::jsonb) || EXCLUDED.metadata,
                fecha_actualizacion = NOW();
        """
        with conn.cursor() as cursor:
            cursor.execute(
                upsert_sql,
                (
                    data["codigo_proceso"],
                    titulo,
                    proveedor,
                    monto,
                    moneda,
                    estado,
                    fecha_publicacion,
                    resumen,
                    data["hash"],
                    data["file_name"],
                    json.dumps(metadata),
                ),
            )

        data["upsert_timestamp"] = datetime.utcnow().isoformat()
        return data

    @task
    def cursor_sql(data: Dict[str, Any]) -> str:
        hook = _get_postgres_hook()
        conn = hook.get_conn()
        conn.autocommit = True
        upsert_txt = f"""
            INSERT INTO {TXT_CONTROL_TABLE} (
                file_name,
                codigo_proceso,
                procesado,
                fecha_procesado,
                intentos,
                fecha_ultima_revision
            )
            VALUES (%s, %s, TRUE, NOW(), 1, NOW())
            ON CONFLICT (file_name)
            DO UPDATE SET
                codigo_proceso = EXCLUDED.codigo_proceso,
                procesado = TRUE,
                fecha_procesado = NOW(),
                intentos = {TXT_CONTROL_TABLE}.intentos + 1,
                fecha_ultima_revision = NOW();
        """
        with conn.cursor() as cursor:
            cursor.execute(
                upsert_txt,
                (
                    data["file_name"],
                    data["codigo_proceso"],
                ),
            )

        logger.info("Archivo %s marcado como procesado en %s.", data["file_name"], TXT_CONTROL_TABLE)
        return data["file_name"]

    archivos = listar_archivos_pendientes()
    contenidos = lectura.expand(file_path=archivos)
    codigos = extraccion_codigo.expand(data=contenidos)
    verificados = verificacion_bd.expand(data=codigos)
    filtrados = pre_filtro.expand(data=verificados)
    llm_outputs = llm.expand(data=filtrados)
    inserciones = insercion.expand(data=llm_outputs)
    cursor_sql.expand(data=inserciones)


procesar_txt_cursor_llm()
