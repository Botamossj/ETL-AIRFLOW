"""
DAG ETL para extracción de información de contratistas desde archivos TXT usando IA (100% LLM).

Este DAG procesa archivos TXT de contratos ubicados en una ruta específica,
extrae información del contratista (razón social, RUC, teléfono, mail, domicilio)
usando IA (DeepSeek) y actualiza únicamente los registros existentes en PostgreSQL.

IMPORTANTE:
- Solo actualiza registros existentes (no crea nuevos)
- El código de proceso se extrae del nombre del archivo TXT
- Solo actualiza: razon_social, ruc, telefono, mail, domicilio
- Usa 100% IA para extracción (sin regex)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import google.generativeai as genai
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Importar utilidades ETL (solo para código de proceso)
try:
    from include.etl_utils import extract_codigo_proceso_from_filename
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    from etl_utils import extract_codigo_proceso_from_filename


logger = logging.getLogger(__name__)

# Configuración desde variables de entorno
TXT_SOURCE_DIR = Path(
    os.environ.get("TXT_SOURCE_DIR", "/opt/airflow/data/txt_convertidos")
)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID", "oppdesarrollo_postgres")
CONTRATOS_TABLE = "public.sync_contratos"
LLM_API_KEY = os.environ.get("LLM_API_KEY", "AIzaSyCXagg3BcPlPc_v_wWh6yG1vjKEzaWxEuM")
LLM_MODEL = os.environ.get("LLM_MODEL", "gemini-2.5-pro")

# Campos del contratista a verificar y actualizar (nombres exactos de columnas)
CONTRATISTA_FIELDS = ["razon_social", "representante", "ruc", "telefono", "mail", "domicilio"]


def get_postgres_hook() -> PostgresHook:
    """Obtiene el hook de PostgreSQL configurado."""
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def ensure_data_dir() -> None:
    """Asegura que el directorio de datos existe."""
    if not TXT_SOURCE_DIR.exists():
        logger.warning("El directorio %s no existe. Verificar configuración.", TXT_SOURCE_DIR)
        raise FileNotFoundError(f"Directorio no encontrado: {TXT_SOURCE_DIR}")


def dedup(seq: List[Optional[str]]) -> List[str]:
    """Elimina duplicados preservando el orden."""
    seen = set()
    result: List[str] = []
    for item in seq:
        if not item:
            continue
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result


BAD_EMP = re.compile(
    r"(GOBIERNO|MUNICIPAL|GAD|EMPRESA\s+P[ÚU]BLICA|UNIVERSIDAD|MUNICIPIO|ALCALD[IÍ]A|ALCALDE|PREFECTURA|CONSEJO|MINISTERIO|DISTRITAL|SECRETAR[IÍ]A|SENAE|DIRECCI[ÓO]N|DIRECTOR[AA]?|SALUD)",
    re.IGNORECASE,
)
BAD_NAME_CONTEXT = re.compile(
    r"(ALCALDE|CONTRATANTE|MUNICIPAL|GOBIERNO|ADMINISTRADOR|DIRECTOR[AA]?)",
    re.IGNORECASE,
)
REGEX_PERSON_NAME = re.compile(
    r"(?:[A-Z][A-ZÁÉÍÓÚÑ]+\s*\.?\s*)?\b([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+(?:(?:\s+DE|\s+DEL|\s+LA|\s+Y)?\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+){1,4})\b",
    re.UNICODE,
)


def looks_like_employer(texto: Optional[str]) -> bool:
    return bool(texto and BAD_EMP.search(texto))


def es_correo_estado(correo: Optional[str]) -> bool:
    """
    Verifica si un correo pertenece a una institución del Estado.
    
    Args:
        correo: Dirección de correo electrónico
        
    Returns:
        True si es correo del Estado, False en caso contrario
    """
    if not correo:
        return False
    
    correo_lower = str(correo).lower().strip()
    
    # Dominios comunes del Estado ecuatoriano
    dominios_estado = [
        r"@.*\.gob\.ec$",
        r"@.*\.edu\.ec$",
        r"@quito\.gob\.ec$",
        r"@guayaquil\.gob\.ec$",
        r"@ambato\.gob\.ec$",
        r"@cne\.gob\.ec$",
        r"@educacion\.gob\.ec$",
        r"@salud\.gob\.ec$",
        r"@municipio\.",
        r"@gad\.",
        r"@prefectura\.",
        r"@ministerio\.",
    ]
    
    for patron in dominios_estado:
        if re.search(patron, correo_lower):
            return True
    
    return False


def es_direccion_institucional(direccion: Optional[str]) -> bool:
    """
    Verifica si una dirección pertenece a una institución pública.
    
    Args:
        direccion: Texto de dirección
        
    Returns:
        True si parece ser dirección institucional, False en caso contrario
    """
    if not direccion:
        return False
    
    direccion_lower = str(direccion).lower()
    
    # Palabras clave que indican dirección institucional
    indicadores_institucionales = [
        r"\bmunicipio\b",
        r"\bgad\b",
        r"\bprefectura\b",
        r"\bministerio\b",
        r"\bcoordinaci[óo]n\s+zonal\b",
        r"\bempresa\s+p[úu]blica\b",
        r"\buniversidad\s+p[úu]blica\b",
        r"\bhospital\s+p[úu]blico\b",
        r"\bescuela\s+fiscal\b",
        r"\bcolegio\s+fiscal\b",
    ]
    
    for patron in indicadores_institucionales:
        if re.search(patron, direccion_lower):
            return True
    
    return False


def only_digits(value: Optional[str]) -> str:
    return re.sub(r"\D+", "", value or "")


def normalize_phone(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    
    # Si hay múltiples teléfonos separados por "/" o ",", procesar cada uno
    phones = re.split(r'\s*[/,]\s*', s)
    valid_phones = []
    
    for phone in phones:
        phone = phone.strip()
        if not phone:
            continue
        
        # Limpiar extensiones
        phone = re.sub(r"(?:ext\.?|x)\s*\d+$", "", phone, flags=re.IGNORECASE).strip()
        # Normalizar prefijo +593
        phone = re.sub(r"^\+593\s*", "0", phone)
        # Extraer solo dígitos
        digits = re.sub(r"\D+", "", phone)
        
        # Validar formato (celular preferido, luego fijo)
        if re.fullmatch(r"09\d{8}", digits):
            valid_phones.append(("celular", digits))
        elif re.fullmatch(r"0[2-7]\d{7}", digits):
            valid_phones.append(("fijo", digits))
    
    # Preferir celular si hay uno disponible
    for tipo, digits in valid_phones:
        if tipo == "celular":
            return digits
    
    # Si no hay celular, usar el primer fijo válido
    if valid_phones:
        return valid_phones[0][1]
    
    # Si no se encontró ningún formato válido, intentar con el string original
    s = re.sub(r"(?:ext\.?|x)\s*\d+$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"^\+593\s*", "0", s)
    digits = re.sub(r"\D+", "", s)
    if re.fullmatch(r"09\d{8}", digits):
        return digits
    if re.fullmatch(r"0[2-7]\d{7}", digits):
        return digits
    return None


def collect_phones_from_text(texto: str) -> List[str]:
    if not texto:
        return []
    matches = re.findall(
        r"(?:Tel[ée]fonos?|Tel[ée]fono|Cel(?:ular)?|Contacto)?\s*[:\-]?\s*((?:\+593|0)[\d\s().-]{7,})",
        texto,
        flags=re.IGNORECASE,
    )
    loose = re.findall(
        r"(?:^|\b)(\+593[\d\s().-]{7,}|09[\d\s().-]{8,}|0[2-7][\d\s().-]{7,})(?:\b|$)",
        texto,
    )
    candidatos = matches + loose
    return dedup([normalize_phone(item) for item in candidatos if normalize_phone(item)])


def extract_after(texto: str, label_pattern: re.Pattern[str]) -> Optional[str]:
    regex = re.compile(
        label_pattern.pattern
        + r"\s*[:\-]\s*(.{6,250}?)(?=(?:;|\.|,|\s-\s)?\s*(?:Correo\s*electr[óo]nico|E[-\s]?mail|Mail|Tel[ée]fonos?|Celular|Contacto|RUC|C\.?I\.?|C[ée]dula|Representante|Direcci[óo]n|Domicilio|Ubicaci[óo]n|LA\s+ENTIDAD\s+CONTRATANTE|EL\s+CONTRATISTA|POR\s+LA\s+PARTE\s+CONTRATISTA|CL[ÁA]USULA|$))",
        re.IGNORECASE,
    )
    match = regex.search(texto)
    return match.group(1).strip() if match else None


def es_nombre_persona(nombre: Optional[str]) -> Optional[str]:
    """Valida si un texto es un nombre de persona natural."""
    if not nombre:
        return None
    nombre = nombre.strip()
    if len(nombre) < 3 or len(nombre) > 120:
        return None
    if looks_like_employer(nombre):
        return None
    if BAD_NAME_CONTEXT.search(nombre):
        return None
    if not REGEX_PERSON_NAME.search(nombre):
        return None
    nombre = re.sub(r"^(Ing\.?|Arq\.?|Abg\.?|Dr\.?|Dra\.?|Sr\.?|Sra\.?|Lic\.?|Msc\.?|Mg\.?|MBA\.?|Eco\.?)\s*", "", nombre, flags=re.IGNORECASE)
    nombre = re.sub(r"\s+", " ", nombre).strip()
    if len(nombre.split()) < 2:
        return None
    return nombre


def pre_filtro(texto: str) -> str:
    """
    Pre-filtro avanzado que recupera TODOS los bloques donde pueda aparecer el contratista.
    
    Combina regex inteligentes + slicing contextual para capturar 500-1500 caracteres
    alrededor de cada match. Si no encuentra nada, usa los primeros 8000 caracteres.
    
    Args:
        texto: Texto completo del contrato
    
    Returns:
        Texto filtrado con bloques relevantes del contratista
    """
    if not texto:
        return ""
    
    # Normalizar texto
    texto_norm = texto.replace("\r", "").replace("\t", " ")
    texto_norm = re.sub(r"[ \t]+", " ", texto_norm)
    texto_norm = re.sub(r"\n{3,}", "\n\n", texto_norm)
    
    bloques_encontrados: List[tuple[int, int, str]] = []  # (inicio, fin, texto)
    texto_lower = texto_norm.lower()
    
    # Patrones clave para identificar bloques del contratista
    patrones_clave = [
        # Indicadores directos del contratista
        (r"\bcomparecen\b", 500, 1500),
        (r"\bpor\s+otra\s+parte\b", 500, 1500),
        (r"\bcontratista\b", 500, 1500),
        (r"\badjudicatario\b", 500, 1500),
        (r"\bproveedor\b", 500, 1500),
        (r"\bla\s+empresa\b", 500, 1500),
        (r"\bla\s+compa[ñn][íi]a\b", 500, 1500),
        (r"\bla\s+sociedad\b", 500, 1500),
        (r"\bel\s+proveedor\b", 500, 1500),
        (r"\ba\s+quien\s+en\s+adelante\s+se\s+denominar[áa]\s+contratista\b", 500, 1500),
        (r"\bcomparece\b.*\brepresentado\s+por\b", 500, 1500),
        (r"\bel\s+oferente\s+ganador\b", 500, 1500),
        (r"\bla\s+persona\s+natural\b", 500, 1500),
        (r"\bsocio\s+adjudicado\b", 500, 1500),
        (r"\bconsorcio\b", 500, 1500),
        (r"\bcooperativa\b", 500, 1500),
        (r"\bel\s+contratista\s*[:]", 500, 1500),
        (r"\bsr\.\s+[a-záéíóúñ]+", 300, 1000),  # Nombres de personas naturales
        (r"\bsra\.\s+[a-záéíóúñ]+", 300, 1000),
        
        # Patrones RUC (10-13 dígitos) - más variaciones
        (r"\bruc\s*[:#]?\s*\d{10,13}\b", 300, 1200),
        (r"\br\.u\.c\.\s*[:#]?\s*\d{10,13}\b", 300, 1200),
        (r"\bruc\s*n[°ºo]?\s*[:#]?\s*\d{10,13}\b", 300, 1200),
        (r"\bcon\s+ruc\s*[:#]?\s*\d{10,13}\b", 300, 1200),
        
        # Patrones de direcciones - más variaciones
        (r"\b(?:con\s+)?domicilio\s+en\b", 300, 1200),
        (r"\b(?:con\s+)?domicilio\s+fiscal\b", 300, 1200),
        (r"\bubicado\s+en\b", 300, 1200),
        (r"\bdirecci[óo]n\s*[:]\s*", 300, 1200),
        (r"\bdirecci[óo]n\s+fiscal\s*[:]\s*", 300, 1200),
        (r"\bse\s+encuentra\s+en\b", 300, 1200),
        (r"\blocalizado\s+en\b", 300, 1200),
        (r"\breside\s+en\b", 300, 1200),
        (r"\bsede\s+en\b", 300, 1200),
        (r"\boficina\s+en\b", 300, 1200),
        (r"\bav\.\s+[a-z0-9]", 200, 800),
        (r"\bavenida\s+[a-z0-9]", 200, 800),
        (r"\bcalle\s+[a-z0-9]", 200, 800),
        (r"\bsector\s+[a-z0-9]", 200, 800),
        (r"\bparroquia\s+[a-z0-9]", 200, 800),
        (r"\bbarrio\s+[a-z0-9]", 200, 800),
        (r"\bconjunto\s+[a-z0-9]", 200, 800),
        (r"\bentre\s+calles\b", 200, 800),
        (r"\bkm\s+\d+", 200, 800),
        (r"\bv[íi]a\s+[a-z0-9]", 200, 800),
        
        # Patrones de contacto - más variaciones
        (r"\b(?:correo|mail|e[-]?mail|e\s+mail)\s*(?:electr[óo]nico)?\s*[:]\s*[a-z0-9._%+-]+@", 200, 800),
        (r"\b(?:tel[ée]fono|tel\.?|telf\.?)\s*[:]\s*", 200, 800),
        (r"\b(?:tel[ée]fonos?|tels?\.?)\s*[:]\s*", 200, 800),
        (r"\b(?:celular|cel\.?|m[óo]vil|m[óo]bil)\s*[:]\s*", 200, 800),
        (r"\bcontacto\s*[:]\s*", 200, 800),
        (r"\bcomunicaci[óo]n\s*[:]\s*", 200, 800),
        
        # Secciones que suelen contener datos del contratista
        (r"\bcl[áa]usula\s+(?:d[ée]cima\s+)?(?:octava|novena|d[ée]cima)\s*[\.\-]\s*domicilio\b", 400, 1500),
        (r"\bcl[áa]usula\s+(?:d[ée]cima\s+)?(?:octava|novena|d[ée]cima)\s*[\.\-]\s*comunicaciones\b", 400, 1500),
        (r"\bcl[áa]usula\s+(?:d[ée]cima\s+)?(?:octava|novena|d[ée]cima)\s*[\.\-]\s*contacto\b", 400, 1500),
        (r"\bdatos\s+del\s+contratista\b", 400, 1500),
        (r"\binformaci[óo]n\s+del\s+contratista\b", 400, 1500),
    ]
    
    # Buscar todos los matches y capturar bloques contextuales
    for patron, contexto_antes, contexto_despues in patrones_clave:
        for match in re.finditer(patron, texto_lower, re.IGNORECASE):
            inicio = max(0, match.start() - contexto_antes)
            fin = min(len(texto_norm), match.end() + contexto_despues)
            bloque = texto_norm[inicio:fin]
            
            # Evitar duplicados (verificar solapamiento)
            solapado = False
            for inicio_exist, fin_exist, _ in bloques_encontrados:
                if not (fin < inicio_exist or inicio > fin_exist):
                    solapado = True
                    # Extender bloque existente si es necesario
                    if inicio < inicio_exist or fin > fin_exist:
                        nuevo_inicio = min(inicio, inicio_exist)
                        nuevo_fin = max(fin, fin_exist)
                        bloques_encontrados.remove((inicio_exist, fin_exist, texto_norm[inicio_exist:fin_exist]))
                        bloques_encontrados.append((nuevo_inicio, nuevo_fin, texto_norm[nuevo_inicio:nuevo_fin]))
                    break
            
            if not solapado:
                bloques_encontrados.append((inicio, fin, bloque))
    
    # Si no se encontró nada, usar los primeros 8000 caracteres
    if not bloques_encontrados:
        logger.warning("No se encontraron bloques relevantes, usando primeros 8000 caracteres")
        return texto_norm[:8000]
    
    # Combinar bloques únicos (ordenados por posición)
    bloques_encontrados.sort(key=lambda x: x[0])
    
    # Fusionar bloques cercanos (menos de 500 caracteres de separación)
    bloques_fusionados: List[tuple[int, int, str]] = []
    for inicio, fin, bloque in bloques_encontrados:
        if bloques_fusionados:
            ultimo_inicio, ultimo_fin, ultimo_bloque = bloques_fusionados[-1]
            if inicio - ultimo_fin < 500:
                # Fusionar bloques
                nuevo_fin = max(fin, ultimo_fin)
                nuevo_bloque = texto_norm[ultimo_inicio:nuevo_fin]
                bloques_fusionados[-1] = (ultimo_inicio, nuevo_fin, nuevo_bloque)
            else:
                bloques_fusionados.append((inicio, fin, bloque))
        else:
            bloques_fusionados.append((inicio, fin, bloque))
    
    # Combinar todos los bloques
    texto_filtrado = "\n\n---\n\n".join(bloque for _, _, bloque in bloques_fusionados)
    
    # Limitar tamaño total (máximo 12000 caracteres para el LLM)
    if len(texto_filtrado) > 12000:
        texto_filtrado = texto_filtrado[:12000]
        logger.info("Texto filtrado truncado a 12000 caracteres")
    
    logger.info(
        "Pre-filtro: %d bloques encontrados, %d caracteres totales",
        len(bloques_fusionados),
        len(texto_filtrado),
    )
    
    return texto_filtrado


def aislar_bloque_contratista(texto: str) -> str:
    """
    Función legacy - ahora usa pre_filtro() internamente.
    Mantenida para compatibilidad con código existente.
    """
    return pre_filtro(texto)


def extract_contratista_reglas_estaticas(texto: str) -> Dict[str, Optional[str]]:
    """
    Extractor de contratistas basado en reglas estáticas (sin IA).
    
    Extrae:
    1. Bloque de COMPARECIENTES (desde COMPARECEN/COMPARECIENTES hasta CONTRATISTA)
    2. Nombre humano antes de "representante legal", "apoderado", "delegado", "en calidad de"
    3. Empresa (si aparece)
    4. RUC asociado al contratista
    
    Returns:
        Dict con: representante, razon_social, ruc
    """
    if not texto:
        return {"representante": None, "razon_social": None, "ruc": None}
    
    resultado = {"representante": None, "razon_social": None, "ruc": None}
    
    # 1. Extraer bloque de COMPARECIENTES
    # Empieza desde: COMPARECEN, COMPARECIENTES, Comparecen, Por otra parte
    # Termina cuando encuentre: CONTRATISTA o CONTRATISTAS
    inicio_patrones = [
        r"\bCOMPARECEN\b",
        r"\bCOMPARECIENTES\b",
        r"\bComparecen\b",
        r"\bPor otra parte\b",
    ]
    
    bloque_comparecientes = None
    for patron_inicio in inicio_patrones:
        match_inicio = re.search(patron_inicio, texto, re.IGNORECASE)
        if match_inicio:
            inicio_pos = match_inicio.start()
            # Buscar fin: CONTRATISTA o CONTRATISTAS
            fin_match = re.search(r"\bCONTRATISTA(?:S)?\b", texto[inicio_pos:], re.IGNORECASE)
            if fin_match:
                fin_pos = inicio_pos + fin_match.start()
                bloque_comparecientes = texto[inicio_pos:fin_pos]
                break
            else:
                # Si no encuentra CONTRATISTA, tomar hasta 2000 caracteres
                bloque_comparecientes = texto[inicio_pos:inicio_pos + 2000]
                break
    
    if not bloque_comparecientes:
        logger.debug("No se encontró bloque de COMPARECIENTES")
        return resultado
    
    logger.debug("Bloque COMPARECIENTES extraído: %d caracteres", len(bloque_comparecientes))
    
    # 2. Identificar NOMBRE HUMANO antes de: "representante legal", "apoderado", "delegado", "en calidad de"
    # Regex exacto proporcionado por el usuario:
    regex_nombre_humano = r"\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,4})(?=.*,?\s*(representante legal|apoderado|delegado|en calidad))"
    
    match_nombre = re.search(regex_nombre_humano, bloque_comparecientes, re.IGNORECASE | re.UNICODE)
    if match_nombre:
        nombre_candidato = match_nombre.group(1).strip()
        
        # Validaciones: nunca retornar direcciones, teléfonos, frases largas
        excluir_inicios = [
            r"^Av\.", r"^Calle", r"^Sector", r"^Provincia", r"^Cantón", r"^Código",
            r"^S/N", r"^Km", r"^Intersección", r"^Teléfono", r"^Mail",
        ]
        
        es_valido = True
        for excluir in excluir_inicios:
            if re.match(excluir, nombre_candidato, re.IGNORECASE):
                es_valido = False
                break
        
        # Nunca retornar empresas (todo en mayúsculas o que termine en S.A., CIA. LTDA., etc.)
        if es_valido:
            excluir_empresas = [
                r"\.S\.A\.$", r"CIA\.?\s*LTDA\.?$", r"CIA\s+LTDA$", r"LTDA$", r"CÍA\.$", r"S\.A\.S\.$",
            ]
            for excluir in excluir_empresas:
                if re.search(excluir, nombre_candidato, re.IGNORECASE):
                    es_valido = False
                    break
            
            # Verificar que no sea todo mayúsculas (empresa)
            if nombre_candidato.isupper() and len(nombre_candidato.split()) > 2:
                es_valido = False
        
        if es_valido:
            # Limpiar títulos y validar
            nombre_limpio = limpiar_nombre_persona(nombre_candidato)
            if nombre_limpio and validar_representante(nombre_limpio):
                resultado["representante"] = nombre_limpio
                logger.debug("Nombre humano encontrado y validado: %s", nombre_limpio)
            else:
                logger.debug("Nombre humano encontrado pero no pasó validación: %s", nombre_candidato)
                resultado["representante"] = None
    
    # 3. Extraer empresa (solo si se quiere en razon_social)
    # Regex exacto proporcionado por el usuario:
    regex_empresa = r"([A-Z0-9][A-Z0-9\.\-&\s]+?(?:S\.A\.|CIA\.? LTDA\.?|S\.A\.S\.))"
    
    match_empresa = re.search(regex_empresa, bloque_comparecientes, re.IGNORECASE)
    if match_empresa:
        empresa = match_empresa.group(1).strip()
        # Validar que no empiece con direcciones ni frases
        excluir_inicios_empresa = [
            r"^Av\.", r"^Calle", r"^Sector", r"^Provincia", r"^Cantón", r"^Código",
            r"^S/N", r"^Km", r"^Intersección", r"^Teléfono", r"^Mail",
            r"^Comparecen", r"^Partes", r"^quienes", r"^será", r"^Avenida",
        ]
        
        es_valida_empresa = True
        for excluir in excluir_inicios_empresa:
            if re.match(excluir, empresa, re.IGNORECASE):
                es_valida_empresa = False
                break
        
        if es_valida_empresa:
            # Limpiar prefijos comunes antes de asignar (sin validación estricta)
            empresa_limpia = limpiar_razon_social(empresa)
            if empresa_limpia and len(empresa_limpia) >= 3:
                resultado["razon_social"] = empresa_limpia
                logger.debug("Empresa encontrada y limpiada: %s", empresa_limpia)
            else:
                logger.debug("Empresa encontrada pero muy corta después de limpiar: %s", empresa)
    
    # 4. Extraer RUC asociado al contratista
    # Regex exacto proporcionado por el usuario:
    regex_ruc = r"(?:RUC|ruc|R\.U\.C\.?)\s*[:#]?\s*(\d{13})"
    
    match_ruc = re.search(regex_ruc, bloque_comparecientes, re.IGNORECASE)
    if match_ruc:
        ruc = match_ruc.group(1).strip()
        if len(ruc) == 13:
            resultado["ruc"] = ruc
            logger.debug("RUC encontrado: %s", ruc)
    
    # Si hay dos contratistas, devolver solo el primer representante humano
    # (ya se toma el primero con re.search)
    
    return resultado


def generar_candidatos_contratista(
    contenido: str,
    txt_path: Optional[str],
    codigo_proceso: Optional[str],
) -> Dict[str, Any]:
    """Replica EXACTAMENTE la estrategia de n8n V11 Híbrida para generar candidatos."""
    # Normaliza
    full_text = contenido.replace("\r", "").replace("\t", " ")
    full_text = re.sub(r"[ \t]+", " ", full_text)
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    
    # =====================================================
    # AISLAR BLOQUE DEL CONTRATISTA (CRÍTICO)
    # =====================================================
    contractor_block = aislar_bloque_contratista(full_text)
    
    # Si no se pudo aislar, usar fallback mínimo
    if not contractor_block or len(contractor_block.strip()) < 50:
        logger.warning("No se pudo aislar bloque CONTRATISTA, usando fallback mínimo")
        # Fallback: buscar cualquier mención a CONTRATISTA y tomar 800 chars
        match = re.search(r"(?i)(CONTRATISTA|PROVEEDOR|ADJUDICATARI[OA])", full_text)
        if match:
            start = match.start()
            contractor_block = full_text[start:start + 800]
        else:
            contractor_block = full_text[:1000]  # Último recurso: primeros 1000 chars
    
    # Usar contractor_block para TODAS las extracciones
    lines = contractor_block.split("\n")

    # 1) Sección CONTRATISTA - buscar nombre inicial
    start = -1
    contractor_name_from_line = None
    for i, line in enumerate(lines):
        ln = line.strip()
        m = re.search(r"\b(CONTRATISTA|PROVEEDOR|ADJUDICATARI[OA])\s*[:\-]\s*(.*)$", ln, re.IGNORECASE)
        if m:
            start = i
            rest = m.group(2).strip()
            if not rest or len(rest) < 3 or re.match(r"^(ING\.?|ARQ\.?|ABG\.?|DR\.?|DRA\.?|SR\.?|SRA\.?)?$", rest, re.IGNORECASE):
                next_line = (lines[i + 1] if i + 1 < len(lines) else "").strip()
                if next_line and len(next_line) < 4 and i + 2 < len(lines):
                    next_line = (next_line + " " + (lines[i + 2] or "")).strip()
                rest = next_line
            if re.match(r"^(Provincia|Cant[oó]n|Calles?|Callej[oó]n|Correo|E[-\s]?mail|Mail|Tel[ée]fono|Tel[ée]fonos?)\b", rest, re.IGNORECASE):
                pass  # Evita confundir domicilio con razón social
            elif rest:
                rest = re.sub(r'["""]+', "", rest)
                rest = re.sub(r"\s+", " ", rest).strip()
                if not looks_like_employer(rest) or REGEX_PERSON_NAME.search(rest):
                    contractor_name_from_line = rest
            break

    cand_razon = contractor_name_from_line
    cand_ruc: List[str] = []
    cand_mails: List[str] = []
    cand_tels: List[str] = []
    cand_dir: Optional[str] = None
    cand_dom: Optional[str] = None

    # Dirección, Mail y Teléfono del CONTRATISTA (usar SOLO contractor_block)
    all_joined = re.sub(r"\s+", " ", contractor_block.replace("\n", " "))

    # 1) Dirección/Domicilio dentro del bloque "EL CONTRATISTA"
    dir_in_block = re.search(
        r"EL\s+CONTRATISTA\s*:\s*[^:]*?(?:Direcci[óo]n|Domicilio)\s*[:\-]\s*(.{6,250}?)(?=(?:;|\.|,|\s-\s)?\s*(?:Correo\s*electr[óo]nico|E[-\s]?mail|Mail|Tel[ée]fonos?|Celular|Contacto|RUC|C\.?I\.?|C[ée]dula|Representante|LA\s+ENTIDAD\s+CONTRATANTE|EL\s+CONTRATISTA|POR\s+LA\s+PARTE\s+CONTRATISTA|CL[ÁA]USULA|$))",
        all_joined,
        re.IGNORECASE,
    )
    if dir_in_block and dir_in_block.group(1):
        cand_dir = dir_in_block.group(1).strip()
        cand_dom = cand_dir

    # Fallback global de Dirección/Domicilio
    if not cand_dir or not cand_dom:
        m_dom_global = re.search(
            r"\bDomicilio(?:\s+del\s+contratista|\s+del\s+proveedor)?\s*[:\-]\s*(.{6,250}?)(?=(?:;|\.|,|\s-\s)?\s*(?:Correo|E[-\s]?mail|Mail|Tel[ée]fonos?|Celular|Contacto|RUC|LA\s+ENTIDAD\s+CONTRATANTE|EL\s+CONTRATISTA|POR\s+LA\s+PARTE\s+CONTRATISTA|CL[ÁA]USULA|$))",
            all_joined,
            re.IGNORECASE,
        )
        if m_dom_global and m_dom_global.group(1):
            cand_dom = m_dom_global.group(1).strip()
            if not cand_dir:
                cand_dir = cand_dom
        if not cand_dir:
            m_dir_global = re.search(
                r"\bDirecci[óo]n(?:\s+del\s+contratista|\s+del\s+proveedor)?\s*[:\-]\s*(.{6,250}?)(?=(?:;|\.|,|\s-\s)?\s*(?:Correo\s*electr[óo]nico|E[-\s]?mail|Mail|Tel[ée]fonos?|Celular|Contacto|RUC|LA\s+ENTIDAD\s+CONTRATANTE|EL\s+CONTRATISTA|POR\s+LA\s+PARTE\s+CONTRATISTA|CL[ÁA]USULA|$))",
                all_joined,
                re.IGNORECASE,
            )
            if m_dir_global and m_dir_global.group(1):
                cand_dir = m_dir_global.group(1).strip()
        if not cand_dom and cand_dir:
            cand_dom = cand_dir

    # 2) Correo electrónico: primero "EL CONTRATISTA", luego última coincidencia global
    m_mail_contratista = re.search(
        r"EL\s+CONTRATISTA\s*:\s*[^:]*?(?:Correo\s*electr[óo]nico|E[-\s]?mail|Mail|Correo)\s*[:\-]\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
        all_joined,
        re.IGNORECASE,
    )
    if m_mail_contratista and m_mail_contratista.group(1):
        mail = m_mail_contratista.group(1).lower().strip()
        if not re.search(r"@example\.(com|org|net)$", mail, re.IGNORECASE) and not re.match(r"^correo@", mail, re.IGNORECASE):
            cand_mails = [mail]

    if not cand_mails:
        all_mails = list(re.finditer(r"(?:Correo\s*electr[óo]nico|E[-\s]?mail|Mail|Correo)\s*[:\-]\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", all_joined, re.IGNORECASE))
        if all_mails:
            last_mail = all_mails[-1].group(1).lower().strip()
            if not re.search(r"@example\.(com|org|net)$", last_mail, re.IGNORECASE) and not re.match(r"^correo@", last_mail, re.IGNORECASE):
                cand_mails = [last_mail]

    # 3) Teléfono: primero "EL CONTRATISTA", luego última coincidencia global
    m_tel_contratista = re.search(
        r"EL\s+CONTRATISTA\s*:\s*[^:]*?(?:Tel[ée]fonos?|Celular|Contacto)\s*[:\-]\s*(\+593[\s\d\-()]{7,}|0[2-7]\d{7}|09\d{8})",
        all_joined,
        re.IGNORECASE,
    )
    if m_tel_contratista and m_tel_contratista.group(1):
        tel = normalize_phone(m_tel_contratista.group(1).strip())
        if tel:
            cand_tels = [tel]

    if not cand_tels:
        all_tels = list(re.finditer(r"(?:Tel[ée]fonos?|Celular|Contacto)\s*[:\-]\s*(\+593[\s\d\-()]{7,}|0[2-7]\d{7}|09\d{8})", all_joined, re.IGNORECASE))
        if all_tels:
            last_tel_raw = all_tels[-1].group(1).strip()
            tel = normalize_phone(last_tel_raw)
            if tel:
                cand_tels = [tel]

    if not cand_tels:
        phones = collect_phones_from_text(contractor_block)
        if phones:
            cand_tels = phones

    # === Fallback strategies (SOLO si falta info) + Enriquecimiento de contactos ===
    def needs_replace(value: Optional[str]) -> bool:
        if not value:
            return True
        if re.search(r"Para\s+todos\s+los\s+efectos", value, re.IGNORECASE):
            return True
        return len(value) > 180

    def extract_after(text: str, label_re: re.Pattern[str]) -> Optional[str]:
        pattern_str = label_re.pattern + r"\s*[:\-]\s*(.{6,250}?)(?=(?:;|\.|,|\s-\s)?\s*(?:Correo\s*electr[óo]nico|E[-\s]?mail|Mail|Tel[ée]fonos?|Celular|Contacto|RUC|C\.?I\.?|C[ée]dula|Representante|Direcci[óo]n|Domicilio|Ubicaci[óo]n|LA\s+ENTIDAD\s+CONTRATANTE|EL\s+CONTRATISTA|POR\s+LA\s+PARTE\s+CONTRATISTA|CL[ÁA]USULA|$))"
        regex = re.compile(pattern_str, re.IGNORECASE)
        m = regex.search(text)
        return m.group(1).strip() if m and m.group(1) else None

    # contractorContactSlice (usar SOLO contractor_block)
    idx_contractor = contractor_block.find("EL CONTRATISTA:")
    if idx_contractor < 0:
        idx_contractor = contractor_block.find("EL CONTRATISTA")
    contractor_contact_slice = None
    if idx_contractor >= 0:
        contractor_contact_slice = contractor_block[idx_contractor : min(len(contractor_block), idx_contractor + 600)]

    if contractor_contact_slice:
        addr_match = re.search(
            r"EL\s+CONTRATISTA\s*:\s*([\s\S]{0,320}?)(?=(?:Tel[ée]fonos?|Tel[ée]fono|Celular|Contacto|Correo|E[-\s]?mail|Mail|$))",
            contractor_contact_slice,
            re.IGNORECASE,
        )
        if addr_match and addr_match.group(1):
            addr_block = re.sub(r"\s+", " ", addr_match.group(1)).strip()
            addr_block = re.sub(r"[.;,\s]+$", "", addr_block)
            if addr_block and len(addr_block) >= 6:
                if needs_replace(cand_dir) or not cand_dir:
                    cand_dir = addr_block
                if needs_replace(cand_dom) or not cand_dom:
                    cand_dom = addr_block

        if not cand_dir or needs_replace(cand_dir):
            direct = extract_after(contractor_contact_slice, re.compile(r"(Direcci[óo]n|Domicilio|Ubicaci[óo]n)", re.IGNORECASE))
            if direct:
                if needs_replace(cand_dir) or not cand_dir:
                    cand_dir = direct
                if needs_replace(cand_dom) or not cand_dom:
                    cand_dom = direct

        if not cand_tels:
            phones = collect_phones_from_text(contractor_contact_slice)
            if phones:
                cand_tels = dedup((cand_tels or []) + phones)

        if not cand_mails:
            mails = [m.group(0).lower() for m in re.finditer(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", contractor_contact_slice, re.IGNORECASE)]
            if mails:
                cand_mails = dedup((cand_mails or []) + mails)

    # stratDatosContratista
    def strat_datos_contratista() -> None:
        nonlocal cand_dir, cand_dom, cand_mails, cand_tels
        title_re = re.compile(r"(DATOS\s+DEL\s+(CONTRATISTA|PROVEEDOR|OFERENTE))", re.IGNORECASE)
        idx = next((i for i, l in enumerate(lines) if title_re.search(l or "")), -1)
        if idx < 0:
            return
        stop_re = re.compile(r"^(CONTRATANTE|OBJETO|PLAZO|MONTO|ANTECEDENTES|CONSIDERANDO|CLA[ÚU]SULA|FIRMAS)\b", re.IGNORECASE)
        end = idx + 1
        while end < len(lines) and (end - idx) <= 40 and not stop_re.search((lines[end] or "").upper().strip()):
            end += 1
        sec_text = " ".join(lines[idx:end])
        sec_text = re.sub(r"\s+", " ", sec_text)
        if needs_replace(cand_dir) or not cand_dir or needs_replace(cand_dom) or not cand_dom:
            v_dom = extract_after(sec_text, re.compile(r"(Domicilio|Direcci[óo]n|Ubicaci[óo]n)", re.IGNORECASE))
            if v_dom:
                if needs_replace(cand_dir) or not cand_dir:
                    cand_dir = v_dom
                if needs_replace(cand_dom) or not cand_dom:
                    cand_dom = v_dom
        if not cand_mails:
            m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", sec_text, re.IGNORECASE)
            if m and m.group(1):
                cand_mails = [m.group(1).lower()]
        if not cand_tels:
            t = re.search(r"(\+593[\s\d\-()]{7,}|0[2-7]\d{7}|09\d{8})", sec_text, re.IGNORECASE)
            if t and t.group(1):
                tel = normalize_phone(t.group(1))
                if tel:
                    cand_tels = [tel]

    # stratProveedorBloque (usar SOLO contractor_block)
    def strat_proveedor_bloque() -> None:
        nonlocal cand_dir, cand_dom, cand_mails, cand_tels
        m_block = re.search(r"\bPROVEEDOR\s*:\s*[^:]{0,500}", contractor_block, re.IGNORECASE)
        if not m_block:
            return
        block = m_block.group(0)
        if needs_replace(cand_dir) or not cand_dir or needs_replace(cand_dom) or not cand_dom:
            v = extract_after(block, re.compile(r"(Direcci[óo]n|Domicilio|Ubicaci[óo]n)", re.IGNORECASE))
            if v:
                if needs_replace(cand_dir) or not cand_dir:
                    cand_dir = v
                if needs_replace(cand_dom) or not cand_dom:
                    cand_dom = v
        if not cand_mails:
            m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", block, re.IGNORECASE)
            if m and m.group(1):
                cand_mails = [m.group(1).lower()]
        if not cand_tels:
            t = re.search(r"(\+593[\s\d\-()]{7,}|0[2-7]\d{7}|09\d{8})", block, re.IGNORECASE)
            if t and t.group(1):
                tel = normalize_phone(t.group(1))
                if tel:
                    cand_tels = [tel]

    # stratClausulaDomicilioElContratista
    def strat_clausula_domicilio_el_contratista() -> None:
        nonlocal cand_dir, cand_dom, cand_mails, cand_tels
        idx = next((i for i, l in enumerate(lines) if re.search(r"EL\s+CONTRATISTA\s*:", l or "", re.IGNORECASE)), -1)
        if idx < 0:
            return
        stop_re = re.compile(r"(LA\s+ENTIDAD\s+CONTRATANTE\s*:)|(^\s*CL[ÁA]SULA\b)", re.IGNORECASE)
        segment = []
        for i in range(idx, min(len(lines), idx + 80)):
            li = str(lines[i] or "")
            if i > idx and stop_re.search(li.upper().strip()):
                break
            segment.append(li)
        trimmed = [l.strip() for l in segment if l.strip()]
        if not trimmed:
            return

        def clean_tail(s: Optional[str]) -> str:
            return (s or "").strip().rstrip(".;,")

        def parse_colon(line: str) -> str:
            m = re.search(r"^[^:]+:\s*(.+)$", line)
            return clean_tail(m.group(1) if m else line)

        provincia = None
        canton = None
        street_parts = []
        started = False

        for line in trimmed:
            if re.match(r"^(Tel[ée]fono|Correo|E[-\s]?mail)", line, re.IGNORECASE):
                break
            if re.match(r"^Provincia\s*:", line, re.IGNORECASE):
                provincia = parse_colon(line)
                started = True
                continue
            if re.match(r"^Cant[oó]n\s*:", line, re.IGNORECASE):
                canton = parse_colon(line)
                started = True
                continue
            if re.match(r"^(Calles?|Callej[oó]n)\s*:", line, re.IGNORECASE):
                street_parts.append(parse_colon(line))
                started = True
                continue
            if ":" not in line and started:
                street_parts.append(clean_tail(line))

        parts = []
        if provincia:
            parts.append("Provincia " + provincia)
        if canton:
            parts.append("Cantón " + canton)
        streets_str = ", ".join([s for s in street_parts if s])
        if streets_str:
            if re.match(r"^(call(es|e)|callej[oó]n)", streets_str, re.IGNORECASE):
                parts.append(streets_str)
            else:
                parts.append("Calles " + streets_str)

        addr = ", ".join(parts)
        addr = re.sub(r"\s{2,}", " ", addr).strip()
        should_replace_dir = addr and (not cand_dir or needs_replace(cand_dir))
        should_replace_dom = addr and (not cand_dom or needs_replace(cand_dom))
        if should_replace_dir:
            cand_dir = addr
        if should_replace_dom:
            cand_dom = addr

        joined_sec = " ".join(trimmed)
        joined_sec = re.sub(r"\s+", " ", joined_sec)
        emails = [m.group(0).lower() for m in re.finditer(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", joined_sec, re.IGNORECASE)]
        if emails:
            cand_mails = dedup((cand_mails or []) + emails)
        tels = [normalize_phone(m.group(1)) for m in re.finditer(r"(\+593[\s\d\-()]{7,}|0[2-7]\d{7}|09\d{8})", joined_sec, re.IGNORECASE)]
        tels = [t for t in tels if t]
        if tels:
            cand_tels = dedup((cand_tels or []) + tels)

    # stratNombrePorLaParteContratista
    def strat_nombre_por_la_parte_contratista() -> None:
        nonlocal cand_razon
        if cand_razon:
            return
        i = next((idx for idx, l in enumerate(lines) if re.search(r"POR\s+LA\s+PARTE\s+CONTRATISTA", l or "", re.IGNORECASE)), -1)
        if i >= 0:
            for k in range(i + 1, min(len(lines), i + 6)):
                raw_line = lines[k] or ""
                if BAD_NAME_CONTEXT.search(raw_line):
                    continue
                s = re.sub(r'["""]+', "", raw_line).strip()
                if not s:
                    continue
                m = REGEX_PERSON_NAME.search(s)
                if m and m.group(1) and not looks_like_employer(m.group(1)):
                    cand_razon = re.sub(r"\s+", " ", m.group(1)).strip()
                    return
                if re.match(r"^[A-ZÁÉÍÓÚÑ\s.'-]{3,}$", s) and len(s) <= 120 and not BAD_NAME_CONTEXT.search(s):
                    cand_razon = re.sub(r"\s+", " ", s).strip()
                    return

        if not cand_razon:
            tail = " ".join(lines[-150:])
            m = re.search(r"Firmado\s+electr[óo]nicamente\s+por\s*:\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+(?:\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+){1,4})", tail, re.IGNORECASE | re.UNICODE)
            if m and m.group(1) and not looks_like_employer(m.group(1)) and not BAD_NAME_CONTEXT.search(m.group(0) or ""):
                cand_razon = re.sub(r"\s+", " ", m.group(1)).strip()

        if not cand_razon:
            tail_lines = lines[-200:]
            for idx, line in enumerate(tail_lines):
                if not re.search(r"CONTRATISTA", line or "", re.IGNORECASE):
                    continue
                prev = re.sub(r'["""]+', "", (tail_lines[idx - 1] if idx > 0 else "") or "").strip()
                if not prev or BAD_NAME_CONTEXT.search(prev):
                    continue
                m = REGEX_PERSON_NAME.search(prev)
                if m and m.group(1) and not looks_like_employer(m.group(1)):
                    cand_razon = re.sub(r"\s+", " ", m.group(1)).strip()
                    break

    # Ejecutar estrategias
    strategies = [strat_clausula_domicilio_el_contratista, strat_nombre_por_la_parte_contratista, strat_datos_contratista, strat_proveedor_bloque]
    for s in strategies:
        if cand_dir and cand_dom and cand_mails and cand_tels and cand_razon:
            break
        s()

    # Fuerza razón social desde "... con RUC ..." (usar SOLO contractor_block)
    ruc_pattern = re.compile(r"([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+(?:\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+){1,5})\s*,?\s*(?:con\s+)?R\.?U\.?C\.?\s*[:#]?\s*(\d{13})", re.IGNORECASE | re.UNICODE)
    matches = list(ruc_pattern.finditer(contractor_block))
    if matches:
        choice = matches[-1]
        if choice and choice.group(1):
            cleaned = re.sub(r"\b(?:ING|ING\.|INGENIERO|ARQ|ARQ\.|ARQUITECTO|DR|DR\.|DRA|DRA\.|SR|SR\.|SRA|SRA\.)\b", "", choice.group(1), flags=re.IGNORECASE)
            cleaned = re.sub(r"\bFirmado(?:\s+electrónicamente)?\b", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            if cleaned:
                context = choice.group(0) or ""
                if not looks_like_employer(cleaned) and not BAD_NAME_CONTEXT.search(context):
                    cand_razon = cleaned

    # Excepción para nombres capturados desde firmas de la contratante
    needs_override = False
    if not cand_razon:
        needs_override = True
    elif re.search(r"firmad[oa]", cand_razon or "", re.IGNORECASE):
        needs_override = True
    else:
        try:
            escaped = re.escape(cand_razon or "")
            found = re.search(escaped, contractor_block, re.IGNORECASE)
            if found:
                idx = found.start()
                seg = contractor_block[max(0, idx - 120) : min(len(contractor_block), idx + len(cand_razon or "") + 120)]
                if re.search(r"CONTRATANTE|DIRECTOR[AA]?", seg, re.IGNORECASE) and not re.search(r"CONTRATISTA", seg, re.IGNORECASE):
                    needs_override = True
        except Exception:
            pass

    if needs_override:
        re_pattern = re.compile(
            r"(?:EL\s+S[EÉ][NÑ]OR|S[EÉ][NÑ]ORA|SR\.?|SRA\.?|EL\s+CONTRATISTA|CONTRATISTA|ADJUDICATARI[OA]|OFERENTE)\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+(?:\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+){1,4})\s*,?\s*(?:CON\s+)?R\.?U\.?C\.?\s*[:#]?\s*(\d{13})",
            re.IGNORECASE | re.UNICODE,
        )
        matches = list(re_pattern.finditer(contractor_block))
        if matches:
            for i in range(len(matches) - 1, -1, -1):
                name_raw = re.sub(r"\b(ING(?:ENIERO)?|ARQ(?:UITECT[OA])?|DR|DRA|SR|SRA|LIC|MSC|MBA)\b\.?", "", matches[i].group(1), flags=re.IGNORECASE)
                name_raw = re.sub(r"\s+", " ", name_raw).strip()
                if not name_raw or looks_like_employer(name_raw):
                    continue
                around = contractor_block[max(0, matches[i].start() - 80) : min(len(contractor_block), matches[i].end() + 80)]
                if re.search(r"CONTRATANTE", around, re.IGNORECASE) and not re.search(r"CONTRATISTA", around, re.IGNORECASE):
                    continue
                cand_razon = name_raw
                ruc_digits = only_digits(matches[i].group(2))
                if ruc_digits:
                    cand_ruc = [ruc_digits]
                break

    # Excepción extra: si aún queda una entidad, usa al representante legal
    needs_rep_name = False
    if not cand_razon:
        needs_rep_name = True
    elif looks_like_employer(cand_razon):
        needs_rep_name = True
    elif re.search(r"\bAUT[ÓO]NOMO\b|\bDESCENTRALIZADO\b", cand_razon or "", re.IGNORECASE):
        needs_rep_name = True

    if needs_rep_name:
        rep_pattern = re.compile(
            r"representad[oa]\s+(?:legalmente\s+)?por\s+(?:el|la|los|las)?\s*(?:ING(?:\.|ENIERO)?|ARQ(?:\.|UITECT[OA])?|ABG\.?|DR\.?|DRA\.?|SR\.?|SRA\.?|LIC\.?|MBA\.?|MSC\.?)?\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+(?:\s+[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-zñáéíóú]+){1,4})",
            re.IGNORECASE | re.UNICODE,
        )
        matches = list(rep_pattern.finditer(contractor_block))
        if matches:
            for i in range(len(matches) - 1, -1, -1):
                raw_name = re.sub(r"\b(ING(?:ENIERO)?|ARQ(?:UITECT[OA])?|ABG|DR|DRA|SR|SRA|LIC|MBA|MSC)\b\.?", "", matches[i].group(1), flags=re.IGNORECASE)
                raw_name = re.sub(r"\s+", " ", raw_name).strip()
                if not raw_name or looks_like_employer(raw_name):
                    continue
                around = contractor_block[max(0, matches[i].start() - 120) : min(len(contractor_block), matches[i].end() + 120)]
                if re.search(r"CONTRATANTE", around, re.IGNORECASE) and not re.search(r"CONTRATISTA", around, re.IGNORECASE):
                    continue
                cand_razon = raw_name
                ruc_nearby = re.search(r"\b\d{13}\b", contractor_block[matches[i].start() : min(len(contractor_block), matches[i].start() + 220)])
                if ruc_nearby:
                    cand_ruc = [only_digits(ruc_nearby.group(0))]
                break

    # Limpieza
    def clean_str(s: Optional[str]) -> Optional[str]:
        return re.sub(r"\s+", " ", s).strip() if s else None

    cand_dir = clean_str(cand_dir)
    cand_dom = clean_str(cand_dom)
    cand_mails = dedup([s.lower() for s in cand_mails if s])
    cand_tels = dedup([normalize_phone(t) for t in cand_tels if normalize_phone(t)])

    # Código de proceso
    codigo_proceso_final = codigo_proceso
    if not codigo_proceso_final and txt_path:
        file = str(txt_path).split("/")[-1].split("\\")[-1].upper().replace(".TXT", "").replace("_", "-")
        m = re.search(r"([A-Z]{2,}(?:-[A-Z0-9]+)+-\d{2,4}(?:-[A-Z0-9]{1,4})?)", file)
        if m:
            codigo_proceso_final = m.group(1)

    # RUC — prioriza el del contratista y devuelve solo el mejor (usar SOLO contractor_block)
    contractor_ctx = re.compile(r"(CONTRATISTA|CONSORCIO|PROVEEDOR|ADJUDICATARI[OA]|OFERENTE|REPRESENTAD[AO])", re.IGNORECASE)
    contractant_ctx = re.compile(r"(CONTRATANTE|ENTIDAD\s+CONTRATANTE|GOBIERNO|MUNICIPIO|GAD|PREFECT[OA]|MINISTERIO|EMPRESA\s+P[ÚU]BLICA)", re.IGNORECASE)
    matches = list(re.finditer(r"\b\d{13}\b", contractor_block))
    if matches:
        scored = []
        for m in matches:
            idx = m.start()
            ruc = m.group(0)
            before = contractor_block[max(0, idx - 150) : idx]
            after = contractor_block[idx + len(ruc) : min(len(contractor_block), idx + len(ruc) + 150)]
            ctx = before + " " + after
            score = 0
            if contractor_ctx.search(ctx):
                score += 3
            if re.search(r"\bCONTRATISTA\b", ctx, re.IGNORECASE):
                score += 2
            if re.search(r"\bCONSORCIO\b", ctx, re.IGNORECASE):
                score += 1
            if contractant_ctx.search(ctx):
                score -= 3
            if re.search(r"\bCONTRATANTE\b", ctx, re.IGNORECASE):
                score -= 2
            scored.append({"ruc": ruc, "score": score, "idx": idx})
        scored.sort(key=lambda x: (-x["score"], x["idx"]))
        best = next((s for s in scored if s["score"] >= 0), scored[0] if scored else None)
        if best:
            cand_ruc = [only_digits(best["ruc"])]
        elif not cand_ruc:
            fallback = re.findall(r"\b\d{13}\b", contractor_block)
            if fallback:
                cand_ruc = [only_digits(fallback[0])]

    # LIMPIAR CANDIDATOS ANTES DE RETORNAR (sin validación estricta)
    # Separar empresa (razon_social) de persona (representante) solo si es claro
    cand_representante = None
    
    # Si cand_razon existe, limpiarlo pero no rechazarlo estrictamente
    if cand_razon:
        # Limpiar prefijos comunes
        cand_razon_limpio = limpiar_razon_social(cand_razon)
        if cand_razon_limpio and len(cand_razon_limpio) >= 3:
            # Si parece persona (2-3 palabras, solo letras, formato nombre), podría ser representante
            palabras = cand_razon_limpio.split()
            es_posible_persona = (2 <= len(palabras) <= 3 and 
                                 all(p[0].isupper() for p in palabras if p) and
                                 not re.search(r"\d|LTDA|S\.A\.|CIA|CONSORCIO", cand_razon_limpio, re.IGNORECASE))
            
            if es_posible_persona:
                # Podría ser representante, guardar en ambos campos y dejar que la validación final decida
                cand_representante = limpiar_nombre_persona(cand_razon_limpio)
            cand_razon = cand_razon_limpio
        else:
            cand_razon = None
    
    # Resultado final sin validación estricta (se validará después)
    candidatos = {
        "razon_social": cand_razon,
        "representante": cand_representante,
        "ruc": cand_ruc or [],
        "telefono": cand_tels or [],
        "mail": cand_mails or [],
        "direccion": cand_dir or None,
        "domicilio": cand_dom or None,
    }

    # contractor_hint_block ahora es el mismo contractor_block (ya aislado)
    contractor_hint_block = re.sub(r"\s+", " ", contractor_block).strip()

    return {
        "txt_path": txt_path or None,
        "src_year": None,  # No se usa en Airflow
        "full_text": full_text,  # Mantener para referencia, pero NO usar en extractores
        "codigo_proceso": codigo_proceso_final,
        "candidatos": candidatos,
        "contractor_hint_block": contractor_hint_block,  # Bloque aislado del CONTRATISTA
    }


def extract_contratista_segment(texto: str) -> str:
    """Extrae el segmento más relevante del texto donde se detalla al contratista."""
    if not texto:
        return ""

    lineas = texto.splitlines()
    if not lineas:
        return texto

    indices_contratista = [
        i for i, linea in enumerate(lineas) if re.search(r"\bCONTRATISTA\b", linea, re.IGNORECASE)
    ]
    if not indices_contratista:
        indices_contratista = [
            i for i, linea in enumerate(lineas) if re.search(r"representad[oa]", linea, re.IGNORECASE)
        ]

    if indices_contratista:
        idx = indices_contratista[0]
        inicio = max(idx - 10, 0)
        fin = min(idx + 150, len(lineas))
        segmento_base = lineas[inicio:fin]
    else:
        segmento_base = lineas[:150]

    patrones_contacto = r"(tel|telf|tel\.|mail|correo|domicilio|dirección|direccion)"
    segmento_contacto: List[str] = []
    for idx in [i for i, linea in enumerate(lineas) if re.search(patrones_contacto, linea, re.IGNORECASE)]:
        ventana = lineas[max(idx - 3, 0) : min(idx + 4, len(lineas))]
        segmento_contacto.extend(ventana)

    combinado: List[str] = []
    vistos = set()
    for linea in segmento_base + segmento_contacto:
        if linea not in vistos:
            combinado.append(linea)
            vistos.add(linea)

    texto_segmento = "\n".join(combinado)
    if len(texto_segmento) > 12000:
        texto_segmento = texto_segmento[:9000] + "\n...\n" + texto_segmento[-3000:]
    return texto_segmento


def find_representante_desde_texto(texto: str) -> Optional[str]:
    """Intenta identificar el nombre del representante legal o procurador del contratista."""
    if not texto:
        return None

    patrones = [
        r"procurador(?:a)?\s+com[úu]n[^,\n:;]*[:,-]?\s*(?P<nombre>[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑñüÜ .]+)",
        r"representante\s+legal[^,\n:;]*[:,-]?\s*(?P<nombre>[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑñüÜ .]+)",
        r"representad[oa]\s+(?:legalmente\s+)?por[^,\n:;]*[:,-]?\s*(?P<nombre>[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑñüÜ .]+)",
        r"firmado\s+electrónicamente\s+por[:\s-]*(?P<nombre>[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑñüÜ .]+)",
        r"en\s+calidad\s+de\s+\w[^,\n:;]*[:,-]?\s*(?P<nombre>[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑñüÜ .]+)",
    ]

    for patron in patrones:
        for match in re.finditer(patron, texto, re.IGNORECASE):
            nombre = match.group("nombre").strip()
            nombre = re.sub(
                r"^(Ing\.?|Dr\.?|Dra\.?|Sr\.?|Sra\.?|Lcdo\.?|Lic\.?|Arq\.?|Abg\.?|Msc\.?|Mg\.?|PhD\.?|Eco\.?|C\.?)\s*",
                "",
                nombre,
                flags=re.IGNORECASE,
            )
            nombre = re.sub(r"[,;:.]+$", "", nombre).strip()
            nombre = re.sub(r"\ben\s+calidad.*$", "", nombre, flags=re.IGNORECASE).strip()
            nombre = re.sub(r"\bquien\b.*$", "", nombre, flags=re.IGNORECASE).strip()
            if 3 <= len(nombre) <= 120:
                return " ".join(nombre.split())
    return None


def limpiar_razon_social(razon: Optional[str]) -> Optional[str]:
    """
    Limpia razon_social removiendo prefijos comunes incorrectos.
    
    Remueve prefijos como:
    - "a ", "la ", "el ", "los ", "las "
    - "por otra parte", "y por la otra parte"
    - "la empresa", "el oferente", "la oferente"
    - "en calidad de Representante Legal de"
    - Saltos de línea y espacios múltiples
    """
    if not razon:
        return None
    
    razon = str(razon).strip()
    
    # Remover saltos de línea y espacios múltiples
    razon = razon.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    razon = re.sub(r"\s+", " ", razon).strip()
    
    # Remover prefijos comunes al inicio
    prefijos_remover = [
        r"^a\s+",
        r"^la\s+",
        r"^el\s+",
        r"^los\s+",
        r"^las\s+",
        r"^y\s+por\s+la\s+otra\s+parte\s+",
        r"^por\s+otra\s+parte\s+",
        r"^y\s+por\s+otra\s+parte\s+",
        r"^la\s+empresa\s+",
        r"^el\s+oferente\s+",
        r"^la\s+oferente\s+",
        r"^en\s+calidad\s+de\s+Representante\s+Legal\s+de\s+",
        r"^representante\s+legal\s+de\s+",
        r"^representado\s+por\s+",
    ]
    
    for prefijo in prefijos_remover:
        razon = re.sub(prefijo, "", razon, flags=re.IGNORECASE)
        razon = razon.strip()
    
    return razon if len(razon) >= 3 else None


def validar_razon_social(razon: Optional[str]) -> bool:
    """
    Valida que razon_social sea una empresa válida.
    
    Debe contener al menos UNA de estas palabras clave:
    - CIA, CÍA, CI A, CIA LTDA, LTDA, L.T.D.A.
    - S.A., S. A.
    - EP, E.P.
    - CONSORCIO, COOPERATIVA, COMPAÑÍA
    - EMPRESA PÚBLICA, ASOCIACIÓN
    - CONSTRUCTORA, SERVICIOS, COMERCIAL, INDUSTRIAL
    - ESTACIÓN DE SERVICIO, GASOLINERA, DISTRIBUIDORA
    
    NO debe empezar con:
    - números, "1.1", "Comparecen", "Partes", direcciones
    """
    if not razon:
        return False
    
    razon = razon.strip()
    if len(razon) < 3 or len(razon) > 200:
        return False
    
    # NO debe empezar con números o frases prohibidas
    if re.match(r"^\d", razon) or re.match(r"^1\.1", razon):
        return False
    
    prohibidos_inicio = [
        r"^Comparecen",
        r"^Partes",
        r"^quienes",
        r"^será",
        r"^Avenida",
        r"^Av\.",
        r"^Calle",
        r"^Sector",
        r"^Provincia",
        r"^Cantón",
        r"^Código",
    ]
    
    for prohibido in prohibidos_inicio:
        if re.match(prohibido, razon, re.IGNORECASE):
            return False
    
    # DEBE contener al menos UNA palabra clave de empresa O tener formato típico de empresa
    keywords_empresa = [
        r"CIA",
        r"CÍA",
        r"CIA\s+LTDA",
        r"LTDA",
        r"L\.T\.D\.A\.",
        r"S\.A\.",
        r"S\.\s+A\.",
        r"\bEP\b",
        r"E\.P\.",
        r"CONSORCIO",
        r"COOPERATIVA",
        r"COMPAÑ[ÍI]A",
        r"EMPRESA\s+P[UÚ]BLICA",
        r"ASOCIACI[ÓO]N",
        r"CONSTRUCTORA",
        r"SERVICIOS",
        r"COMERCIAL",
        r"INDUSTRIAL",
        r"ESTACI[ÓO]N\s+DE\s+SERVICIO",
        r"GASOLINERA",
        r"DISTRIBUIDORA",
    ]
    
    tiene_keyword = False
    for keyword in keywords_empresa:
        if re.search(keyword, razon, re.IGNORECASE):
            tiene_keyword = True
            break
    
    # Si no tiene palabra clave, pero tiene formato típico de empresa (más de 2 palabras, mayúsculas iniciales)
    # y NO parece ser una persona (no tiene formato de nombre personal), aceptar
    if not tiene_keyword:
        palabras = razon.split()
        # Si tiene más de 3 palabras o contiene palabras típicas de empresa, aceptar
        if len(palabras) > 3:
            # Podría ser empresa con nombre largo
            tiene_keyword = True
        elif len(palabras) == 2 or len(palabras) == 3:
            # Verificar que NO parezca un nombre personal (2-3 palabras con mayúscula inicial es sospechoso)
            # Si todas las palabras empiezan con mayúscula y no tienen números, podría ser persona
            todas_mayuscula_inicial = all(palabra and palabra[0].isupper() for palabra in palabras if palabra)
            solo_letras = all(re.match(r"^[A-ZÁÉÍÓÚÑa-záéíóúñ]+$", p) for p in palabras if p)
            if todas_mayuscula_inicial and solo_letras:
                # Parece nombre personal, rechazar
                return False
            # Si tiene números o caracteres especiales, probablemente es empresa
            tiene_keyword = True
    
    if not tiene_keyword:
        return False
    
    # NO debe contener comas al inicio o punto y coma en medio (indica frase legal)
    if razon.startswith(",") or ";" in razon:
        return False
    
    # NO debe ser demasiado larga (> 8 palabras indica que es una frase)
    palabras = razon.split()
    if len(palabras) > 8:
        return False
    
    return True


def validar_representante(representante: Optional[str]) -> bool:
    """
    Valida que representante sea un nombre humano válido.
    
    Debe:
    - Tener 2 a 4 palabras
    - Cada palabra empieza con mayúscula
    - Solo letras y tildes (no números)
    - NO contener palabras clave de empresa (LTDA, S.A., CONSORCIO, etc.)
    - NO contener cargos (Coordinador, Director, etc.)
    """
    if not representante:
        return False
    
    representante = representante.strip()
    
    # Remover títulos
    representante = re.sub(
        r"^(Ing\.?|Arq\.?|Abg\.?|Dr\.?|Dra\.?|Sr\.?|Sra\.?|Lic\.?|Msc\.?|Mg\.?|MBA\.?|Eco\.?)\s+",
        "",
        representante,
        flags=re.IGNORECASE,
    ).strip()
    
    # Validar longitud
    if len(representante) < 6 or len(representante) > 120:
        return False
    
    # NO debe contener números
    if re.search(r"\d", representante):
        return False
    
    # NO debe contener palabras clave de empresa
    keywords_empresa = [
        r"LTDA",
        r"S\.A\.",
        r"CONSORCIO",
        r"CIA",
        r"EP",
        r"CIA\s+LTDA",
        r"COMPAÑ[ÍI]A",
        r"gasolinera",
        r"avenida",
        r"calle",
    ]
    
    for keyword in keywords_empresa:
        if re.search(keyword, representante, re.IGNORECASE):
            return False
    
    # NO debe contener cargos comunes
    cargos_prohibidos = [
        r"Coordinador",
        r"Director",
        r"Administrador",
        r"Gerente",
        r"Jefe",
        r"Secretario",
        r"Ministro",
        r"Alcalde",
        r"Prefecto",
        r"Gobernador",
    ]
    
    for cargo in cargos_prohibidos:
        if re.search(cargo, representante, re.IGNORECASE):
            return False
    
    # Validar formato: 2-4 palabras, cada una con mayúscula inicial
    palabras = representante.split()
    if len(palabras) < 2 or len(palabras) > 4:
        return False
    
    # Cada palabra debe empezar con mayúscula y contener solo letras/tildes
    for palabra in palabras:
        if not palabra or not palabra[0].isupper():
            return False
        if not re.match(r"^[A-ZÁÉÍÓÚÑ][a-záéíóúñ\s-]*$", palabra):
            return False
    
    return True


def limpiar_nombre_persona(valor: Optional[str]) -> Optional[str]:
    """Normaliza y valida que el valor se parezca a un nombre de persona natural."""
    if not valor:
        return None

    nombre = valor.strip()
    # eliminar prefijos comunes y texto residual
    nombre = re.sub(
        r"^(Ing\.?|Dr\.?|Dra\.?|Sr\.?|Sra\.?|Lcdo\.?|Lic\.?|Arq\.?|Abg\.?|Msc\.?|Mg\.?|PhD\.?|Eco\.?|C\.?|Ing\.|Lcd[aou]\.?)\s*",
        "",
        nombre,
        flags=re.IGNORECASE,
    )
    nombre = re.sub(r"[,;:.]+$", "", nombre).strip()
    nombre = re.sub(r"\b(en\s+su\s+calidad\s+de|quien\s+act[úu]a|legalmente\s+representad[oa]\s+por)\b.*$", "", nombre, flags=re.IGNORECASE).strip()

    nombre = re.sub(r"\d+", "", nombre)

    if not re.search(r"[A-Za-zÁÉÍÓÚÑñ]+\s+[A-Za-zÁÉÍÓÚÑñ]+", nombre):
        return None

    tokens_invalidos = re.compile(
        r"\b(direcci[oó]n|coordinaci[oó]n|prefecto|prefectura|gerencia|departamento|contrataci[oó]n|ruc|domicilio|correo|mail|tel[eé]fono|secci[oó]n|unidad|calle|ciudad|provincia|ecuador)\b",
        re.IGNORECASE,
    )
    if tokens_invalidos.search(nombre):
        return None

    if len(nombre) > 120:
        return None

    partes_validas = []
    for part in nombre.split():
        limpio = part.strip(".")
        if len(limpio) < 2:
            continue
        if not re.search(r"[A-Za-zÁÉÍÓÚÑñ]", limpio):
            continue
        partes_validas.append(limpio.capitalize())

    if len(partes_validas) < 2:
        return None

    return " ".join(partes_validas)


def hash_text(texto: str) -> str:
    """Calcula el hash SHA-256 del contenido."""
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def dividir_en_chunks(texto: str, tamaño: int = 8000) -> List[str]:
    """
    Divide el texto completo en chunks de tamaño especificado.
    
    Args:
        texto: Texto completo a dividir
        tamaño: Tamaño de cada chunk en caracteres (default: 8000)
        
    Returns:
        Lista de chunks de texto
    """
    chunks = []
    for i in range(0, len(texto), tamaño):
        chunks.append(texto[i:i+tamaño])
    return chunks


def fusionar_resultados(base: Dict[str, Optional[str]], nuevo: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """
    Fusiona dos resultados de extracción, priorizando datos no-null.
    
    Reglas:
    - Si base tiene un valor y nuevo tiene null → mantener base
    - Si base tiene null y nuevo tiene valor → usar nuevo
    - Si ambos tienen valores → mantener base (prioridad al primero encontrado)
    
    Args:
        base: Resultado base (se mantiene si tiene valores)
        nuevo: Resultado nuevo a fusionar
        
    Returns:
        Diccionario fusionado
    """
    resultado = base.copy()
    
    for campo in ["razon_social", "representante", "ruc", "telefono", "mail", "domicilio"]:
        valor_base = resultado.get(campo)
        valor_nuevo = nuevo.get(campo)
        
        # Si base está vacío/null y nuevo tiene valor, usar nuevo
        if (not valor_base or valor_base in ["null", "none", ""]) and valor_nuevo and valor_nuevo not in ["null", "none", ""]:
            resultado[campo] = valor_nuevo
            logger.debug("Campo %s actualizado desde null a: %s", campo, str(valor_nuevo)[:50])
        # Si base ya tiene valor, mantenerlo (no sobrescribir con null ni con otro valor)
        elif valor_base and valor_base not in ["null", "none", ""]:
            # Mantener base, no hacer nada
            pass
    
    return resultado


def llm_chunk(
    chunk: str,
    prompt: str,
    model,
    file_name: str,
    codigo_proceso: str,
    chunk_num: int,
    total_chunks: int
) -> Dict[str, Optional[str]]:
    """
    Procesa un solo chunk del texto con el LLM.
    
    Args:
        chunk: Fragmento de texto a procesar
        prompt: Prompt base para el LLM
        model: Modelo de Gemini configurado
        file_name: Nombre del archivo
        codigo_proceso: Código del proceso
        chunk_num: Número del chunk actual
        total_chunks: Total de chunks
        
    Returns:
        Diccionario con datos extraídos del chunk
    """
    max_retries = 3  # Reducido de 5 a 3 para fallar más rápido si hay problemas
    base_delay = 1  # Reducido de 2 a 1 segundo para reintentos más rápidos
    
    for intento in range(max_retries):
        try:
            # Agregar delay entre llamadas para evitar saturar la API
            if intento > 0:
                delay = base_delay * (2 ** intento)
                logger.info("Reintento %d/%d para chunk %d/%d de %s después de %d segundos", 
                          intento + 1, max_retries, chunk_num, total_chunks, file_name, delay)
                time.sleep(delay)
            
            try:
                # Construir el prompt completo con el chunk
                prompt_completo = f"{prompt}\n\n---\n\nTEXTO DEL CONTRATO (FRAGMENTO {chunk_num}/{total_chunks}):\n\n{chunk}"
                
                # Configurar generación
                generation_config = {
                    "temperature": 0.1,
                    "max_output_tokens": 2048,  # Aumentado para evitar cortes de respuesta
                }
                
                # Safety settings para API moderna
                safety_settings = [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                ]
                
                # Llamar al modelo con API moderna
                response = model.generate_content(
                    prompt_completo,
                    generation_config=generation_config,
                    safety_settings=safety_settings
                )
                
                # Verificar finish_reason antes de acceder a response.text
                respuesta_texto = None
                if response and response.candidates:
                    candidate = response.candidates[0]
                    finish_reason = candidate.finish_reason if hasattr(candidate, 'finish_reason') else None
                    
                    # Log del finish_reason para diagnóstico
                    # finish_reason: 1=STOP, 2=MAX_TOKENS, 3=SAFETY, 4=RECITATION, 5=OTHER
                    if finish_reason and finish_reason != 1:
                        logger.info("Chunk %d/%d finish_reason=%s (1=STOP, 2=MAX_TOKENS, 3=SAFETY). Intentando leer contenido parcial...", 
                                  chunk_num, total_chunks, finish_reason)
                    
                    # Solo descartar si es SAFETY (3) - para MAX_TOKENS (2) intentamos leer el contenido parcial
                    if finish_reason == 3:  # SAFETY solamente
                        logger.warning("Respuesta bloqueada por filtro de seguridad para chunk %d/%d. Saltando chunk.", 
                                     chunk_num, total_chunks)
                        return {
                            "razon_social": None,
                            "representante": None,
                            "ruc": None,
                            "telefono": None,
                            "mail": None,
                            "domicilio": None,
                        }
                    
                    # Intentar obtener el texto de la respuesta - múltiples métodos para compatibilidad
                    try:
                        # Método 1: Intentar response.text directamente
                        if hasattr(response, 'text'):
                            try:
                                respuesta_texto = response.text.strip()
                            except (ValueError, AttributeError):
                                # Método 2: Acceder a través de candidates y parts
                                if response.candidates and len(response.candidates) > 0:
                                    candidate = response.candidates[0]
                                    if hasattr(candidate, 'content') and candidate.content:
                                        if hasattr(candidate.content, 'parts'):
                                            for part in candidate.content.parts:
                                                if hasattr(part, 'text') and part.text:
                                                    respuesta_texto = part.text.strip()
                                                    break
                    except Exception as text_error:
                        # Si hay error al acceder a response.text, intentar método alternativo
                        logger.warning("Error al acceder a response.text para chunk %d/%d: %s. Intentando método alternativo...", 
                                     chunk_num, total_chunks, text_error)
                        # Método alternativo: acceder directamente a parts
                        try:
                            if response.candidates and len(response.candidates) > 0:
                                candidate = response.candidates[0]
                                if hasattr(candidate, 'content') and candidate.content:
                                    if hasattr(candidate.content, 'parts'):
                                        for part in candidate.content.parts:
                                            if hasattr(part, 'text') and part.text:
                                                respuesta_texto = part.text.strip()
                                                break
                        except Exception as alt_error:
                            logger.warning("Error en método alternativo para chunk %d/%d: %s. Finish reason: %s", 
                                         chunk_num, total_chunks, alt_error, finish_reason)
                        # Retornar resultado vacío para este chunk
                        return {
                            "razon_social": None,
                            "representante": None,
                            "ruc": None,
                            "telefono": None,
                            "mail": None,
                            "domicilio": None,
                        }
                
                if not respuesta_texto:
                    logger.warning("LLM no devolvió respuesta válida para chunk %d/%d de %s (intento %d/%d). Finish reason: %s", 
                                 chunk_num, total_chunks, file_name, intento + 1, max_retries, finish_reason if 'finish_reason' in locals() else 'unknown')
                    if intento < max_retries - 1:
                        continue  # Reintentar
                    else:
                        # Si falla después de todos los intentos, retornar resultado vacío
                        logger.error("No se pudo obtener respuesta del LLM para chunk %d/%d después de %d intentos. Response: %s", 
                                   chunk_num, total_chunks, max_retries, str(response)[:200] if 'response' in locals() else 'N/A')
                        return {
                            "razon_social": None,
                            "representante": None,
                            "ruc": None,
                            "telefono": None,
                            "mail": None,
                            "domicilio": None,
                        }
                
                # Log de la respuesta recibida para debugging (INFO para ver qué está pasando)
                logger.info("Respuesta del LLM para chunk %d/%d de %s (primeros 500 chars): %s", 
                           chunk_num, total_chunks, file_name, respuesta_texto[:500])
                
                # Extraer JSON de la respuesta
                json_match = re.search(r'\{[^{}]*"razon_social"[^{}]*\}', respuesta_texto, re.DOTALL)
                if not json_match:
                    json_match = re.search(r'\{.*?"razon_social".*?\}', respuesta_texto, re.DOTALL | re.IGNORECASE)
                
                if json_match:
                    json_str = json_match.group(0)
                else:
                    json_str = respuesta_texto
                
                # Limpiar y parsear JSON
                json_str = re.sub(r'```json\s*', '', json_str, flags=re.IGNORECASE)
                json_str = re.sub(r'```\s*', '', json_str)
                json_str = json_str.strip()
                
                try:
                    datos = json.loads(json_str)
                    logger.info("JSON parseado exitosamente para chunk %d/%d de %s: %s", 
                               chunk_num, total_chunks, file_name, datos)
                except json.JSONDecodeError as e:
                    logger.warning("Error al parsear JSON del chunk %d/%d: %s. JSON string: %s. Respuesta completa: %s", 
                                 chunk_num, total_chunks, e, json_str[:500], respuesta_texto[:500])
                    # Intentar extraer campos manualmente con regex
                    datos = {}
                    for campo in ["razon_social", "representante", "ruc", "telefono", "correo", "mail", "domicilio"]:
                        pattern = rf'"{campo}"\s*:\s*"([^"]*)"'
                        match = re.search(pattern, json_str, re.IGNORECASE)
                        if match:
                            datos[campo] = match.group(1).strip() if match.group(1) else None
                        else:
                            datos[campo] = None
                
                # Retornar datos sin validar (la validación se hace al final)
                return {
                    "razon_social": datos.get("razon_social"),
                    "representante": datos.get("representante"),
                    "ruc": datos.get("ruc"),
                    "telefono": datos.get("telefono"),
                    "mail": datos.get("correo") or datos.get("mail") or datos.get("email"),
                    "domicilio": datos.get("domicilio") or datos.get("direccion"),
                }
                
            except Exception as api_call_exc:
                error_msg = str(api_call_exc)
                logger.warning("Error en llamada a API (intento %d/%d) para chunk %d/%d de %s: %s", 
                             intento + 1, max_retries, chunk_num, total_chunks, file_name, error_msg)
                
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower() or "network" in error_msg.lower():
                    if intento < max_retries - 1:
                        continue
                    else:
                        logger.error("Error de conexión/timeout para chunk %d/%d después de %d intentos", 
                                   chunk_num, total_chunks, max_retries)
                        return {
                            "razon_social": None,
                            "representante": None,
                            "ruc": None,
                            "telefono": None,
                            "mail": None,
                            "domicilio": None,
                        }
                else:
                    raise
                    
        except Exception as quota_exc:
            error_str = str(quota_exc).lower()
            is_rate_limit = "429" in error_str or "rate limit" in error_str or "quota" in error_str
            
            if is_rate_limit and intento < max_retries - 1:
                delay = base_delay * (2 ** intento)
                logger.warning("Error de rate limit/cuota para chunk %d/%d. Reintentando en %d segundos (intento %d/%d)", 
                            chunk_num, total_chunks, delay, intento + 1, max_retries)
                time.sleep(delay)
                continue
            elif is_rate_limit:
                logger.error("Error de rate limit/cuota para chunk %d/%d después de %d intentos", 
                           chunk_num, total_chunks, max_retries)
                return {
                    "razon_social": None,
                    "representante": None,
                    "ruc": None,
                    "telefono": None,
                    "mail": None,
                    "domicilio": None,
                }
            else:
                raise
    
    # Si llegamos aquí, todos los intentos fallaron
    logger.error("No se pudo procesar chunk %d/%d de %s después de %d intentos", 
               chunk_num, total_chunks, file_name, max_retries)
    return {
        "razon_social": None,
        "representante": None,
        "ruc": None,
        "telefono": None,
        "mail": None,
        "domicilio": None,
    }


def llm(texto: str, codigo_proceso: str, file_name: str) -> Dict[str, Any]:
    """
    Función LLM robusta que extrae información del contratista usando procesamiento por chunks.
    
    El texto completo se divide en chunks de ~8000 caracteres y cada uno se procesa
    por separado. Los resultados se fusionan priorizando datos no-null.
    
    Incluye:
    - División del texto en chunks
    - Procesamiento de cada chunk con el LLM
    - Fusión de resultados parciales
    - Validación y normalización de datos (RUC, teléfono)
    - Limpieza de ruido
    
    Args:
        texto: Texto completo del contrato
        codigo_proceso: Código del proceso
        file_name: Nombre del archivo
        
    Returns:
        Diccionario con datos extraídos y normalizados
    """
    if not LLM_API_KEY:
        logger.warning("LLM_API_KEY no definido. Saltando extracción con IA.")
        raise AirflowSkipException("LLM_API_KEY no configurado")

    try:
        # Configurar cliente de Gemini
        genai.configure(api_key=LLM_API_KEY)
        model = genai.GenerativeModel(LLM_MODEL)
        logger.info("Cliente Gemini configurado con model=%s", LLM_MODEL)

        # NOTA: El contratista puede ser empresa privada, consorcio o persona natural.
        # El contratante suele ser GAD, ministerio, empresa pública, municipio, etc. y debe ignorarse.
        
        # Prompt para el LLM - Extracción de datos del contratista
        prompt = """Eres un extractor experto de información contractual. Tu única tarea es encontrar la información EXACTA del CONTRATISTA en un archivo de contrato del Ecuador.

El archivo puede tener miles de líneas y estructuras diferentes. 
Debes leer TODO el texto y buscar con máxima atención las secciones donde aparece 
el CONTRATISTA (proveedor u oferente adjudicado).

NUNCA confundir contratista con:
- entidad contratante
- alcaldes
- procuradores
- administradores de contrato
- jefes de compras
- funcionarios públicos

Tu salida SIEMPRE debe venir del contratista.

-------------------------------------------
BUSCA LA INFORMACIÓN SIGUIENDO ESTAS REGLAS:
-------------------------------------------

1. RAZÓN SOCIAL:
   - Debe ser una empresa, consorcio, compañía, o persona natural oferente.
   - Prioriza frases como:
     "la CONTRATISTA", "el CONTRATISTA", "adjudicado a", 
     "oferente ganador", "proveedor adjudicado", 
     "comparece… (empresa)", "(empresa) con RUC…".
   - No uses la entidad contratante.

2. REPRESENTANTE:
   - Busca nombres justo después de:
     "representado legalmente por", 
     "representante legal", 
     "gerente general", 
     "apoderado", 
     "representado por".
   - Si hay varios, el correcto es el que esté más cerca de la empresa contratista.

3. RUC:
   - Número de 13 dígitos asociado al contratista.
   - Si hay varios, el válido es el que esté más cerca de la razón social.

4. TELÉFONO:
   - Números de 7 a 10 dígitos.
   - Puede incluir espacios o guiones.
   - Si hay varios, reporta el principal del contratista.

5. MAIL:
   - Cualquier dirección de correo que pertenezca al contratista.

6. DOMICILIO:
   - Busca expresiones como:
     "domicilio", "dirección", "ubicado en", "calle", "av.", 
     "cantón", "provincia", "parroquia".
   - Debe ser la dirección del CONTRATISTA, NO de la entidad.

-------------------------------------------
FORMATO DE RESPUESTA (JSON):
-------------------------------------------

{
  "razon_social": "",
  "representante": "",
  "ruc": "",
  "telefono": "",
  "mail": "",
  "domicilio": ""
}

Si un dato no existe en el contrato, deja el campo como null. 
No inventes nada. Usa SOLO lo que está explícito en el texto."""

        # Dividir el texto completo en chunks de ~8000 caracteres
        logger.info("Dividiendo texto completo en chunks para %s (proceso: %s). Tamaño total: %d caracteres", 
                   file_name, codigo_proceso, len(texto))
        # Aumentar tamaño de chunks para reducir número de llamadas a la API
        # De 8000 a 15000 caracteres = menos chunks = menos llamadas = más rápido
        chunks = dividir_en_chunks(texto, tamaño=15000)
        logger.info("Texto dividido en %d chunks para %s", len(chunks), file_name)
        
        # Inicializar resultado final con todos los campos en null
        resultado_final: Dict[str, Optional[str]] = {
            "razon_social": None,
            "representante": None,
            "ruc": None,
            "telefono": None,
            "mail": None,
            "domicilio": None,
        }
        
        # Procesar cada chunk
        # OPTIMIZACIÓN: Si ya tenemos todos los campos con datos, podemos detener el procesamiento
        campos_objetivo = ["razon_social", "representante", "ruc", "telefono", "mail", "domicilio"]
        
        for i, chunk in enumerate(chunks, 1):
            logger.info("Procesando chunk %d/%d para %s (proceso: %s). Tamaño del chunk: %d caracteres", 
                       i, len(chunks), file_name, codigo_proceso, len(chunk))
            
            # Procesar chunk con el LLM
            resultado_parcial = llm_chunk(
                chunk=chunk,
                prompt=prompt,
                model=model,
                file_name=file_name,
                codigo_proceso=codigo_proceso,
                chunk_num=i,
                total_chunks=len(chunks)
            )
            
            # Fusionar resultado parcial con el resultado final
            resultado_final = fusionar_resultados(resultado_final, resultado_parcial)
            
            # Log de progreso
            campos_encontrados = sum(1 for v in resultado_final.values() if v is not None and v not in ["null", "none", ""])
            logger.info("Después del chunk %d/%d: %d campos con datos encontrados", i, len(chunks), campos_encontrados)
            
            # OPTIMIZACIÓN DESACTIVADA TEMPORALMENTE: La detención temprana puede causar que se pierdan datos
            # Si ya tenemos todos los campos críticos, podemos detener el procesamiento
            # Esto reduce significativamente el tiempo cuando los datos están al inicio del documento
            # DESACTIVADO: Puede causar que se pierdan datos si están distribuidos en múltiples chunks
            # campos_criticos = ["razon_social", "ruc"]
            # tiene_criticos = all(resultado_final.get(campo) for campo in campos_criticos)
            # tiene_suficientes = campos_encontrados >= 4  # Si tenemos 4 o más campos, es suficiente
            # 
            # if tiene_criticos and tiene_suficientes and i < len(chunks):
            #     logger.info("Datos suficientes encontrados después del chunk %d/%d. Saltando chunks restantes para optimizar tiempo.", 
            #               i, len(chunks))
            #     break  # Salir del loop si ya tenemos datos suficientes
            
            # Delay mínimo entre chunks solo si hay muchos chunks para evitar rate limiting
            # Eliminado delay para mejorar rendimiento (la API de Gemini puede manejar llamadas rápidas)
            # Solo agregar delay si hay más de 10 chunks para evitar saturar la API
            if i < len(chunks) and len(chunks) > 10:
                time.sleep(0.05)  # 50ms solo si hay muchos chunks
        
        # Ahora validar y normalizar los datos finales
        datos = resultado_final
        
        # Validar y limpiar cada campo
        resultado: Dict[str, Optional[str]] = {
            "razon_social": None,
            "representante": None,
            "ruc": None,
            "telefono": None,
            "mail": None,
            "domicilio": None,
        }
        
        # razon_social
        razon = datos.get("razon_social")
        if razon and razon.lower() not in ["null", "none", ""]:
            razon_limpia = limpiar_razon_social(str(razon))
            if razon_limpia and len(razon_limpia) >= 3:
                # Si parece institución pública / contratante, descartar
                if not looks_like_employer(razon_limpia):
                    resultado["razon_social"] = razon_limpia
                else:
                    resultado["razon_social"] = None
        
        # representante (el prompt usa "representante", pero aceptamos ambos por compatibilidad)
        representante = datos.get("representante") or datos.get("representante_legal")
        if representante and str(representante).lower() not in ["null", "none", ""]:
            rep_limpio = limpiar_nombre_persona(str(representante))
            if rep_limpio and validar_representante(rep_limpio):
                resultado["representante"] = rep_limpio
        
        # ruc - normalizar a 10-13 dígitos y validar que no sea genérico/inventado
        ruc = datos.get("ruc")
        if ruc and str(ruc).lower() not in ["null", "none", ""]:
            ruc_digits = only_digits(str(ruc))
            if 10 <= len(ruc_digits) <= 13:
                # Rechazar RUCs sospechosos que parezcan inventados
                # (todos los dígitos iguales, patrones genéricos)
                if re.match(r'^0{10,13}$', ruc_digits) or re.match(r'^1{10,13}$', ruc_digits) or re.match(r'^9{10,13}$', ruc_digits):
                    logger.warning("RUC sospechoso (genérico) rechazado: %s", ruc_digits)
                    resultado["ruc"] = None
                else:
                    resultado["ruc"] = ruc_digits
        
        # telefono - normalizar y validar que no sea genérico/inventado
        telefono = datos.get("telefono")
        if telefono and str(telefono).lower() not in ["null", "none", ""]:
            tel_normalizado = normalize_phone(str(telefono))
            if tel_normalizado:
                # Rechazar teléfonos sospechosos que parezcan inventados
                # (todos los dígitos iguales, patrones genéricos)
                if re.match(r'^0{10,13}$', tel_normalizado) or re.match(r'^1{10,13}$', tel_normalizado) or re.match(r'^9{10,13}$', tel_normalizado):
                    logger.warning("Teléfono sospechoso (genérico) rechazado: %s", tel_normalizado)
                    resultado["telefono"] = None
                else:
                    resultado["telefono"] = tel_normalizado
        
        # correo/mail - filtrar correos del Estado y genéricos sospechosos
        correo = datos.get("correo") or datos.get("mail") or datos.get("email")
        if correo and str(correo).lower() not in ["null", "none", ""]:
            correo_str = str(correo).strip().lower()
            # Validar formato básico de email
            if re.match(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$', correo_str):
                # NO aceptar correos del Estado
                if not es_correo_estado(correo_str):
                    # Rechazar correos genéricos sospechosos que parezcan inventados
                    correos_sospechosos = [
                        r'^contacto@.*$',
                        r'^info@.*$',
                        r'^admin@.*$',
                        r'^noreply@.*$',
                        r'^no-reply@.*$',
                        r'^test@.*$',
                        r'^example@.*$',
                        r'^ejemplo@.*$',
                    ]
                    es_sospechoso = False
                    for patron in correos_sospechosos:
                        if re.match(patron, correo_str):
                            logger.warning("Correo genérico sospechoso rechazado: %s", correo_str)
                            es_sospechoso = True
                            break
                    
                    if not es_sospechoso:
                        resultado["mail"] = correo_str
                    else:
                        resultado["mail"] = None
                else:
                    logger.debug("Correo del Estado descartado: %s", correo_str)
                    resultado["mail"] = None
            else:
                logger.debug("Formato de correo inválido: %s", correo_str)
                resultado["mail"] = None
        
        # domicilio - mantener completo pero descartar textos legales largos e institucionales
        domicilio = datos.get("domicilio") or datos.get("direccion")
        if domicilio and str(domicilio).lower() not in ["null", "none", ""]:
            dom_limpio = str(domicilio).strip()
            # Limpiar espacios múltiples pero mantener estructura
            dom_limpio = re.sub(r'\s+', ' ', dom_limpio)
            
            # Descartar textos típicos de cláusulas generales
            if re.search(r"para todos los efectos", dom_limpio, re.IGNORECASE):
                dom_limpio = ""
            
            # Descartar si es dirección institucional
            if es_direccion_institucional(dom_limpio):
                logger.debug("Dirección institucional descartada: %s", dom_limpio[:100])
                resultado["domicilio"] = None
            # Si es demasiado largo, probablemente es una cláusula y no solo dirección
            elif len(dom_limpio) > 0 and len(dom_limpio) <= 250:
                resultado["domicilio"] = dom_limpio
            else:
                resultado["domicilio"] = None
        
        # Log de campos detectados con detalles y validación de datos
        logger.info("Datos extraídos para %s (proceso: %s): razon_social=%s, representante=%s, ruc=%s, telefono=%s, mail=%s, domicilio=%s",
                   file_name, codigo_proceso,
                   "✓" if resultado["razon_social"] else "null",
                   "✓" if resultado["representante"] else "null",
                   "✓" if resultado["ruc"] else "null",
                   "✓" if resultado["telefono"] else "null",
                   "✓" if resultado["mail"] else "null",
                   "✓" if resultado["domicilio"] else "null")
        
        # Verificar que no se hayan inventado datos (todos null es válido si no hay información)
        campos_con_datos = sum(1 for v in resultado.values() if v is not None)
        logger.info("Total de campos con datos extraídos: %d/6 para %s", campos_con_datos, file_name)
        
        # Log de campos detectados con detalles
        campos_detectados = [k for k, v in resultado.items() if v is not None]
        logger.info(
            "LLM extrajo datos para %s (proceso: %s). Campos detectados: %s",
            file_name,
            codigo_proceso,
            campos_detectados,
        )

        # Log detallado de cada campo detectado
        for campo, valor in resultado.items():
            if valor:
                logger.info("  ✓ %s: %s", campo, str(valor)[:100] if len(str(valor)) > 100 else str(valor))

        return resultado

    except AirflowSkipException:
        # Re-lanzar skip exceptions (como cuando no hay API key)
        raise
    except Exception as exc:
        logger.exception("Error en llamada LLM para %s: %s", file_name, exc)
        # Lanzar excepción normal para que Airflow pueda reintentar la tarea más tarde
        raise Exception(f"Error en LLM: {exc}") from exc


def call_llm_contratista(
    texto: str,
    codigo_proceso: str,
    file_name: str,
    pistas: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Función legacy - ahora usa llm() internamente.
    Mantenida para compatibilidad con código existente.
    """
    return llm(texto, codigo_proceso, file_name)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="extract_contratista_etl",
    description="ETL para extracción de información de contratistas desde archivos TXT usando IA (100% LLM)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=32,  # Permitir múltiples ejecuciones en paralelo
    max_active_tasks=32,  # Aumentar paralelismo dentro del DAG
    default_args=default_args,
    tags=["etl", "contratos", "contratista", "llm"],
)
def extract_contratista_etl():
    """
    DAG principal para extracción de información de contratistas usando IA.
    
    Flujo:
    1. list_txt_files: Lista todos los archivos .txt en la ruta (recursivo)
    2. filter_processed: Filtra archivos ya procesados y verifica que existan en BD
    3. read_txt_batch: Lee archivos
    4. llm_extract_contratista: Extrae datos del contratista usando IA (DeepSeek)
    5. update_postgres: Actualiza solo registros existentes en PostgreSQL
    6. log_results: Registra resultados finales
    """

    @task
    def list_txt_files() -> List[str]:
        """
        Lista todos los archivos .txt en el directorio fuente y subdirectorios.
        Busca recursivamente en todas las carpetas (ej: 2022, 2023, 2024, 2025).
        
        Returns:
            Lista de rutas completas de archivos .txt
        """
        ensure_data_dir()
        
        # Buscar recursivamente en todos los subdirectorios
        archivos = sorted(TXT_SOURCE_DIR.rglob("*.txt"))
        rutas = [str(archivo) for archivo in archivos]
        
        logger.info("Encontrados %d archivos TXT en %s (búsqueda recursiva)", len(rutas), TXT_SOURCE_DIR)
        if rutas:
            logger.debug("Primeros archivos encontrados: %s", rutas[:5])
        return rutas

    @task
    def filter_processed(file_paths: List[str]) -> List[Dict[str, Any]]:
        """
        Filtra archivos que:
        1. Ya tienen información de contratista en la BD (para omitirlos)
        2. No existen en la BD (para omitirlos, solo actualizamos existentes)
        
        OPTIMIZADO: Usa una sola consulta SQL con IN para verificar todos los archivos a la vez.
        
        Args:
            file_paths: Lista de rutas de archivos
            
        Returns:
            Lista de diccionarios con información de archivos pendientes que existen en BD
        """
        if not file_paths:
            logger.info("No hay archivos para procesar")
            return []
        
        hook = get_postgres_hook()
        
        # PRIMERO: Extraer todos los códigos de proceso de una vez
        codigos_proceso_map = {}  # {codigo_proceso: {file_path, file_name, codigo_proceso}}
        for file_path in file_paths:
            file_name = Path(file_path).name
            codigo_proceso = extract_codigo_proceso_from_filename(file_name)
            if codigo_proceso:
                codigos_proceso_map[codigo_proceso] = {
                    "file_path": file_path,
                    "file_name": file_name,
                    "codigo_proceso": codigo_proceso,
                }
        
        if not codigos_proceso_map:
            logger.warning("No se pudieron extraer códigos de proceso de los archivos")
            return []
        
        codigos_list = list(codigos_proceso_map.keys())
        logger.info(
            "Verificando %d códigos de proceso en BD (consulta batch única)",
            len(codigos_list),
        )
        
        archivos_pendientes = []
        archivos_omitidos = []
        archivos_no_existen = []
        
        try:
            conn = hook.get_conn()
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                # OPTIMIZACIÓN: Una sola consulta para obtener TODOS los registros
                columnas = CONTRATISTA_FIELDS
                placeholders = ','.join(['%s'] * len(codigos_list))
                
                query_completa = f"""
                    SELECT codigo_proceso, {', '.join(columnas)}
                    FROM {CONTRATOS_TABLE}
                    WHERE codigo_proceso IN ({placeholders})
                """
                
                cursor.execute(query_completa, codigos_list)
                resultados_bd = cursor.fetchall()
                
                # Crear diccionario de datos actuales: {codigo_proceso: {campo: valor}}
                datos_por_codigo = {}
                codigos_que_existen = set()
                for row in resultados_bd:
                    codigo = row[0]
                    codigos_que_existen.add(codigo)
                    datos_por_codigo[codigo] = dict(zip(columnas, row[1:]))
                
                logger.info(
                    "Consulta batch: %d códigos existen en BD, %d no existen",
                    len(codigos_que_existen),
                    len(codigos_list) - len(codigos_que_existen),
                )
                
                # Filtrar archivos
                def esta_vacio(valor: Any) -> bool:
                    if valor is None:
                        return True
                    if isinstance(valor, str) and not valor.strip():
                        return True
                    return False
                
                for codigo, file_info in codigos_proceso_map.items():
                    if codigo not in codigos_que_existen:
                        archivos_no_existen.append(file_info["file_name"])
                        logger.debug(
                            "Archivo %s omitido: proceso %s no existe en la BD (solo actualizamos existentes)",
                            file_info["file_name"],
                            codigo,
                        )
                        continue
                    
                    datos_actuales = datos_por_codigo.get(codigo, {})
                    
                    # PRIORIDAD: Primero procesar archivos SIN NINGÚN dato (razon_social y ruc vacíos)
                    # Esto es más eficiente que procesar archivos que ya tienen datos parciales
                    campos_criticos = ["razon_social", "ruc"]
                    sin_datos_criticos = all(
                        esta_vacio(datos_actuales.get(col)) for col in campos_criticos
                    )
                    
                    if sin_datos_criticos:
                        archivos_pendientes.append(file_info)
                        logger.debug(
                            "Archivo %s PRIORITARIO: proceso %s sin razon_social ni ruc",
                            file_info["file_name"],
                            codigo,
                        )
                    else:
                        # Si ya tiene razon_social o ruc, omitir (ya fue procesado)
                        archivos_omitidos.append(file_info["file_name"])
                        logger.debug(
                            "Archivo %s omitido: proceso %s ya tiene datos de contratista",
                            file_info["file_name"],
                            codigo,
                        )
            
            conn.close()
            
            logger.info(
                "Filtrado completado: %d pendientes, %d omitidos (ya tienen datos), %d no existen en BD",
                len(archivos_pendientes),
                len(archivos_omitidos),
                len(archivos_no_existen),
            )
            
        except Exception as exc:
            logger.exception("Error al filtrar archivos procesados: %s", exc)
            # En caso de error, procesar todos los archivos (se validará en update_postgres)
            archivos_pendientes = list(codigos_proceso_map.values())
            logger.warning(
                "Error en filtrado batch, procesando todos los archivos: %s", exc
            )
        
        # Limitar al BATCH_SIZE
        total_pendientes = len(archivos_pendientes)
        if total_pendientes > BATCH_SIZE:
            logger.warning(
                "Se encontraron %d archivos pendientes; se procesarán solo los primeros %d en esta corrida. Ajusta BATCH_SIZE si deseas otro valor.",
                total_pendientes,
                BATCH_SIZE,
            )
            archivos_pendientes = archivos_pendientes[:BATCH_SIZE]

        return archivos_pendientes

    @task
    def read_txt_batch(file_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Lee el contenido de un archivo TXT.
        
        Args:
            file_info: Diccionario con file_path, file_name, codigo_proceso
            
        Returns:
            Diccionario con el contenido del archivo y metadatos
        """
        file_path = Path(file_info["file_path"])
        
        if not file_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        try:
            contenido = file_path.read_text(encoding="utf-8", errors="ignore")
            
            logger.info(
                "Archivo leído: %s (código: %s, tamaño: %d caracteres)",
                file_info["file_name"],
                file_info["codigo_proceso"],
                len(contenido),
            )
            
            return {
                **file_info,
                "contenido": contenido,
            }
            
        except Exception as exc:
            logger.exception("Error al leer archivo %s: %s", file_path, exc)
            raise

    @task
    def llm_extract_contratista(file_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrae la información del contratista usando el nuevo sistema LLM robusto.
        
        Usa:
        - Pre-filtro avanzado para aislar bloques relevantes
        - Prompt mejorado y específico
        - Validación y normalización de datos
        
        Args:
            file_data: Diccionario con contenido y metadatos del archivo
            
        Returns:
            Diccionario con datos extraídos del contratista usando IA
        """
        contenido = file_data["contenido"]
        codigo_proceso = file_data["codigo_proceso"]
        file_name = file_data["file_name"]
        
        try:
            # Usar el nuevo sistema LLM robusto
            logger.info("Iniciando extracción con sistema LLM robusto para %s (proceso: %s)", file_name, codigo_proceso)
            
            datos_contratista = llm(contenido, codigo_proceso, file_name)
            
            # Log de datos extraídos
            datos_encontrados = {k: v for k, v in datos_contratista.items() if v is not None}
            logger.info(
                "Datos extraídos para %s (proceso: %s). Campos detectados: %s",
                file_name,
                codigo_proceso,
                list(datos_encontrados.keys()),
            )
            
            # Log detallado de cada campo
            for campo, valor in datos_contratista.items():
                if valor:
                    logger.info("  - %s: %s", campo, str(valor)[:100])
            
            if not any(datos_contratista.values()):
                logger.warning(
                    "No se encontró información de contratista en %s",
                    file_name,
                )
            
            return {
                **file_data,
                "contratista_data": datos_contratista,
            }
            
        except AirflowSkipException:
            # Re-lanzar para que Airflow maneje el skip correctamente (solo cuando no hay API key)
            raise
        except Exception as exc:
            logger.exception("Error inesperado en extracción con IA para %s: %s", file_name, exc)
            # Lanzar excepción normal para que Airflow pueda reintentar la tarea más tarde
            raise Exception(f"Error en extracción IA: {exc}") from exc

    @task
    def update_postgres(file_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Actualiza los datos del contratista y la ficha general en PostgreSQL.

        IMPORTANTE: Solo actualiza registros existentes, NO crea nuevos.
        El código de proceso viene del nombre del archivo TXT.
        """
        codigo_proceso = file_data.get("codigo_proceso")
        if not codigo_proceso:
            logger.error("codigo_proceso no encontrado en file_data. Keys disponibles: %s", list(file_data.keys()))
            raise ValueError("codigo_proceso es requerido")
        
        # Intentar conexión con reintentos
        max_conn_retries = 3
        conn = None
        for conn_intento in range(max_conn_retries):
            try:
                hook = get_postgres_hook()
                conn = hook.get_conn()
                conn.autocommit = True
                logger.info("Conexión a PostgreSQL establecida exitosamente (intento %d/%d)", conn_intento + 1, max_conn_retries)
                break  # Éxito, salir del loop
            except Exception as conn_exc:
                error_str = str(conn_exc).lower()
                is_connection_error = (
                    "connection refused" in error_str or 
                    "could not connect" in error_str or 
                    "timeout" in error_str or
                    "connection" in error_str and "failed" in error_str
                )
                
                if is_connection_error:
                    if conn_intento < max_conn_retries - 1:
                        wait_time = (conn_intento + 1) * 2  # 2, 4, 6 segundos
                        logger.warning(
                            "Error de conexión a PostgreSQL para proceso %s (intento %d/%d): %s. "
                            "Reintentando en %d segundos...",
                            codigo_proceso,
                            conn_intento + 1,
                            max_conn_retries,
                            conn_exc,
                            wait_time
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            "Error de conexión a PostgreSQL para proceso %s después de %d intentos: %s. "
                            "Verifica que el servidor PostgreSQL esté accesible en la conexión '%s'. "
                            "Host configurado: verifica conectividad de red.",
                            codigo_proceso,
                            max_conn_retries,
                            conn_exc,
                            POSTGRES_CONN_ID
                        )
                        # Lanzar excepción para que Airflow reintente automáticamente más tarde
                        raise Exception(f"Error de conexión a PostgreSQL después de {max_conn_retries} intentos: {conn_exc}") from conn_exc
                else:
                    # Otros errores de conexión
                    logger.exception("Error inesperado al conectar a PostgreSQL para proceso %s: %s", codigo_proceso, conn_exc)
                    raise
        
        if conn is None:
            raise RuntimeError(f"No se pudo establecer conexión a PostgreSQL después de {max_conn_retries} intentos")

        datos_contratista = file_data.get("contratista_data") or {}
        file_name = file_data.get("file_name")
        
        # Log inicial para diagnóstico
        logger.info(
            "=== INICIO update_postgres para proceso %s ===\n"
            "Datos contratista recibidos (tipo: %s, keys: %s): %s\n"
            "File name: %s\n"
            "Todas las keys en file_data: %s",
            codigo_proceso,
            type(datos_contratista).__name__,
            list(datos_contratista.keys()) if isinstance(datos_contratista, dict) else "NO ES DICT",
            datos_contratista,
            file_name,
            list(file_data.keys()),
        )

        schema_name, table_name = (
            CONTRATOS_TABLE.split(".", 1)
            if "." in CONTRATOS_TABLE
            else ("public", CONTRATOS_TABLE)
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    """,
                    (schema_name, table_name),
                )
                available_columns = {row[0] for row in cursor.fetchall()}
                
                logger.info(
                    "Columnas disponibles en tabla %s para proceso %s: %s",
                    CONTRATOS_TABLE,
                    codigo_proceso,
                    sorted(available_columns),
                )
                
                # Verificar que las columnas esperadas existan
                columnas_faltantes = set(CONTRATISTA_FIELDS) - available_columns
                if columnas_faltantes:
                    logger.warning(
                        "Algunas columnas esperadas no existen en la tabla %s: %s",
                        CONTRATOS_TABLE,
                        sorted(columnas_faltantes),
                    )

                query_existe = f"""
                    SELECT codigo_proceso
                    FROM {CONTRATOS_TABLE}
                    WHERE codigo_proceso = %s
                    LIMIT 1
                """

                cursor.execute(query_existe, (codigo_proceso,))
                existe = cursor.fetchone()

                if not existe:
                    logger.warning(
                        "Registro con código_proceso %s no existe en BD. Solo actualizamos existentes.",
                        codigo_proceso,
                    )
                    return {
                        **file_data,
                        "update_status": "skipped",
                        "update_reason": "Registro no existe en BD",
                    }

                # NO validar si hay datos aquí - siempre intentar actualizar si hay set_clauses
                # La validación se hace después de generar set_clauses

                # Guardar JSON completo del LLM en metadata
                metadata_contratista = {
                    "model": LLM_MODEL,
                    "timestamp": datetime.utcnow().isoformat(),
                    "campos_detectados": [k for k, v in datos_contratista.items() if v is not None],
                    "datos_llm": datos_contratista,  # JSON completo del LLM
                }
                metadata_final = {
                    "llm_contratista": metadata_contratista,
                }

                set_clauses: List[str] = []
                params: List[Any] = []

                def add_update(column: str, value: Any, template: str | None = None) -> None:
                    if column not in available_columns:
                        logger.debug("Columna %s no existe en tabla %s, omitiendo", column, CONTRATOS_TABLE)
                        return
                    # Solo actualizar si hay un valor (no None y no vacío)
                    if value is None or (isinstance(value, str) and not value.strip()):
                        logger.debug("Omitiendo actualización de columna %s: valor es None o vacío", column)
                        return
                    if template is None:
                        # Actualizar directamente sin COALESCE para que siempre se actualice cuando hay valor
                        set_clauses.append(f"{column} = %s")
                    else:
                        set_clauses.append(template)
                    params.append(value)
                    logger.debug("Agregando actualización para columna %s con valor: %s", column, value)

                # PRIMERO: Agregar updated_at si está disponible (para asegurar al menos un set_clause)
                # Esto garantiza que siempre se ejecute el UPDATE, incluso si todos los valores son None
                if "updated_at" in available_columns:
                    set_clauses.append("updated_at = NOW()")
                    logger.debug("Agregado updated_at = NOW()")
                elif "fecha_actualizacion" in available_columns:
                    set_clauses.append("fecha_actualizacion = NOW()")
                    logger.debug("Agregado fecha_actualizacion = NOW()")

                # Agregar campos del contratista
                logger.info(
                    "Datos contratista recibidos para proceso %s: %s",
                    codigo_proceso,
                    {k: v for k, v in datos_contratista.items() if v is not None},
                )
                
                # VALIDAR Y LIMPIAR ANTES DE GUARDAR (menos estricto, solo limpiar prefijos)
                # razon_social: solo limpiar prefijos, no validar estrictamente
                razon_raw = datos_contratista.get("razon_social")
                if razon_raw:
                    razon_limpia = limpiar_razon_social(razon_raw)
                    if razon_limpia and len(razon_limpia) >= 3:
                        # Validación menos estricta: solo verificar que no sea claramente inválida
                        if not re.match(r"^(Comparecen|Partes|quienes|será|Av\.|Calle|Sector)", razon_limpia, re.IGNORECASE):
                            datos_contratista["razon_social"] = razon_limpia
                            logger.debug("razon_social limpiada: %s -> %s", razon_raw[:50], razon_limpia[:50])
                        else:
                            logger.warning("razon_social rechazada (frase inválida): %s", razon_raw[:100])
                            datos_contratista["razon_social"] = None
                    else:
                        logger.warning("razon_social inválida (muy corta o None después de limpiar): %s", razon_raw[:100])
                        datos_contratista["razon_social"] = None
                else:
                    datos_contratista["razon_social"] = None
                
                # representante: limpiar y validar formato básico
                representante_raw = datos_contratista.get("representante")
                if representante_raw:
                    representante_limpio = limpiar_nombre_persona(representante_raw)
                    if representante_limpio:
                        # Validación menos estricta: solo verificar formato básico (2-4 palabras, sin números, sin cargos)
                        palabras = representante_limpio.split()
                        tiene_numeros = bool(re.search(r"\d", representante_limpio))
                        tiene_cargos = bool(re.search(r"(Coordinador|Director|Administrador|Gerente|Jefe|Secretario|Ministro|Alcalde|Prefecto|Gobernador)", representante_limpio, re.IGNORECASE))
                        tiene_empresa = bool(re.search(r"(LTDA|S\.A\.|CONSORCIO|CIA|EP|COMPAÑ[ÍI]A)", representante_limpio, re.IGNORECASE))
                        
                        if 2 <= len(palabras) <= 4 and not tiene_numeros and not tiene_cargos and not tiene_empresa:
                            datos_contratista["representante"] = representante_limpio
                            logger.debug("representante validado: %s -> %s", representante_raw[:50], representante_limpio[:50])
                        else:
                            logger.warning("representante inválido (palabras=%d, números=%s, cargos=%s, empresa=%s): %s", 
                                         len(palabras), tiene_numeros, tiene_cargos, tiene_empresa, representante_raw[:100])
                            datos_contratista["representante"] = None
                    else:
                        logger.warning("representante no se pudo limpiar: %s", representante_raw[:100])
                        datos_contratista["representante"] = None
                else:
                    datos_contratista["representante"] = None
                
                for campo in CONTRATISTA_FIELDS:
                    valor = datos_contratista.get(campo)
                    # Solo agregar si hay un valor válido (no None y no vacío)
                    # Esto asegura que solo actualizamos cuando realmente hay datos
                    if campo in available_columns:
                        if valor is not None and (not isinstance(valor, str) or valor.strip()):
                            add_update(campo, valor)
                            logger.debug("Campo contratista '%s' agregado con valor: %s", campo, valor)
                        else:
                            logger.debug("Campo contratista '%s' omitido: valor es None o vacío", campo)
                    else:
                        logger.warning("Campo contratista '%s' no existe en tabla %s", campo, CONTRATOS_TABLE)

                if "fuente_archivo" in available_columns:
                    add_update("fuente_archivo", file_name)

                if "metadata" in available_columns:
                    set_clauses.append("metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb")
                    params.append(json.dumps(metadata_final))

                logger.info(
                    "Total de set_clauses generados para proceso %s: %d. Clauses: %s",
                    codigo_proceso,
                    len(set_clauses),
                    [c.split("=")[0].strip() for c in set_clauses] if set_clauses else [],
                )
                
                # CRÍTICO: Verificar que siempre haya al menos un set_clause
                # Si no hay set_clauses después de todo, es un error crítico porque ya agregamos updated_at
                if not set_clauses:
                    logger.error(
                        "ERROR CRÍTICO: No se generaron set_clauses después de procesar campos del contratista. "
                        "Proceso: %s, Campos esperados: %s, Columnas disponibles: %s, "
                        "Datos contratista: %s",
                        codigo_proceso,
                        CONTRATISTA_FIELDS,
                        sorted(available_columns),
                        datos_contratista,
                    )
                    # Último recurso: intentar agregar cualquier columna de fecha disponible
                    fecha_columns = [col for col in available_columns if "fecha" in col.lower() or "updated" in col.lower() or "actualizacion" in col.lower()]
                    if fecha_columns:
                        set_clauses.append(f"{fecha_columns[0]} = NOW()")
                        logger.warning(
                            "No se generaron set_clauses, agregando %s como último recurso para proceso %s",
                            fecha_columns[0],
                            codigo_proceso,
                        )

                # CRÍTICO: Verificar que siempre haya al menos un set_clause
                # Si no hay set_clauses después de todo, es un error crítico
                if not set_clauses:
                    logger.error(
                        "ERROR CRÍTICO: No se generaron set_clauses para proceso %s después de procesar todos los campos. "
                        "Esto NO debería pasar porque ya agregamos updated_at. "
                        "Proceso: %s, Campos esperados: %s, Columnas disponibles: %s, "
                        "Datos contratista: %s",
                        codigo_proceso,
                        codigo_proceso,
                        CONTRATISTA_FIELDS,
                        sorted(available_columns),
                        datos_contratista,
                    )
                    # Esto no debería pasar nunca porque ya agregamos updated_at
                    # Si pasa, hay un problema grave - lanzar excepción
                    raise RuntimeError(
                        f"ERROR CRÍTICO: No se generaron set_clauses para proceso {codigo_proceso}. "
                        f"Esto NO debería pasar. Columnas disponibles: {sorted(available_columns)}"
                    )

                update_sql = f"""
                    UPDATE {CONTRATOS_TABLE}
                    SET {', '.join(set_clauses)}
                    WHERE codigo_proceso = %s
                """

                logger.info(
                    "Ejecutando UPDATE para proceso %s con %d set_clauses. SQL: %s",
                    codigo_proceso,
                    len(set_clauses),
                    update_sql[:500] if len(update_sql) > 500 else update_sql,
                )
                
                # Verificar que la conexión sigue activa antes de ejecutar
                if conn.closed:
                    logger.error("Conexión a PostgreSQL cerrada inesperadamente para proceso %s", codigo_proceso)
                    raise Exception("Conexión a PostgreSQL cerrada inesperadamente")
                
                try:
                    cursor.execute(
                        update_sql,
                        (*params, codigo_proceso),
                    )
                except Exception as update_exc:
                    error_str = str(update_exc).lower()
                    # Si es un error de conexión durante el UPDATE, reintentar
                    if "connection" in error_str and ("lost" in error_str or "closed" in error_str or "refused" in error_str):
                        logger.warning(
                            "Error de conexión durante UPDATE para proceso %s: %s. "
                            "La conexión puede haberse perdido. Airflow reintentará la tarea completa.",
                            codigo_proceso,
                            update_exc
                        )
                        raise Exception(f"Error de conexión durante UPDATE: {update_exc}") from update_exc
                    else:
                        # Otros errores de SQL, re-lanzar
                        raise

                # cursor.rowcount puede ser 0 si:
                # 1. El registro no existe (pero ya verificamos esto arriba)
                # 2. Todos los valores son iguales a los actuales (COALESCE no cambió nada)
                # En ambos casos, consideramos éxito si el UPDATE se ejecutó sin errores
                if cursor.rowcount == 0:
                    logger.warning(
                        "UPDATE ejecutado pero rowcount = 0 para proceso %s. "
                        "Esto puede significar que los valores no cambiaron o el registro no existe.",
                        codigo_proceso,
                    )
                    # Verificar nuevamente si el registro existe
                    cursor.execute(query_existe, (codigo_proceso,))
                    existe_despues = cursor.fetchone()
                    if not existe_despues:
                        return {
                            **file_data,
                            "update_status": "skipped",
                            "update_reason": "Registro no existe después del UPDATE",
                        }
                    # Si existe, consideramos éxito (los valores no cambiaron pero el UPDATE se ejecutó)
                    logger.info(
                        "Registro %s existe pero valores no cambiaron (rowcount=0). Considerando éxito.",
                        codigo_proceso,
                    )

                logger.info(
                    "Registro %s actualizado correctamente (rowcount: %d)",
                    codigo_proceso,
                    cursor.rowcount,
                )
                return {
                    **file_data,
                    "update_status": "success",
                    "update_timestamp": datetime.utcnow().isoformat(),
                }

        except Exception as exc:
            error_str = str(exc).lower()
            
            # Detectar errores de conexión específicos
            if "connection refused" in error_str or "could not connect" in error_str or "timeout" in error_str:
                logger.error(
                    "Error de conexión a PostgreSQL para proceso %s: %s. "
                    "El servidor puede estar caído o inaccesible. Airflow reintentará automáticamente.",
                    codigo_proceso,
                    exc,
                )
                # Lanzar excepción para que Airflow reintente
                raise Exception(f"Error de conexión a PostgreSQL: {exc}") from exc
            elif "column" in error_str or "does not exist" in error_str:
                logger.error(
                    "Error de esquema para proceso %s: %s. Verifica que las columnas existan en la tabla %s. Columnas esperadas: %s",
                    codigo_proceso,
                    exc,
                    CONTRATOS_TABLE,
                    CONTRATISTA_FIELDS,
                )
                raise
            else:
                logger.exception(
                    "Error inesperado al actualizar datos para proceso %s: %s",
                    codigo_proceso,
                    exc,
                )
                raise
        finally:
            try:
                if 'conn' in locals() and conn:
                    conn.close()
            except Exception as close_exc:
                logger.warning("Error al cerrar conexión PostgreSQL: %s", close_exc)

    @task
    def log_results(results: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Registra los resultados finales del procesamiento.
        
        Args:
            results: Lista de resultados de las tareas de update
            
        Returns:
            Diccionario con estadísticas
        """
        if not results:
            logger.info("No hubo resultados de actualización que registrar.")
            return {"total": 0, "exitosos": 0, "omitidos": 0, "errores": 0}

        total = len(results)
        exitosos = sum(1 for r in results if r.get("update_status") == "success")
        omitidos = sum(1 for r in results if r.get("update_status") == "skipped")
        errores = total - exitosos - omitidos
        
        for item in results:
            codigo = item.get("codigo_proceso", "desconocido")
            status = item.get("update_status")
            if status == "success":
                datos_contratista = item.get("contratista_data", {})
                # Mostrar solo campos que tienen valor (no None)
                campos_con_datos = {k: v for k, v in datos_contratista.items() if v is not None}
                if campos_con_datos:
                    logger.info(
                        "Contrato %s ACTUALIZADO con datos: %s",
                        codigo,
                        " | ".join(f"{k}={v}" for k, v in campos_con_datos.items()),
                    )
                else:
                    logger.info(
                        "Contrato %s procesado pero LLM no encontró datos en el texto",
                        codigo,
                    )
            else:
                logger.info(
                    "Contrato %s no actualizado -> estado=%s | motivo=%s",
                    codigo,
                    status,
                    item.get("update_reason"),
                )
        
        estadisticas = {
            "total": total,
            "exitosos": exitosos,
            "omitidos": omitidos,
            "errores": errores,
        }
        
        logger.info(
            "Procesamiento completado - Total: %d, Exitosos: %d, Omitidos: %d, Errores: %d",
            total,
            exitosos,
            omitidos,
            errores,
        )
        
        return estadisticas

    # Definir el flujo del DAG
    archivos = list_txt_files()
    archivos_filtrados = filter_processed(archivos)
    
    # Procesar cada archivo (usando expand para paralelismo)
    contenidos = read_txt_batch.expand(file_info=archivos_filtrados)
    datos_contratista = llm_extract_contratista.expand(file_data=contenidos)
    resultados = update_postgres.expand(file_data=datos_contratista)

    # Registrar resultados finales
    # Cuando expand() recibe lista vacía, las tareas se marcan como SKIPPED
    # y el XComArg se resuelve automáticamente a lista vacía en la tarea downstream
    log_results(resultados)


# Instanciar el DAG
extract_contratista_etl()
