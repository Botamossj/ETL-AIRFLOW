"""
Utilidades ETL para extracción de información de contratistas.
Módulo reutilizable para procesamiento de contratos.
"""

import logging
import re
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def extract_codigo_proceso_from_filename(filename: str) -> str:
    """
    Extrae el código de proceso desde el nombre del archivo.
    Usa el mismo patrón regex que el script force_codigo.
    
    Args:
        filename: Nombre del archivo (ej: "PROC-2024-001.txt")
        
    Returns:
        Código de proceso en mayúsculas
    """
    # Patrones comunes para códigos de proceso en nombres de archivo
    patrones = [
        r"([A-Z0-9\-_/]{6,})",  # Código alfanumérico con guiones/barras
        r"PROC[_-]?([A-Z0-9\-_/]+)",  # Prefijo PROC-
        r"([A-Z]{2,}\d{4,})",  # Letras seguidas de números
    ]
    
    # Intentar extraer del nombre sin extensión
    stem = Path(filename).stem.upper()
    
    for patron in patrones:
        match = re.search(patron, stem, re.IGNORECASE)
        if match:
            codigo = match.group(1) if match.lastindex else match.group(0)
            return codigo.strip().upper()
    
    # Fallback: usar el stem completo
    return stem


def extract_codigo_proceso_from_text(text: str, fallback: str) -> str:
    """
    Extrae el código de proceso desde el contenido del texto.
    
    Args:
        text: Contenido del archivo TXT
        fallback: Código a usar si no se encuentra en el texto
        
    Returns:
        Código de proceso en mayúsculas
    """
    patrones = [
        r"(?:codigo|c[oó]d)\s*(?:del\s+)?proceso[:\s-]*([A-Z0-9\-_/]{6,})",
        r"(?:proc(?:eso)?\s*no\.?|expediente)[:\s-]*([A-Z0-9\-_/]{6,})",
    ]
    for patron in patrones:
        match = re.search(patron, text, re.IGNORECASE)
        if match:
            return match.group(1).strip().upper()
    return fallback.upper()


def extract_razon_social(text: str) -> Optional[str]:
    """
    Extrae la razón social del contratista del texto.
    
    Args:
        text: Contenido del archivo TXT
        
    Returns:
        Razón social encontrada o None
    """
    patrones = [
        r"(?:raz[oó]n\s+social|denominaci[oó]n\s+social|empresa|contratista)[:\s-]+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ\s.,&]{10,100})",
        r"(?:raz[oó]n\s+social|denominaci[oó]n)[:\s]*\n\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ\s.,&]{10,100})",
        r"contratista[:\s-]+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ\s.,&]{10,100})",
    ]
    
    for patron in patrones:
        match = re.search(patron, text, re.IGNORECASE | re.MULTILINE)
        if match:
            razon = match.group(1).strip()
            # Limpiar: remover líneas múltiples y espacios excesivos
            razon = " ".join(razon.split())
            if len(razon) >= 5:  # Validar longitud mínima
                return razon[:200]  # Limitar longitud
    return None


def extract_ruc(text: str) -> Optional[str]:
    """
    Extrae el RUC del contratista del texto.
    
    Args:
        text: Contenido del archivo TXT
        
    Returns:
        RUC encontrado o None
    """
    # RUC peruano: 11 dígitos
    patrones = [
        r"(?:ruc|r\.?u\.?c\.?)[:\s-]+(\d{11})",
        r"ruc[:\s]*\n\s*(\d{11})",
        r"(\d{11})(?:\s|$)",  # 11 dígitos consecutivos
    ]
    
    for patron in patrones:
        match = re.search(patron, text, re.IGNORECASE | re.MULTILINE)
        if match:
            ruc = match.group(1).strip()
            if len(ruc) == 11 and ruc.isdigit():
                return ruc
    return None


def extract_telefono(text: str) -> Optional[str]:
    """
    Extrae el teléfono del contratista del texto.
    
    Args:
        text: Contenido del archivo TXT
        
    Returns:
        Teléfono encontrado o None
    """
    # Teléfonos peruanos: 7-9 dígitos, pueden tener prefijos
    patrones = [
        r"(?:tel[ée]fono|tel\.?|f[oó]no|celular|m[óo]vil)[:\s-]+([+]?[\d\s\-\(\)]{7,15})",
        r"(?:tel[ée]fono|tel\.?)[:\s]*\n\s*([+]?[\d\s\-\(\)]{7,15})",
        r"(\+?51[\s\-]?[\d\s\-]{7,12})",  # Formato internacional Perú
        r"(\d{7,9})",  # 7-9 dígitos simples
    ]
    
    for patron in patrones:
        match = re.search(patron, text, re.IGNORECASE | re.MULTILINE)
        if match:
            telefono = match.group(1).strip()
            # Limpiar: remover espacios, guiones, paréntesis
            telefono_limpio = re.sub(r'[\s\-\(\)]', '', telefono)
            if 7 <= len(telefono_limpio) <= 15:
                return telefono_limpio
    return None


def extract_mail(text: str) -> Optional[str]:
    """
    Extrae el correo electrónico (mail) del contratista del texto.
    
    Args:
        text: Contenido del archivo TXT
        
    Returns:
        Correo electrónico encontrado o None
    """
    patrones = [
        r"(?:correo|email|e-mail|mail)[:\s-]+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        r"(?:correo|email|mail)[:\s]*\n\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",  # Email genérico
    ]
    
    for patron in patrones:
        match = re.search(patron, text, re.IGNORECASE | re.MULTILINE)
        if match:
            mail = match.group(1).strip().lower()
            if "@" in mail and "." in mail.split("@")[1]:
                return mail[:100]  # Limitar longitud
    return None


def extract_domicilio(text: str) -> Optional[str]:
    """
    Extrae el domicilio del contratista del texto.
    
    Args:
        text: Contenido del archivo TXT
        
    Returns:
        Domicilio encontrado o None
    """
    patrones = [
        r"(?:domicilio|direcci[oó]n|direcci[oó]n\s+fiscal|domicilio\s+fiscal)[:\s-]+([A-ZÁÉÍÓÚÑ0-9][A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s.,#\-]{15,200})",
        r"(?:domicilio|direcci[oó]n)[:\s]*\n\s*([A-ZÁÉÍÓÚÑ0-9][A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s.,#\-]{15,200})",
        r"domicilio\s+fiscal[:\s-]+([A-ZÁÉÍÓÚÑ0-9][A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s.,#\-]{15,200})",
    ]
    
    for patron in patrones:
        match = re.search(patron, text, re.IGNORECASE | re.MULTILINE)
        if match:
            domicilio = match.group(1).strip()
            # Limpiar: remover líneas múltiples y espacios excesivos
            domicilio = " ".join(domicilio.split())
            if len(domicilio) >= 10:  # Validar longitud mínima
                return domicilio[:300]  # Limitar longitud
    return None


def extract_contratista_data(text: str) -> Dict[str, Optional[str]]:
    """
    Extrae toda la información del contratista del texto.
    
    Args:
        text: Contenido del archivo TXT
        
    Returns:
        Diccionario con los campos del contratista (mapeados a nombres de columna de BD)
    """
    return {
        "razon_social": extract_razon_social(text),
        "ruc": extract_ruc(text),
        "telefono": extract_telefono(text),
        "mail": extract_mail(text),  # Nota: se llama "mail" en la BD, no "correo"
        "domicilio": extract_domicilio(text),
    }

