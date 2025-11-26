"""
Backend Flask para el dashboard de contratos con chatbot.
"""
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, static_folder='.')
CORS(app)

# Configuración de PostgreSQL para oppdesarrollo
# Intentar obtener configuración de Airflow primero, luego usar variables de entorno
POSTGRES_CONFIG = None

# Intentar obtener configuración desde Airflow (si está disponible)
try:
    from airflow.hooks.postgres import PostgresHook
    POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID", "oppdesarrollo_postgres")
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        POSTGRES_CONFIG = {
            'host': conn.info.host,
            'port': conn.info.port,
            'database': conn.info.dbname,
            'user': conn.info.user,
            'password': conn.info.password
        }
        conn.close()
        print(f"✓ Configuración obtenida de Airflow connection: {POSTGRES_CONN_ID}")
        print(f"  Base de datos: {POSTGRES_CONFIG['database']} en {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
    except Exception as e:
        print(f"⚠ No se pudo obtener configuración de Airflow: {e}")
        print("  Usando variables de entorno...")
except ImportError:
    print("⚠ Airflow no disponible, usando variables de entorno...")

# Si no se obtuvo de Airflow, usar variables de entorno o valores por defecto
if POSTGRES_CONFIG is None:
    # Si POSTGRES_HOST está configurado, usarlo; si no, usar localhost
    POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
    if not POSTGRES_HOST:
        POSTGRES_HOST = 'localhost'
    if os.environ.get('DOCKER_ENV') == 'true':
        POSTGRES_HOST = 'postgres'
    
    POSTGRES_CONFIG = {
        'host': POSTGRES_HOST,
        'port': int(os.environ.get('POSTGRES_PORT', '5432')),
        'database': os.environ.get('OPPDESARROLLO_DB', 'oppdesarrollo'),
        'user': os.environ.get('OPPDESARROLLO_USER', 'postgres'),
        'password': os.environ.get('OPPDESARROLLO_PASSWORD', 'postgres')
    }
    print(f"📊 Usando configuración desde variables de entorno")
    print(f"  Base de datos: {POSTGRES_CONFIG['database']} en {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")

print(f"✓ Configuración PostgreSQL lista: {POSTGRES_CONFIG['database']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")

# Configuración de Gemini
GEMINI_API_KEY = os.environ.get('LLM_API_KEY')
GEMINI_MODEL = os.environ.get('LLM_MODEL', 'gemini-2.5-pro')

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

CONTRATOS_TABLE = "public.sync_contratos"

def get_db_connection():
    """Obtiene una conexión a PostgreSQL."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        print(f"❌ Error de conexión a PostgreSQL: {error_msg}")
        print(f"   Base de datos: {POSTGRES_CONFIG['database']}")
        print(f"   Host: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
        print(f"   Usuario: {POSTGRES_CONFIG['user']}")
        if "password authentication failed" in error_msg.lower():
            print("   ⚠ Verifica las credenciales (usuario/contraseña)")
        elif "could not connect" in error_msg.lower() or "connection refused" in error_msg.lower():
            print("   ⚠ Verifica que PostgreSQL esté corriendo y accesible")
        return None
    except Exception as e:
        print(f"❌ Error inesperado conectando a PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return None

@app.route('/')
def index():
    """Sirve el archivo HTML principal."""
    return send_from_directory('.', 'contratos_dashboard.html')

@app.route('/api/contratos', methods=['GET'])
def get_contratos():
    """Obtiene todos los contratos con datos de contratista."""
    conn = get_db_connection()
    if not conn:
        return jsonify({
            'error': 'No se pudo conectar a la base de datos',
            'details': f"Verifica que PostgreSQL esté corriendo y accesible en {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}",
            'database': POSTGRES_CONFIG['database']
        }), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Obtener contratos con datos de contratista (donde al menos uno de los campos no sea NULL)
        query = f"""
            SELECT 
                codigo_proceso,
                razon_social,
                representante,
                ruc,
                telefono,
                mail,
                domicilio,
                updated_at,
                fecha_actualizacion
            FROM {CONTRATOS_TABLE}
            WHERE (
                razon_social IS NOT NULL OR
                representante IS NOT NULL OR
                ruc IS NOT NULL OR
                telefono IS NOT NULL OR
                mail IS NOT NULL OR
                domicilio IS NOT NULL
            )
            ORDER BY updated_at DESC NULLS LAST, fecha_actualizacion DESC NULLS LAST
            LIMIT 1000
        """
        
        cursor.execute(query)
        contratos = cursor.fetchall()
        
        # Convertir a lista de diccionarios
        result = [dict(row) for row in contratos]
        
        cursor.close()
        conn.close()
        
        return jsonify({'contratos': result, 'total': len(result)})
    
    except psycopg2.Error as e:
        if conn:
            conn.close()
        error_msg = str(e)
        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
            return jsonify({
                'error': 'Tabla no encontrada',
                'details': f"La tabla {CONTRATOS_TABLE} no existe en la base de datos {POSTGRES_CONFIG['database']}",
                'suggestion': 'Verifica que el DAG haya ejecutado y creado la tabla'
            }), 500
        return jsonify({
            'error': 'Error de base de datos',
            'details': error_msg,
            'database': POSTGRES_CONFIG['database']
        }), 500
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({
            'error': 'Error inesperado',
            'details': str(e),
            'database': POSTGRES_CONFIG['database']
        }), 500

@app.route('/api/contrato/<codigo_proceso>', methods=['GET'])
def get_contrato(codigo_proceso):
    """Obtiene un contrato específico por código de proceso."""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'No se pudo conectar a la base de datos'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = f"""
            SELECT 
                codigo_proceso,
                razon_social,
                representante,
                ruc,
                telefono,
                mail,
                domicilio,
                updated_at,
                fecha_actualizacion
            FROM {CONTRATOS_TABLE}
            WHERE codigo_proceso = %s
        """
        
        cursor.execute(query, (codigo_proceso,))
        contrato = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if contrato:
            return jsonify({'contrato': dict(contrato)})
        else:
            return jsonify({'error': 'Contrato no encontrado'}), 404
    
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'error': f'Error al obtener contrato: {str(e)}'}), 500

@app.route('/api/chatbot', methods=['POST'])
def chatbot():
    """Endpoint para el chatbot que responde preguntas sobre contratos."""
    if not GEMINI_API_KEY:
        return jsonify({'error': 'API key de Gemini no configurada'}), 500
    
    data = request.json
    pregunta = data.get('pregunta', '')
    codigo_proceso = data.get('codigo_proceso', None)
    
    if not pregunta:
        return jsonify({'error': 'Pregunta requerida'}), 400
    
    try:
        # Obtener datos del contrato si se especifica un código
        contexto_contrato = ""
        if codigo_proceso:
            conn = get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor(cursor_factory=RealDictCursor)
                    query = f"""
                        SELECT 
                            codigo_proceso,
                            razon_social,
                            representante,
                            ruc,
                            telefono,
                            mail,
                            domicilio
                        FROM {CONTRATOS_TABLE}
                        WHERE codigo_proceso = %s
                    """
                    cursor.execute(query, (codigo_proceso,))
                    contrato = cursor.fetchone()
                    cursor.close()
                    conn.close()
                    
                    if contrato:
                        contrato_dict = dict(contrato)
                        contexto_contrato = f"""
Información del contrato {codigo_proceso}:
- Razón Social: {contrato_dict.get('razon_social', 'N/A')}
- Representante: {contrato_dict.get('representante', 'N/A')}
- RUC: {contrato_dict.get('ruc', 'N/A')}
- Teléfono: {contrato_dict.get('telefono', 'N/A')}
- Email: {contrato_dict.get('mail', 'N/A')}
- Domicilio: {contrato_dict.get('domicilio', 'N/A')}
"""
                except Exception as e:
                    print(f"Error obteniendo contrato para contexto: {e}")
        
        # Configurar el modelo de Gemini
        model = genai.GenerativeModel(GEMINI_MODEL)
        
        # Crear el prompt con contexto
        prompt = f"""Eres un asistente virtual especializado en contratos públicos. 
Responde preguntas sobre contratos de manera clara, profesional y concisa.

{contexto_contrato if contexto_contrato else "Puedes responder preguntas generales sobre contratos públicos."}

Pregunta del usuario: {pregunta}

Responde de manera amigable y profesional, en español."""
        
        # Generar respuesta
        response = model.generate_content(prompt)
        
        # Extraer texto de la respuesta
        respuesta_texto = ""
        if hasattr(response, 'text'):
            respuesta_texto = response.text
        elif hasattr(response, 'candidates') and len(response.candidates) > 0:
            if hasattr(response.candidates[0], 'content'):
                if hasattr(response.candidates[0].content, 'parts'):
                    respuesta_texto = "".join([part.text for part in response.candidates[0].content.parts if hasattr(part, 'text')])
        
        if not respuesta_texto:
            respuesta_texto = "Lo siento, no pude generar una respuesta en este momento."
        
        return jsonify({'respuesta': respuesta_texto})
    
    except Exception as e:
        return jsonify({'error': f'Error al generar respuesta: {str(e)}'}), 500

if __name__ == '__main__':
    port = int(os.environ.get('FLASK_PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)

