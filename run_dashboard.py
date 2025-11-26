"""
Script para ejecutar el dashboard de contratos.
Asegúrate de tener las variables de entorno configuradas en .env
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Verificar variables de entorno necesarias
required_vars = ['AIRFLOW_DATABASE_USER', 'AIRFLOW_DATABASE_PASSWORD', 'AIRFLOW_DATABASE_NAME']
missing_vars = [var for var in required_vars if not os.environ.get(var)]

if missing_vars:
    print(f"⚠️  Advertencia: Variables de entorno faltantes: {', '.join(missing_vars)}")
    print("Usando valores por defecto...")

# Importar y ejecutar la app
from app import app

if __name__ == '__main__':
    port = int(os.environ.get('FLASK_PORT', 5000))
    print("=" * 60)
    print("🚀 Iniciando Dashboard de Contratos IA")
    print("=" * 60)
    print(f"📊 Servidor: http://localhost:{port}")
    print(f"💬 Chatbot IA: Habilitado")
    print(f"🗄️  PostgreSQL: localhost:5432/airflow")
    print("=" * 60)
    print("Presiona Ctrl+C para detener el servidor")
    print("=" * 60)
    try:
        app.run(host='127.0.0.1', port=port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print("\n\nServidor detenido por el usuario")
    except Exception as e:
        print(f"\n\n❌ Error al iniciar servidor: {e}")
        import traceback
        traceback.print_exc()

