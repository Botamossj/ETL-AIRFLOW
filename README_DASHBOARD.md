# 🤖 Dashboard de Contratos IA

Interfaz web futurista para visualizar los datos extraídos por el DAG `extract_contratista_etl` y un chatbot inteligente que responde preguntas sobre contratos.

## 🚀 Características

- **Visualización de Datos**: Muestra todos los contratos con datos extraídos (razón social, representante, RUC, teléfono, email, domicilio)
- **Estadísticas en Tiempo Real**: Cards con métricas de contratos procesados
- **Chatbot IA**: Asistente virtual que responde preguntas sobre contratos usando Gemini
- **Diseño Futurista**: Interfaz moderna con efectos visuales estilo IA
- **Auto-refresh**: Actualización automática cada 30 segundos

## 📋 Requisitos

- Python 3.8+
- PostgreSQL (la misma base de datos que usa Airflow)
- API Key de Google Gemini (la misma que usa el DAG)

## 🔧 Instalación

1. Instala las dependencias:

```bash
pip install -r requirements_dashboard.txt
```

2. Configura las variables de entorno en tu archivo `.env`:

```env
# PostgreSQL - Base de datos oppdesarrollo
# El dashboard intentará obtener la configuración de Airflow primero
# Si Airflow no está disponible, usa estas variables:
OPPDESARROLLO_DB=oppdesarrollo
OPPDESARROLLO_USER=tu_usuario
OPPDESARROLLO_PASSWORD=tu_contraseña
POSTGRES_HOST=localhost  # o 'postgres' si ejecutas dentro de Docker
POSTGRES_PORT=5432

# Conexión de Airflow (opcional, si quieres usar la misma conexión)
POSTGRES_CONN_ID=oppdesarrollo_postgres

# Gemini API
LLM_API_KEY=tu_api_key_aqui
LLM_MODEL=gemini-2.5-pro

# Flask (opcional)
FLASK_PORT=5000
```

**Nota:** El dashboard intentará obtener la configuración de la conexión de Airflow `oppdesarrollo_postgres` automáticamente. Si Airflow no está disponible, usará las variables de entorno `OPPDESARROLLO_*`.

## 🎯 Ejecución

### Opción 1: Ejecutar directamente

```bash
python run_dashboard.py
```

### Opción 2: Ejecutar con Flask directamente

```bash
python app.py
```

### Opción 3: Ejecutar dentro de Docker (si tu PostgreSQL está en Docker)

1. Asegúrate de que tu contenedor de PostgreSQL esté corriendo
2. Ejecuta el dashboard con:

```bash
DOCKER_ENV=true python run_dashboard.py
```

O modifica `POSTGRES_HOST=postgres` en tu `.env` si PostgreSQL está en Docker.

## 🌐 Acceso

Una vez iniciado, abre tu navegador en:

```
http://localhost:5000
```

## 💬 Uso del Chatbot

1. **Preguntas Generales**: Puedes hacer preguntas generales sobre contratos públicos
2. **Preguntas Específicas**: Selecciona un contrato del dropdown y haz preguntas sobre ese contrato específico
3. **Click en Cards**: Haz click en cualquier card de contrato para seleccionarlo automáticamente en el chatbot

### Ejemplos de Preguntas:

- "¿Cuántos contratos hay con RUC?"
- "¿Qué información tienes sobre el contrato [código]?"
- "¿Cuál es la razón social del contrato seleccionado?"
- "Explícame qué es un RUC"
- "¿Qué datos faltan en los contratos?"

## 🎨 Características del Diseño

- **Tema Oscuro Futurista**: Colores neón (cyan y púrpura)
- **Animaciones Suaves**: Efectos de hover, glow y transiciones
- **Responsive**: Se adapta a diferentes tamaños de pantalla
- **Chatbot Flotante**: Siempre accesible en la esquina inferior derecha

## 🔍 Estructura de Archivos

```
.
├── app.py                    # Backend Flask con API
├── contratos_dashboard.html  # Frontend HTML/CSS/JS
├── run_dashboard.py         # Script de inicio
├── requirements_dashboard.txt # Dependencias Python
└── README_DASHBOARD.md      # Este archivo
```

## 🐛 Solución de Problemas

### Error de conexión a PostgreSQL

1. **Prueba la conexión manualmente:**
   ```bash
   python test_oppdesarrollo_connection.py
   ```

2. **Verifica que PostgreSQL esté corriendo:**
   - Si está en Docker: `docker ps | findstr postgres`
   - Si está local: Verifica el servicio de PostgreSQL

3. **Verifica las credenciales:**
   - El dashboard intenta obtener la configuración de Airflow primero
   - Si Airflow no está disponible, usa las variables `OPPDESARROLLO_*` en `.env`
   - Verifica que la base de datos `oppdesarrollo` exista

4. **Si PostgreSQL está en Docker:**
   - Asegúrate de que el puerto 5432 esté expuesto en `docker-compose.yml`
   - Usa `POSTGRES_HOST=localhost` si ejecutas el dashboard fuera de Docker

### El chatbot no responde

- Verifica que `LLM_API_KEY` esté configurada en `.env`
- Verifica que el modelo `LLM_MODEL` esté disponible
- Revisa la consola del servidor para errores

### No se muestran contratos

- Verifica que el DAG haya ejecutado y actualizado datos
- Verifica que la tabla `public.sync_contratos` exista
- Revisa los logs del servidor Flask

## 📝 Notas

- El dashboard se conecta directamente a la misma base de datos que usa Airflow
- El chatbot usa la misma API de Gemini que el DAG
- Los datos se actualizan automáticamente cada 30 segundos
- El chatbot puede minimizarse haciendo click en el header

