# Gong Webhook System

Sistema automatizado para procesar transcripts de Gong que incluye:
- Listener para nuevos transcripts
- Extracción de datos de Snowflake
- Procesamiento con OpenAI Assistant
- Documentación automática en Google Drive
- Notificaciones por email

## Arquitectura

### Módulo 1 - Gong Webhook / Listener (`listener.py`)
- Escucha nuevos transcripts en Gong
- Polling cada 5 minutos (configurable)
- Evita duplicados
- Coloca eventos en cola interna

### Módulo 2 - Snowflake Data Fetcher (`fetcher.py`)
- Conecta a Snowflake
- Ejecuta query hard-codeada
- Extrae datos del cliente y transcript
- Reintento automático con backoff exponencial

### Módulo 3 - OpenAI Assistant Bridge (`assistant_bridge.py`)
- Envía transcript al Assistant de OpenAI
- Obtiene resumen automatizado
- Manejo de timeouts y reintentos

### Módulo 4 - Google Drive Documenter (`drive_documenter.py`)
- Crea carpetas por cliente
- Mantiene "Call Summary" (último resumen)
- Mantiene "Summary Log" (historial completo)

### Módulo 5 - Email Dispatcher (`email_dispatcher.py`)
- Envía emails templados a CS y AM
- Incluye links a documentos de Drive
- Templates HTML personalizados

### Módulo 6 - Orchestrator (`orchestrator.py`)
- Coordina todo el pipeline
- Manejo de errores y reintentos
- Métricas de Prometheus
- Logging estructurado
- Graceful shutdown

## Configuración

### Variables de Entorno (.env)
```bash
# Snowflake
SNOWFLAKE_USER=your_username
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_AUTHENTICATOR=externalbrowser

# OpenAI
OPENAI_API_KEY=your_openai_api_key
ASSISTANT_ID=your_assistant_id

# Google Drive
GOOGLE_CREDS_JSON=path/to/google/credentials.json

# Email
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
FROM_EMAIL=your_email@gmail.com

# Gong
GONG_API_URL=https://api.gong.io
GONG_API_KEY=your_gong_api_key

# Configuración general
POLL_INTERVAL=300
MAX_RETRIES=3
TIMEOUT=120
METRICS_PORT=8000
```

## Instalación

1. **Instalar dependencias:**
```bash
pip install -r requirements.txt
```

2. **Configurar credenciales:**
- Copiar `.env` y completar con valores reales
- Obtener credenciales de Google Service Account
- Configurar OpenAI Assistant

3. **Crear carpetas de logs:**
```bash
mkdir -p logs
```

## Ejecución

### Desarrollo
```bash
python main.py
```

### Producción
```bash
# Con logging
python main.py > logs/application.log 2>&1 &

# O usar systemd, Docker, etc.
```

## Monitoreo

### Métricas (Prometheus)
- `http://localhost:8000/metrics`
- Contadores de éxito/error
- Tiempo de procesamiento
- Cola de eventos

### Logs
- `logs/event_listener.log` - Eventos del listener
- `logs/email_dispatcher.log` - Envío de emails
- `logs/orchestrator.log` - Logs estructurados JSON

## Estructura de Datos

### TranscriptData
```python
@dataclass
class TranscriptData:
    transcript_id: str
    account_name: str
    account_number: str
    speaker_name: str
    speaker_email: str
    cs_email: str
    am_email: str
    transcript_text: str
```

## Query de Snowflake

```sql
SELECT 
    t.transcript_id,
    a.account_name,
    a.account_number,
    c.speaker_name,
    c.speaker_email,
    a.cs_email,
    a.am_email,
    t.transcript_text
FROM transcripts t
JOIN calls c ON t.call_id = c.call_id
JOIN accounts a ON c.account_id = a.account_id
WHERE t.transcript_id = %s
```

## Criterios de Aceptación

- ✅ Latencia < 5 min entre alta en Gong y procesamiento
- ✅ Tasa de éxito ≥ 95%
- ✅ Eventos duplicados ignorados
- ✅ Logs persistentes y estructurados
- ✅ Reintento automático con backoff exponencial
- ✅ Graceful shutdown
- ✅ Métricas de monitoreo

## Troubleshooting

### Problemas Comunes

1. **Error de conexión a Snowflake:**
   - Verificar credenciales en `.env`
   - Verificar conectividad de red
   - Revisar logs de `fetcher.py`

2. **OpenAI timeout:**
   - Aumentar `TIMEOUT` en `.env`
   - Verificar API key
   - Revisar logs de `assistant_bridge.py`

3. **Error de Google Drive:**
   - Verificar credenciales JSON
   - Verificar permisos de la Service Account
   - Revisar logs de `drive_documenter.py`

4. **Emails no enviados:**
   - Verificar configuración SMTP
   - Verificar templates HTML
   - Revisar logs de `email_dispatcher.py`

## Desarrollo

### Agregar nueva funcionalidad
1. Crear nuevo módulo en `src/`
2. Agregarlo al pipeline en `orchestrator.py`
3. Actualizar tests y documentación

### Tests
```bash
# Agregar tests unitarios
pytest tests/
```

## Producción

### Docker
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]
```

### Systemd Service
```ini
[Unit]
Description=Gong Webhook System
After=network.target

[Service]
Type=simple
User=gong-webhook
WorkingDirectory=/opt/gong-webhook
ExecStart=/opt/gong-webhook/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```