# Configurações do BL Reader
# ============================

# Configurações de Email
MAILBOX = "inputs_datalake@eslogistics.onmicrosoft.com"
EMAIL_LIMIT = 100
SUBJECT_FILTER = "none"
TARGET_MAILS= ["wellington.zunino@eslogistics.com.br","guilherme.decker@eslogistics.com.br"]

EMBEDDING_MODEL ="text-embedding-3-small"
LLM_MODEL = "gpt-5.1"

# Configurações de Monitoramento
MONITORING_INTERVAL_SECONDS = 300  # Intervalo entre verificações de email

# Configurações de Processamento
MAX_CONCURRENT_PDFS = 5
PROCESSING_TIMEOUT = 300  # segundos

COLS_COMPARE= ["navio_embarque","previsao_embarque","navio_transbordo","previsao_embarque_transbordo","viagem"]

RUNS= ["11:30"]
WINDOW_MINUTES = 15
