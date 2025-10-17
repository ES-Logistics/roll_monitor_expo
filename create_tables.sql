-- Tabela para armazenar o snapshot atual dos dados monitorados
-- Serve como backup e base para comparações
CREATE TABLE IF NOT EXISTS bronze.roll_monitor_snapshot (
    unique_id VARCHAR(500) PRIMARY KEY,
    proceso VARCHAR(50) NOT NULL,
    porto_embarque VARCHAR(100),
    navio_embarque VARCHAR(100),
    previsao_embarque TIMESTAMP,
    previsao_embarque_feeder TIMESTAMP,
    navio_feeder VARCHAR(100),
    porto_feeder VARCHAR(100),
    previsao_embarque_transbordo TIMESTAMP,
    porto_transbordo VARCHAR(100),
    navio_transbordo VARCHAR(100),
    email_responsavel VARCHAR(150),
    armador VARCHAR(200),
    cliente VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índice para melhorar performance nas consultas por processo
CREATE INDEX IF NOT EXISTS idx_snapshot_proceso ON bronze.roll_monitor_snapshot(proceso);

-- Tabela para registrar todas as alterações detectadas
-- Mantém histórico completo de mudanças por processo
CREATE TABLE IF NOT EXISTS bronze.roll_monitor_changes (
    id SERIAL PRIMARY KEY,
    proceso VARCHAR(50) NOT NULL,
    alteracoes JSONB NOT NULL,
    status_relatorio VARCHAR(20) DEFAULT 'PENDENTE' CHECK (status_relatorio IN ('PENDENTE', 'ENVIADO')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índice único para garantir um registro por processo
CREATE UNIQUE INDEX IF NOT EXISTS idx_changes_proceso_unique ON bronze.roll_monitor_changes(proceso);

-- Índice para consultas de relatório
CREATE INDEX IF NOT EXISTS idx_changes_status ON bronze.roll_monitor_changes(status_relatorio);

-- Comentários para documentar as tabelas
COMMENT ON TABLE bronze.roll_monitor_snapshot IS 'Snapshot atual dos dados monitorados - base para comparações';
COMMENT ON TABLE bronze.roll_monitor_changes IS 'Registro de alterações por processo - base para relatórios';

COMMENT ON COLUMN bronze.roll_monitor_changes.alteracoes IS 'JSON com histórico de todas as alterações do processo';
COMMENT ON COLUMN bronze.roll_monitor_changes.status_relatorio IS 'PENDENTE: alterações não enviadas no relatório, ENVIADO: já incluído em relatório';