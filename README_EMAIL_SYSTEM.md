# 📊 Sistema de Relatórios Excel - Roll Monitor

## 🚀 Funcionalidades Implementadas

### ✅ Funcionalidades Principais
1. **📋 Verificar Status do Sistema** - Mostra status atual do sistema de relatórios
2. **📁 Listar Relatórios Gerados** - Lista todos os relatórios na pasta `generated_reports`
3. **🎯 Gerar Relatório Manual** - Permite gerar relatório sob demanda
4. **⏰ Relatórios Automáticos com Email** - Sistema principal que:
   - Gera relatórios automaticamente às **11h** e **16h**
   - **Envia por email automaticamente** após geração
   - **Limpa pasta automaticamente** após envio
5. **📧 Enviar Relatórios Existentes** - Envia relatórios já gerados por email
6. **🗑️ Limpar Pasta de Relatórios** - Remove todos os relatórios da pasta
7. **🛑 Parar Relatórios Automáticos** - Interrompe o sistema automático
8. **❌ Sair** - Encerra o sistema

### 🔧 Configurações de Email
```python
# Configuradas no main() do excel_reports_manager.py
target_mail = "wellington.zunino@eslogistics.com.br"  # Email de destino
output_mail = "noreply@eslogistics.com.br"           # Email de origem
```

### 📧 Integração com Email
- **Mail Controller**: Utiliza `controllers/mail_controller.py` 
- **Envio Automático**: Callback integrado ao scheduler
- **Limpeza Automática**: Remove relatórios após envio bem-sucedido
- **Formato HTML**: Emails com formatação profissional

### 🆕 Novos Campos Integrados
- **Cliente** (VARCHAR 200) - Coluna C no Excel
- **Armador** (VARCHAR 200) - Coluna D no Excel  
- **Email Responsável** (VARCHAR 150) - Coluna N no Excel

> **Nota**: Estes campos **NÃO** geram eventos de mudança no monitoramento.

### ⚡ Inicialização Automática
- O sistema **inicia automaticamente** na **Opção 4** (Relatórios Automáticos com Email)
- Ideal para execução em produção com mínima intervenção

## 🏃‍♂️ Como Executar

```bash
cd C:\Users\WellingtonZunino\Documents\roll_monitor
python excel_reports_manager.py
```

O sistema iniciará automaticamente configurado para:
1. ✅ Gerar relatórios às 11h e 16h
2. ✅ Enviar por email automaticamente
3. ✅ Limpar pasta após envio
4. ✅ Monitorar continuamente

## 📁 Estrutura do Sistema

```
roll_monitor/
├── excel_reports_manager.py     # 🎯 Script principal (INICIA AQUI)
├── controllers/
│   └── mail_controller.py       # 📧 Controlador de email
├── reports/
│   ├── excel_report_controller.py  # 🎛️ Controlador de relatórios
│   ├── scheduler_service.py        # ⏰ Serviço de agendamento
│   └── excel_formatter.py          # 📊 Formatação do Excel
├── repositories/
│   └── snapshot_repository.py      # 💾 Persistência de dados
└── generated_reports/              # 📁 Pasta de relatórios (limpa automaticamente)
```

## 🔒 Certificados de Email
- Requer certificado `MailboxRobot.pem` na pasta `assets/`
- Configuração Microsoft Graph API para envio de emails corporativos

---
*Sistema desenvolvido para ES Logistics - Roll Monitor*