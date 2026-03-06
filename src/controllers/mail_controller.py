import requests
import time
import base64
import logging
from cryptography.hazmat.primitives import serialization
from cryptography import x509
import jwt
import os
import re
import config
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EmailManager:
    """Gerenciador de emails do Microsoft Graph com autenticação por certificado"""

    def __init__(self, app_id=None, thumbprint=None, tenant_domain=None):
        try:
            # Carregar configurações do .env se não fornecidas
            dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env') 
            load_dotenv(dotenv_path)
            print(f"Path de execução geral: {os.getcwd()}")
            self.app_id = app_id or os.getenv("APP_ID")
            self.thumbprint = thumbprint or os.getenv("THUMBPRINT") 
            self.tenant_domain = tenant_domain or os.getenv("TENANT_DOMAIN")
            self.token = None
            self.private_key = None
            self.certificate = None
            self.pem_b64 = os.getenv("PEM_B64")
            self.default_mailbox = config.MAILBOX
            self.default_subject_filter = config.SUBJECT_FILTER
            self.email_limit = config.EMAIL_LIMIT
        
        except Exception as e:
            logger.error(f"[MAIL] Erro ao inicializar EmailManager: {e}")
            raise    

                
        # Validar se todas as configurações foram carregadas
        if not self.app_id:
            raise ValueError("APP_ID não encontrado nos parâmetros ou .env")
        if not self.thumbprint:
            raise ValueError("THUMBPRINT não encontrado nos parâmetros ou .env")
        if not self.tenant_domain:
            raise ValueError("TENANT_DOMAIN não encontrado nos parâmetros ou .env")
        if not self.pem_b64:
            raise ValueError("PEM_B64 não encontrado no .env")
        
        # Carregar certificados na inicialização
        self._load_certificates()
    
    def _load_certificates(self):
        """Carrega certificado e chave privada do .env"""
        try:
            # Decodifica o conteúdo base64
            pem_bytes = base64.b64decode(self.pem_b64)

            # Extrai blocos PEM individuais (BEGIN ... END)
            pattern = rb"-----BEGIN ([^-]+)-----(.*?)-----END \1-----"
            blocks = re.findall(pattern, pem_bytes, flags=re.DOTALL)

            if not blocks:
                raise ValueError("Nenhum bloco PEM encontrado no conteúdo decodificado")

            # Processar cada bloco
            for block_type, block_content in blocks:
                label = block_type.decode().strip()
                block = f"-----BEGIN {label}-----".encode() + block_content + f"-----END {label}-----".encode()

                if b"PRIVATE KEY" in block_type:
                    try:
                        self.private_key = serialization.load_pem_private_key(block, password=None)
                        logger.info(f"[MAIL] Chave privada ({label}) carregada")
                    except Exception as e:
                        logger.error(f"[MAIL]  Erro ao carregar chave privada: {e}")

                elif b"CERTIFICATE" in block_type:
                    try:
                        self.certificate = x509.load_pem_x509_certificate(block)
                        logger.info(f"[MAIL] Certificado ({label}) carregado")
                    except Exception as e:
                        logger.error(f"[MAIL]  Erro ao carregar certificado: {e}")

            # Validar se ambos foram carregados
            if not self.private_key:
                raise ValueError("Chave privada não encontrada no PEM_B64")
            
            if not self.certificate:
                raise ValueError("Certificado não encontrado no PEM_B64")
                
            logger.info("🔐 Certificados carregados com sucesso")
            
        except Exception as e:
            logger.error(f"❌ Erro ao carregar certificados: {e}")
            raise
    
    def get_token(self):
        """Gera token de autenticação com certificado"""
        try:
            logger.info("Gerando token JWT com certificado...")
            
            # Payload JWT simplificado
            now = int(time.time())
            payload = {
                'aud': f'https://login.microsoftonline.com/{self.tenant_domain}/oauth2/token',
                'iss': self.app_id,
                'sub': self.app_id,
                'nbf': now,
                'exp': now + 3600,  # 1 hora
                'jti': str(now)     # Usar timestamp como ID único
            }
            
            # Headers JWT com thumbprint
            headers = {
                'x5t': base64.b64encode(bytes.fromhex(self.thumbprint)).decode('utf-8')
            }
            
            # Gerar JWT
            jwt_token = jwt.encode(payload, self.private_key, algorithm='RS256', headers=headers)
            
            # Obter access token
            token_data = {
                'grant_type': 'client_credentials',
                'client_id': self.app_id,
                'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
                'client_assertion': jwt_token,
                'scope': 'https://graph.microsoft.com/.default'
            }
            
            response = requests.post(
                f'https://login.microsoftonline.com/{self.tenant_domain}/oauth2/v2.0/token',
                data=token_data
            )
            response.raise_for_status()
            
            self.token = response.json()['access_token']
            return self.token
        
        except Exception as e:
            logger.error(f"[MAIL] Erro ao obter token: {e}")
            raise
    
    def get_filtered_emails(self):
        """Busca emails não lidos com filtro no assunto e anexos"""
        # Usar valores padrão se não fornecidos
        mailbox = self.default_mailbox
        subject_filter = self.default_subject_filter
        
        if not mailbox:
            raise ValueError("Mailbox deve ser fornecido ou configurado como padrão")
        
        self.get_token()
            
        try:
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            url = f'https://graph.microsoft.com/v1.0/users/{mailbox}/messages?$top={self.email_limit}'
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            all_emails = response.json().get('value', [])
            
            # Filtrar emails não lidos com anexos e filtro no assunto
            filtered_emails = []
            for email in all_emails:
                if (not email.get('isRead', True) and 
                    email.get('hasAttachments', False) and 
                    self.default_subject_filter.lower() in email.get('subject', '').lower()):
                    filtered_emails.append(email)
            
            logger.info(f"[MAIL] Encontrados {len(filtered_emails)} emails que atendem aos critérios")
            return filtered_emails
        
        except Exception as e:
            logger.error(f"[MAIL] Erro ao buscar emails: {e}")
            return []
    
    def download_pdf_attachments(self, message_id):
        """Baixa anexos PDF de um email específico"""
        mailbox = self.default_mailbox
        
        if not mailbox:
            raise ValueError("Mailbox deve ser fornecido ou configurado como padrão")
        
        self.get_token()
            
        try:
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            # Buscar anexos
            attachments_url = f'https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}/attachments'
            response = requests.get(attachments_url, headers=headers)
            response.raise_for_status()
            
            attachments = response.json().get('value', [])
            
            # Filtrar e baixar apenas PDFs
            pdf_attachments = []
            for attachment in attachments:
                if attachment.get('name', '').lower().endswith('.pdf'):
                    try:
                        attachment_id = attachment.get('id')
                        content_url = f'https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}/attachments/{attachment_id}/$value'
                        
                        content_response = requests.get(content_url, headers=headers)
                        content_response.raise_for_status()
                        
                        pdf_attachments.append({
                            'name': attachment.get('name'),
                            'content': content_response.content,
                            'size': len(content_response.content)
                        })
                        
                        logger.info(f"[MAIL]  PDF baixado: {attachment.get('name')} ({len(content_response.content)} bytes)")
                        
                    except Exception as e:
                        logger.error(f"[MAIL]  Erro ao baixar anexo {attachment.get('name')}: {e}")
            
            return pdf_attachments
        
        except Exception as e:
            logger.error(f"[MAIL] Erro ao baixar anexos: {e}")
            return []
    
    def mark_as_read(self, message_id):
        """Marca email como lido"""
        # Usar valor padrão se não fornecido
        mailbox = self.default_mailbox
        
        if not mailbox:
            raise ValueError("Mailbox deve ser fornecido ou configurado como padrão")
        self.get_token()
            
        try:
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            url = f'https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}'
            response = requests.patch(url, headers=headers, json={'isRead': True})
            response.raise_for_status()
            
            logger.info(f"[MAIL] Email marcado como lido: {message_id}")
            return True
        
        except Exception as e:
            logger.error(f"[MAIL] Erro ao marcar email como lido: {e}")
            return False

    def send_mail(self, to_address, subject, html_content, attachments=None):
        """
        Envia um email direto para destinatário específico
        
        Args:
            to_address (str): Email do destinatário
            subject (str): Assunto do email
            html_content (str): Conteúdo HTML do email
            
        Returns:
            bool: True se enviado com sucesso, False caso contrário
        """
        # Usar valor padrão se não fornecido
        mailbox = self.default_mailbox
        
        if not mailbox:
            raise ValueError("Mailbox deve ser fornecido ou configurado como padrão")
        
        self.get_token()
            
        try:
            logger.info(f"[MAIL] Enviando email para: {to_address}")
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            # Preparar dados do email
            data = {
                'message': {
                    'subject': subject,
                    'body': {
                        'contentType': 'HTML',
                        'content': html_content
                    },
                    'toRecipients': [
                        {
                            'emailAddress': {
                                'address': to_address
                            }
                        }
                    ],
                    'attachments': attachments or []
                },
                'saveToSentItems': 'true'
            }
            
            # Enviar email
            send_url = f'https://graph.microsoft.com/v1.0/users/{mailbox}/sendMail'
            response = requests.post(send_url, headers=headers, json=data)
            response.raise_for_status()
            
            logger.info(f"[MAIL] Email enviado com sucesso para {to_address}")
            return True
        
        except Exception as e:
            logger.error(f"[MAIL] Erro ao enviar email: {str(e)}")
            if 'response' in locals() and hasattr(response, 'text'):
                logger.error(f"[MAIL] Conteúdo da resposta: {response.text}")
            return False
        