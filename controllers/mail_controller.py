import requests
import time
import jwt
import os
import sys
import base64

from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives import hashes


# Add the parent directory to sys.path to make the module imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Configuration
APP_ID = "9b2aa534-d383-4851-a9dc-feb67f60d58b"
TENANT_DOMAIN = "eslogistics.com.br"
THUMBPRINT = "7E4F0A9CF4E4D5C1ECE0D40063A9D14B98C67A2F"


class MailController:
    def __init__(self):
        self.app_id = APP_ID
        self.tenant_domain = TENANT_DOMAIN
        self.thumbprint = THUMBPRINT
        self.cert_path = self._get_cert_path("MailboxRobot.pem")
        self.private_key_path = self._get_cert_path("MailboxRobot.key")
    
    def _get_cert_path(self, filename):
        """Find certificate file in various locations, prioritizing the assets directory"""
        possible_paths = [
            # Check assets directory relative to the current script
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", filename),
            # Check assets directly in the executable directory (for compiled version)
            os.path.join(os.path.abspath("."), "assets", filename),
            # Environment variable override
            os.environ.get(f"BOOKING_READER_{filename.upper()}_PATH", ""),
        ]
        
        # Return the first path that exists
        for path in possible_paths:
            if path and os.path.exists(path):
                return path
                
        # Default to the assets path for reporting, even if it doesn't exist
        return possible_paths[0]
    
    def get_token_with_certificate(self):
        """Authenticate with Microsoft Graph using a certificate"""
        try:
            print(f"🔍 Tentando carregar certificado: {self.cert_path}")
            
            # Verificar se arquivo existe
            if not os.path.exists(self.cert_path):
                raise FileNotFoundError(f"Certificado não encontrado: {self.cert_path}")
            
            # Load certificate and private key from PEM file
            with open(self.cert_path, 'rb') as cert_file:
                pem_data = cert_file.read().decode('utf-8')
            
            print("📋 Conteúdo do certificado carregado")
            
            # Extract private key
            private_key = None
            if "-----BEGIN PRIVATE KEY-----" in pem_data:
                private_key_start = pem_data.index("-----BEGIN PRIVATE KEY-----")
                private_key_end = pem_data.index("-----END PRIVATE KEY-----") + len("-----END PRIVATE KEY-----")
                private_key_pem = pem_data[private_key_start:private_key_end]
                
                private_key = load_pem_private_key(
                    private_key_pem.encode('utf-8'),
                    password=None,
                    backend=default_backend()
                )
                print("🔑 Chave privada carregada com sucesso")
            
            # Extract certificate
            cert = None
            if "-----BEGIN CERTIFICATE-----" in pem_data:
                cert_start = pem_data.index("-----BEGIN CERTIFICATE-----")
                cert_end = pem_data.index("-----END CERTIFICATE-----") + len("-----END CERTIFICATE-----")
                cert_pem = pem_data[cert_start:cert_end]
                
                cert = load_pem_x509_certificate(cert_pem.encode('utf-8'), default_backend())
                print("📜 Certificado X509 carregado com sucesso")
            
            if not private_key or not cert:
                raise ValueError("Não foi possível extrair chave privada ou certificado do arquivo PEM")
            
            # Calculate thumbprint from certificate
            cert_thumbprint = cert.fingerprint(hashes.SHA1()).hex().upper()
            print(f"🔢 Thumbprint calculado: {cert_thumbprint}")
            print(f"🔢 Thumbprint esperado: {self.thumbprint}")
            
            # Create JWT token
            now = int(time.time())
            exp = now + 3600  # Token valid for 1 hour
            
            # Create the payload
            payload = {
                'aud': f'https://login.microsoftonline.com/{self.tenant_domain}/oauth2/v2.0/token',
                'exp': exp,
                'iss': self.app_id,
                'jti': str(int(time.time())),
                'nbf': now,
                'sub': self.app_id
            }
            
            print("📦 Payload JWT criado")
            
            # Create the JWT token using the calculated thumbprint
            jwt_token = jwt.encode(
                payload,
                private_key,
                algorithm='RS256',
                headers={
                    'x5t': base64.b64encode(bytes.fromhex(cert_thumbprint)).decode('utf-8')
                }
            )
            
            print("🎫 Token JWT criado")
            
            # Get access token
            token_url = f'https://login.microsoftonline.com/{self.tenant_domain}/oauth2/v2.0/token'
            token_data = {
                'grant_type': 'client_credentials',
                'client_id': self.app_id,
                'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
                'client_assertion': jwt_token,
                'scope': 'https://graph.microsoft.com/.default'
            }
            
            print(f"🌐 Fazendo requisição para: {token_url}")
            token_response = requests.post(token_url, data=token_data)
            
            print(f"📡 Resposta da autenticação: {token_response.status_code}")
            
            if token_response.status_code != 200:
                print(f"❌ Erro na resposta: {token_response.text}")
                token_response.raise_for_status()
            
            access_token = token_response.json().get('access_token')
            print("🔑 Token de autenticação obtido com sucesso")
            return access_token
        
        except Exception as e:
            print(f"❌ Erro detalhado ao obter token: {str(e)}")
            print(f"❌ Tipo do erro: {type(e).__name__}")
            return None
    
    def send_email_with_attachment(self, to_email, from_email, subject, body, attachment_path, attachment_name):
        """
        Envia e-mail com anexo usando Microsoft Graph API
        
        Args:
            to_email (str): E-mail do destinatário
            from_email (str): E-mail do remetente  
            subject (str): Assunto do e-mail
            body (str): Corpo do e-mail em HTML
            attachment_path (str): Caminho do arquivo anexo
            attachment_name (str): Nome do anexo
            
        Returns:
            bool: True se enviado com sucesso
        """
        try:
            # Obter token de autenticação
            access_token = self.get_token_with_certificate()
            if not access_token:
                return False
            
            # Preparar anexo
            with open(attachment_path, 'rb') as f:
                file_content = f.read()
                file_size_mb = len(file_content) / (1024 * 1024)
                
                print(f"📁 Tamanho do arquivo: {file_size_mb:.2f} MB")
                
                # Microsoft Graph tem limite de 3MB para anexos inline
                if file_size_mb > 3:
                    print(f"⚠️ Arquivo muito grande ({file_size_mb:.2f} MB). Limite: 3MB")
                    print("💡 Considere usar upload session para arquivos grandes")
                    return False
                
                attachment_content = base64.b64encode(file_content).decode('utf-8')
            
            # Preparar e-mail seguindo especificação Microsoft Graph
            email_data = {
                "message": {
                    "subject": subject,
                    "body": {
                        "contentType": "HTML",
                        "content": body
                    },
                    "toRecipients": [
                        {
                            "emailAddress": {
                                "address": to_email
                            }
                        }
                    ],
                    "attachments": [
                        {
                            "@odata.type": "#microsoft.graph.fileAttachment",
                            "name": attachment_name,
                            "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            "contentBytes": attachment_content
                        }
                    ]
                },
                "saveToSentItems": "true"
            }
            
            # Debug: Mostrar tamanho do anexo
            print(f"📎 Tamanho do anexo: {len(attachment_content)} caracteres")
            print(f"📧 Destinatário: {to_email}")
            print(f"📧 Remetente: {from_email}")
            
            # Enviar e-mail
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            send_url = f"https://graph.microsoft.com/v1.0/users/{from_email}/sendMail"
            
            try:
                response = requests.post(send_url, headers=headers, json=email_data, timeout=30)
            except Exception as e:
                print(f"❌ Erro na requisição: {e}")
                return False
            
            if response.status_code == 202:
                print(f"✅ E-mail enviado com sucesso para {to_email}")
                return True
            else:
                print(f"❌ Falha no envio: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao enviar e-mail: {e}")
            return False