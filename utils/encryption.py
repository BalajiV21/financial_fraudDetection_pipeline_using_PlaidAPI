from cryptography.fernet import Fernet
import os

FERNET_KEY = os.environ.get("FERNET_KEY") or Fernet.generate_key()
cipher = Fernet(FERNET_KEY)

def encrypt_data(value):
    if not value:
        return None
    return cipher.encrypt(value.encode()).decode()

def decrypt_data(value):
    if not value:
        return None
    return cipher.decrypt(value.encode()).decode()
