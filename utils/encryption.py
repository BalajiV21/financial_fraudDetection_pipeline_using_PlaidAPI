from cryptography.fernet import Fernet
import os
from dotenv import load_dotenv

load_dotenv()
FERNET_KEY = os.getenv("FERNET_KEY") or Fernet.generate_key()
cipher = Fernet(FERNET_KEY)


def encrypt_data(value):
    if not value:
        return None
    return cipher.encrypt(value.encode()).decode()

def decrypt_data(value):
    if not value:
        return None
    return cipher.decrypt(value.encode()).decode()
