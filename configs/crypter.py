"""
Secret Encryption Utility 
4/21/25

This script provides a simple utility for encrypting and storing secrets (like API tokens or passwords)
into a JSON config file, and retrieving them later.
--------------------------------------------------------------
Example Usage:                                                
import configs.crypter as crypter                                               
# Encrypt and store a secret                                  
crypter.encrypt_to_config("my_very_secret_token", "my_api")   
# Later, decrypt it                                               
token = crypter.decrypt_from_config("my_api")                                               
print(token)                                               
# This will save the following in 'configs/config.json':                                                
# {                                                
#     "my_api_key": "...",                                                
#     "my_api_token": "..."                                               
# }                                                
--------------------------------------------------------------
Function Overview:
- encrypt(secret_message): Encrypts a message and returns a (key, token).
- encrypt_to_config(secret_string, name, file_path): Stores encrypted values in a config file.
- decrypt(key, token): Decrypts a previously encrypted token using its key.
- decrypt_from_config(name, file_path): Reads key/token from config and decrypts the secret.

Logging is included via `setup_logger` to provide feedback on success/failure.

Dependencies:
- cryptography (pip install cryptography)
- configs.setup_logger 
"""
from cryptography.fernet import Fernet
import os
import json
from pathlib import Path
configs = json.loads(Path("configs/config.json").read_text())
from typing import Union
from configs.setup_logger import setup_logger
import logging
logger = setup_logger(__name__, level=logging.DEBUG)

def encrypt(secret_message: str):
    """Returns encrypted secret message"""
    #create a secret
    secret_stoken = secret_message.encode()
    #build a key
    key = Fernet.generate_key()
    stoken_key=Fernet(key)
    #encrypt key as token
    return key, stoken_key.encrypt(secret_stoken)

def decrypt(key, stoken: str|bytes):
    """
    Decrypts a token using the provided key.

    Args:
        key (str or bytes): The encryption key.
        stoken (str or bytes): The encrypted token to decrypt.

    Returns:
        str: The original decrypted secret string.
    """
    #convert stoken to byte string if not
    if not isinstance(stoken, bytes):
        stoken = stoken.encode()
    #decrypt token back to secret
    decrypted_secret = Fernet(key).decrypt(stoken).decode("utf-8")
    return decrypted_secret

def encrypt_to_config(secret_string:str, name:str, file_path:str = "configs/config.json"):
    """
    Encrypts a secret string and stores the resulting key and token in a JSON config file.

    Args:
        secret_string (str): The string to encrypt.
        name (str): Base name used to generate key and token field names.
                    (e.g., 'my_api' will produce 'my_api_key' and 'my_api_token')
        file_path (str, optional): Path to the config file. Defaults to 'configs/config.json'.
    """
    #create variable names
    key_name = f"{name}_key"
    stoken_name = f"{name}_stoken"
    #encrypt 
    key, stoken = encrypt(secret_string)
    data = {
        key_name:key.decode('UTF-8'), 
        stoken_name:stoken.decode('UTF-8') #convert from byte string
    }

    # load existing config if file exists, otherwise start fresh
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                config = json.load(f)
            except json.JSONDecodeError:
                config = {}
    else:
        config = {}
    # Update config with new data
    config.update(data)
    # Write back to file
    try:
        with open(file_path, 'w') as f:
            json.dump(config, f, indent=4)
            log.info(f"SUCCESS: {name} secret stored as {key_name} {stoken_name} in {file_path}")
    except Exception as e:
        log.error(f"ERROR writing stoken & key to {file_path}: {e}")

def decrypt_from_config(name:str, file_path="configs/config.json"):
    """
    Loads an encrypted key and token from a config file and returns the decrypted secret.
    Args:
        name (str): Base name used to retrieve the key and token (e.g., 'my_api' will look for 'my_api_key' and 'my_api_token').
        file_path (str, optional): Path to the config JSON file. Defaults to 'configs/config.json'.
    Returns:
        str: Decrypted, decoded secret string.
    """
    #format the names
    key_name = f"{name}_key"
    stoken_name = f"{name}_stoken"
    #check the file path
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found at {file_path}")
    #read file
    with open(file_path, 'r') as f:
        config = json.load(f)
    #get variables
    key = config.get(key_name)
    stoken = config.get(stoken_name)
    #if missing
    if key is None or stoken is None:
        raise KeyError(f"Missing '{key_name}' {key} or '{stoken_name}' {stoken} in config file.")
    return decrypt(key.encode(), stoken)