# -*- coding: utf-8 -*-


import functools
from flask import jsonify, request
import binascii
from pyDes import des, CBC, PAD_PKCS5
from .dbengine import DataBaseAlchemy
from sqlalchemy import select
from .secutils import AesEncrypt


def permission_check(permission):
    """
    Check user permission for controller's url resource
    :param permission:
    :return: Permission decorator objectpermission denied
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # get secret from user request
            request_data = request.get_json(force=True)
            secret_token = request_data.get("secret_token")
            if secret_token is None or secret_token == '':
                return jsonify({'status': 'ERROR', 'message': 'no secret key provided'})
            enc = AesEncrypt()
            result = enc.validate_permission(secret_token)
            if result is True:
                return f(*args, **kwargs)
            else:
                return jsonify({'status': 'ERROR', 'message': 'permission denied'})
            # try:
            #     enc = AesEncrypt()
            #     result = enc.validate_permission(secret_token)
            #     if result is True:
            #         return f(*args, **kwargs)
            #     else:
            #         return jsonify({'status': 'ERROR', 'message': 'permission denied'})
            # except Exception:
            #     return jsonify({'status': 'ERROR', 'message': 'exception occurred'})
        return wrapper
    return decorator


def des_encrypt(s):
    """
    DES Encryption
    :param s: Source string
    :return: Encryted string(hexadecimal)
    """
    secret_token = 'hand_hhr'
    iv = secret_token
    k = des(secret_token, CBC, iv, pad=None, padmode=PAD_PKCS5)
    en = k.encrypt(s, padmode=PAD_PKCS5)
    return binascii.b2a_hex(en)


def des_descrypt(s):
    """
    DES Decryption
    :param s: Encrypted string(hexadecimal)
    :return:  Decrypted string
    """
    secret_token = 'hand_hhr'
    iv = secret_token
    k = des(secret_token, CBC, iv, pad=None, padmode=PAD_PKCS5)
    de = k.decrypt(binascii.a2b_hex(s), padmode=PAD_PKCS5)
    return de
