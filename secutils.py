# -*- coding: utf-8 -*-

import base64
from Crypto.Cipher import AES
import datetime

AES_SK = 'P#&^dk@9JPTO%Uki'
IV = "1234567890123456"

# padding算法
BS = len(AES_SK)
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
unpad = lambda s: s[0:-ord(s[-1:])]


class AesEncrypt(object):
    def __init__(self):
        self.key = AES_SK
        self.mode = AES.MODE_CBC

    # 加密函数
    def encrypt(self, text):
        cryptor = AES.new(self.key.encode("utf8"), self.mode, IV.encode("utf8"))
        self.ciphertext = cryptor.encrypt(bytes(pad(text), encoding="utf8"))
        # AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题，使用base64编码
        return base64.b64encode(self.ciphertext)

    # 解密函数
    def decrypt(self, text):
        decode = base64.b64decode(text)
        cryptor = AES.new(self.key.encode("utf8"), self.mode, IV.encode("utf8"))
        plain_text = cryptor.decrypt(decode)
        return unpad(plain_text).decode()

    # 验证权限
    def validate_permission(self, encrypt_str):
        try:
            date_str = self.decrypt(encrypt_str)
            if len(date_str) == 19:
                cur_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if cur_date <= date_str:
                    return True
                else:
                    return False
        except Exception:
            return False


if __name__ == '__main__':
    enc = AesEncrypt()
    result = enc.validate_permission('hYazxd9nJ+BiMoVGssNyvpriXUPV+pl9YoTVRY9fKUg=')
    print(result)
