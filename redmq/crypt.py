import os
import json
from io import BytesIO
from base64 import b64encode, b64decode
from cryptography.hazmat.primitives.hmac import HMAC
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC


def hmac_sha256(key: bytes, data: bytes) -> bytes:
    '''
    校验码
    '''

    h = HMAC(key, SHA256())
    h.update(data)
    return h.finalize()


def encrypt_aes256(key: bytes, iv: bytes, data: bytes) -> bytes:
    '''
    加密
    '''

    cipher = Cipher(AES(key), CBC(iv))
    encryptor = cipher.encryptor()
    padder = PKCS7(256).padder()
    raw = padder.update(data) + padder.finalize()
    return encryptor.update(raw) + encryptor.finalize()


def decrypt_aes256(key: bytes, iv: bytes, data: bytes) -> bytes:
    '''
    解密
    '''

    cipher = Cipher(AES(key), CBC(iv))
    decryptor = cipher.decryptor()
    unpadder = PKCS7(256).unpadder()
    raw = decryptor.update(data) + decryptor.finalize()
    return unpadder.update(raw) + unpadder.finalize()


def encrypt256(key: bytes, data) -> str:
    '''
    加密
    '''

    iv = os.urandom(16)
    text = json.dumps(data, ensure_ascii=False)
    raw = bytes(text, encoding='utf8')
    hmc = hmac_sha256(key, raw)
    ed = encrypt_aes256(key, iv, raw)

    with BytesIO() as writer:
        writer.write(iv)
        writer.write(hmc)
        writer.write(ed)
        r = writer.getvalue()
        return b64encode(r).decode('utf8')


def decrypt256(key: bytes, data: str):
    '''
    解密
    '''

    raw = b64decode(data)

    with BytesIO(raw) as reader:
        iv = reader.read(16)
        hmc = reader.read(32)
        rd = decrypt_aes256(key, iv, reader.read())
        nhmc = hmac_sha256(key, rd)
        if hmc != nhmc:
            raise Exception(f'hmac error {nhmc} {hmc}')
        return json.loads(rd)


if '__main__' == __name__:
    key = b'12345678901234561234567890123456'
    ed = encrypt256(key, {'a': 123, 'b': 567})
    print(ed)
    dd = decrypt256(key, ed)
    print(dd)
