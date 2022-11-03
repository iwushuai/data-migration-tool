#!/usr/bin/python3
# encoding: utf-8
'''
Descripttion: 
Author: wushuai
version: 1.0.0
Date: 2022-08-03 10:19:25
LastEditors: wushuai
LastEditTime: 2022-08-10 10:39:37
'''
from pyDes import *
import binascii

# 秘钥
KEY = 'PASSWORD'

def des_encrypt(s):
    """
    DES 加密
    :param s: 原始字符串
    :return: 加密后字符串，16进制
    """
    secret_key = KEY
    iv = secret_key
    k = des(secret_key, ECB, iv, pad=None, padmode=PAD_PKCS5)
    en = k.encrypt(s, padmode=PAD_PKCS5)
    en = binascii.b2a_hex(en)
    en = en.decode()
    return en

def des_descrypt(s):
    """
    DES 解密
    :param s: 加密后的字符串，16进制
    :return:  解密后的字符串
    """
    secret_key = KEY
    iv = secret_key
    k = des(secret_key, ECB, iv, pad=None, padmode=PAD_PKCS5)
    de = k.decrypt(binascii.a2b_hex(s), padmode=PAD_PKCS5)
    de = de.decode()
    return de

if __name__ == "__main__":
    data = 'elastic:1qaz!qaz'
    encry = des_encrypt(data)
    print("密文：{}".format(encry))

    decry = des_descrypt('ce982fcdac87e22a0333595e261ee867')
    print("明文：{}".format(decry))