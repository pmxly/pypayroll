#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# from os import path
# import configparser
# from requests.auth import HTTPDigestAuth
# from flask_httpauth import HTTPDigestAuth as f_HTTPDigestAuth
from requests.auth import HTTPBasicAuth
from flask_httpauth import HTTPBasicAuth as f_HTTPBasicAuth

# conf = configparser.ConfigParser()
# conf.read(path.join(path.dirname(path.abspath(__file__)), 'conf.ini'))
# fsrv_pl = conf.get('config', 'fsrv_platform')


def get_auth_method():
    auth = f_HTTPBasicAuth()
    return auth


def get_auth_inst():
    auth_inst = HTTPBasicAuth(list(h.keys())[0], list(h.values())[0])
    return auth_inst


# def get_auth_method():
#     if fsrv_pl == "Windows":
#         auth = f_HTTPBasicAuth()
#     else:
#         auth = f_HTTPDigestAuth()
#     return auth
#
#
# def get_auth_inst():
#     if fsrv_pl == "Windows":
#         auth_inst = HTTPBasicAuth(list(h.keys())[0], list(h.values())[0])
#     else:
#         auth_inst = HTTPDigestAuth(list(h.keys())[0], list(h.values())[0])
#     return auth_inst
h = {"hhr": "hand_hhr"}
