# -*- coding: utf-8 -*-


import configparser
from . import applatform
from .applatform import get_sep


conf = configparser.RawConfigParser()
conf.read(applatform.get_prj_root() + get_sep() + 'conf.ini', encoding='utf8')
