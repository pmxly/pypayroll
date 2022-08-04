#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from celery import Celery, platforms

app = Celery("tasks")
app.config_from_object('hhr.celeryconfig')
# 允许celery以root权限启动
platforms.C_FORCE_ROOT = True
print('^^^^^^^^^^^^^^^^^^^^celery app created^^^^^^^^^^^^^^^^^^^^^^^^')
