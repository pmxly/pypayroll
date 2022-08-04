#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ..celeryapp import app


@app.task(name='keep_conn_alive', ignore_result=True)
def keep_conn_alive(message):
    # app.config_from_object('hhr.celeryconfig', force=True)
    return message
