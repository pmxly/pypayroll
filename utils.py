#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
from random import choice
from string import digits, ascii_letters
from . import applatform
from .dbengine import HubDataBaseAlchemy
# from sqlalchemy import select, func
from datetime import datetime, date
from time import time
import calendar
import pytz
from .confreader import conf
from uuid import uuid1
from requests import post
from requests_toolbelt.multipart.encoder import MultipartEncoder
from requests.auth import HTTPBasicAuth
from redis.sentinel import Sentinel
import redis
from .ha import redis_ha_switch_on, redis_sentinels, redis_master_name, redis_socket_timeout, redis_password, redis_db

if redis_ha_switch_on:
    sentinel = Sentinel(redis_sentinels, socket_timeout=redis_socket_timeout)
    redis_master = sentinel.master_for(redis_master_name, socket_timeout=redis_socket_timeout, password=redis_password,
                                       db=redis_db)
else:
    redis_master = redis.Redis.from_url(conf.get('config', "backend_url"))


def get_current_dttm():
    return datetime.fromtimestamp(time(), pytz.timezone(conf.get('config', 'timezone')))


def get_current_date():
    return get_current_dttm().date()


def add_months(d, x):
    new_day = d.day
    new_month = (((d.month - 1) + x) % 12) + 1
    new_year = d.year + (((d.month - 1) + x) // 12)
    if new_day > calendar.mdays[new_month]:
        new_day = calendar.mdays[new_month]
        if new_year % 4 == 0 and new_month == 2:
            new_day += 1
    return date(new_year, new_month, new_day)


def pub_retry(fun):
    max_retries = 3

    def wrapper(cls, files_dict, tenant_id, task_name, task_id):
        for retries in range(max_retries + 1):
            try:
                return fun(cls, files_dict, task_name, task_id)
            except Exception as e:
                if retries + 1 > max_retries:
                    raise e
                else:
                    TaskFileUtil.track_task_msg(tenant_id, task_id, '文件报告发布过程中出现了异常，尝试第' + str(retries + 1) + '次重新发布')

    return wrapper


def get_auth_inst():
    auth_inst = HTTPBasicAuth('hhr', 'hand_hhr')
    return auth_inst


class TaskFile:
    __slots__ = ['fo']

    def __init__(self):
        self.fo = None

    def write_line(self, content):
        self.fo.write(content + '\n')

    def close(self):
        self.fo.close()


class TaskFileUtil:
    sep = applatform.get_sep()
    task_output_path = applatform.get_task_output_path()
    dba = HubDataBaseAlchemy()

    def rec_task_file_info(self, tenant_id, task_id, task_name, user_id, runctl, file_name):
        task_f_tbl = self.dba.get_table('hhr_task_file')

        # generate file uuid
        file_uuid = uuid1().hex

        # insert new file record
        ins = task_f_tbl.insert()
        values = [
            {
                'tenant_id': tenant_id,
                'hhr_task_id': task_id,
                'hhr_task_name': task_name,
                'hhr_user_id': user_id,
                'hhr_runctl_id': runctl,
                'hhr_file_type': 'T',
                'hhr_file_uuid': file_uuid,
                'hhr_file_name': file_name,
                'hhr_dtm_created': get_current_dttm(),
            }
        ]
        # self.dba.conn.execute(ins, values)
        self.dba.new_execute(ins, values)

    def create_task_file(self, task, file_name, *args, **kwargs):
        task_id = task.request.id if hasattr(task, 'request') else task.uuid
        task_name = task.name
        tenant_id = task.request.kwargs['tenant_id'] if hasattr(task, 'request') else dict(eval(task.kwargs))[
            'tenant_id']
        user_id = task.request.kwargs['user_id'] if hasattr(task, 'request') else dict(eval(task.kwargs))['user_id']
        run_ctl = task.request.kwargs['run_ctl'] if hasattr(task, 'request') else dict(eval(task.kwargs))['run_ctl']

        task_file_path = self.task_output_path + task_name + self.sep + task_id + self.sep
        if not os.path.exists(task_file_path):
            os.makedirs(task_file_path)

        try:
            tf = TaskFile()
            tf.fo = open(task_file_path + file_name, 'w', encoding='utf8')
            self.rec_task_file_info(tenant_id, task_id, task_name, user_id, run_ctl, file_name)
            return tf
        except Exception as e:
            raise e

    def create_excel_file_path(self, task, file_name, *args, **kwargs):
        task_id = task.request.id if hasattr(task, 'request') else task.uuid
        task_name = task.name
        tenant_id = task.request.kwargs['tenant_id'] if hasattr(task, 'request') else dict(eval(task.kwargs))[
            'tenant_id']
        user_id = task.request.kwargs['user_id'] if hasattr(task, 'request') else dict(eval(task.kwargs))['user_id']
        run_ctl = task.request.kwargs['run_ctl'] if hasattr(task, 'request') else dict(eval(task.kwargs))['run_ctl']
        task_file_path = self.task_output_path + task_name + self.sep + task_id + self.sep
        if not os.path.exists(task_file_path):
            os.makedirs(task_file_path)
        self.rec_task_file_info(tenant_id, task_id, task_name, user_id, run_ctl, file_name)
        return task_file_path + str(file_name)

    @classmethod
    def update_file_size(cls, tenant_id, task_id, file_name, file_full_name):
        """
        Desc: Update file size to task file info table
        Author: David
        Date: 2018/07/30

        :param task_id:  Id of task
        :param file_name: Name of task file
        :param file_full_name: Full path name of the file
        :return:
        """
        f_bsize = os.path.getsize(file_full_name)
        f_ksize = round(f_bsize, 2)
        ft = cls.dba.get_table('hhr_task_file', 'boogoo_foundation')
        stmt = ft.update(). \
            where(ft.c.tenant_id == tenant_id).where(ft.c.hhr_task_id == task_id). \
            where(ft.c.hhr_file_name == file_name).values({
            ft.c.hhr_file_size: f_ksize
        })
        # cls.dba.conn.execute(stmt)
        cls.dba.new_execute(stmt)

    @classmethod
    def publish_task_files(cls, tenant_id, task_id, task_name):
        """
        Desc: Publish task files after task being finished
        Author: David
        Date: 2018/07/30

        :param task_id: Id of the task
        :param task_name: Name of the task
        :return: None
        """
        task_file_path = cls.task_output_path + task_name + cls.sep + task_id + cls.sep

        if os.path.exists(task_file_path):

            task_file_list = os.listdir(task_file_path)

            cls.update_pub_status(tenant_id, task_id, 'I')
            TaskFileUtil.track_task_msg(tenant_id, task_id, '开始发布文件报告...')

            files_dict = {}
            for i in range(len(task_file_list)):
                task_file_name = task_file_list[i]
                tf_full_name = task_file_path + task_file_name
                files_dict['f' + str(i)] = (task_file_name, open(tf_full_name, 'rb'))

                cls.update_file_size(tenant_id, task_id, task_file_name, tf_full_name)

            if len(files_dict) > 0:

                try:
                    r = cls.post_to_fsvr(files_dict, tenant_id, task_name, task_id)
                    code = r.json().get('code')
                    msg = r.json().get('message')
                    if code == 8000:
                        cls.update_pub_status(tenant_id, task_id, 'P')
                    else:
                        cls.update_pub_status(tenant_id, task_id, 'E')
                        TaskFileUtil.track_task_msg(tenant_id, task_id, str(msg) + ' (' + str(code) + ')')
                    TaskFileUtil.track_task_msg(tenant_id, task_id, str(msg) + ' (' + str(code) + ')')
                except Exception:
                    cls.update_pub_status(tenant_id, task_id, 'E')
                    TaskFileUtil.track_task_msg(tenant_id, task_id, '文件报告发布出错,这可能是由于到文件服务器的网络出现了问题')
            else:
                cls.update_pub_status(tenant_id, task_id, 'P')
        else:
            cls.update_pub_status(tenant_id, task_id, 'P')

    @classmethod
    @pub_retry
    def post_to_fsvr(cls, files_dict, tenant_id, task_name, task_id):
        """
        Desc: Post files to file server for specified task
        Author: David
        Date: 2018/07/30

        :param files_dict: Files dictionary to be uploaded
        :param task_name: The name of the task
        :param task_id: The uuid of the task
        :return:{
            8000:'Task files published successfully',
            -8001:'No file object for uploading',
            -8002:'No file name specified',
            -8003:'The file type of xxx is not permitted',
            -8004:'Exception occurred during uploading'
        }
        """
        me = MultipartEncoder(
            fields=files_dict
        )
        fsrv_url = conf.get('config', 'fsrv_url')
        r = post(fsrv_url + '/upload/' + task_name + '/' + task_id, data=me, headers={'Content-Type': me.content_type},
                 auth=get_auth_inst())
        return r

    @classmethod
    def update_pub_status(cls, tenant_id, task_id, status):
        task_meta = cls.dba.get_table('hhr_task_meta')
        stmt = task_meta.update(). \
            where(task_meta.c.tenant_id == tenant_id). \
            where(task_meta.c.hhr_task_id == task_id). \
            values({
            task_meta.c.hhr_pub_state: status
        })
        # cls.dba.conn.execute(stmt)
        cls.dba.new_execute(stmt)

    @classmethod
    def gen_random_str(cls, length=8):
        """
        :param : The length of random string
        :return: random string
        """
        str_list = [choice(digits + ascii_letters) for i in range(length)]
        s = ''.join(str_list)
        return s

    @classmethod
    def track_task_msg(cls, tenant_id, task_id, msg):
        """
        Desc: Keep track of task message
        Author: David
        Date: 2018/07/30

        :param task_id: The id of task
        :param msg: The message logged
        :return: None
        """
        tm = cls.dba.get_table('hhr_task_msg')

        # insert new file record
        ins = tm.insert()
        values = [
            {
                'tenant_id': tenant_id,
                'hhr_task_id': task_id,
                'hhr_dtm_created': get_current_dttm(),
                'hhr_msg_txt2': msg
            }
        ]
        # cls.dba.conn.execute(ins, values)
        cls.dba.new_execute(ins, values)
