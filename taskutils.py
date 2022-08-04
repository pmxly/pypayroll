#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from sqlalchemy import select
import os
from importlib import import_module
from .applatform import get_task_pool, get_prj_path, get_sep
from .utils import TaskFileUtil
from sqlalchemy.sql import text


def hr_task_filter(name):
    def _wrapper(func):
        def __wrapper(cls, mon, task):
            if name == task.name:
                pass
            else:
                func(cls, mon, task)
        return __wrapper
    return _wrapper


class TaskUtils:
    registry = {}
    router = {}
    __STATUS__ = {
        'failure': '1',
        'ignored': '2',
        'pending': '3',
        'received': '4',
        'rejected': '5',
        'retry': '6',
        'revoked': '7',
        'started': '8',
        'succeeded': '9'}

    @staticmethod
    def is_task(f_name):
        if f_name.endswith('.pyc'):
            return False
        elif '__init__' in f_name:
                return False
        else:
            return True

    @staticmethod
    def get_tasks_registry_router(path):
        for main_dir, subdir, file_name_list in os.walk(path):
            mod_file_list = filter(TaskUtils.is_task, file_name_list)
            for filename in list(mod_file_list):
                m_path = os.path.join(main_dir, filename)
                # strip project parent path
                m_path = m_path[len(get_prj_path()):]
                module_path_name = m_path.replace(get_sep(), '.').rpartition('.')[0]
                task_module = import_module(module_path_name)
                func_name = filename.rpartition('.')[0]
                task = getattr(task_module, func_name)
                TaskUtils.registry[module_path_name] = task
                TaskUtils.router[func_name] = task
        return TaskUtils.registry, TaskUtils.router

    @classmethod
    def get_task_response(cls, task):
        """
        _fields = (
            'uuid', 'name', 'state', 'received', 'sent', 'started', 'rejected',
            'succeeded', 'failed', 'retried', 'revoked', 'args', 'kwargs',
            'eta', 'expires', 'retries', 'worker', 'result', 'exception',
            'timestamp', 'runtime', 'traceback', 'exchange', 'routing_key',
            'clock', 'client', 'root', 'root_id', 'parent', 'parent_id',
            'children',
        )
        :param task:
        :return:
        """
        response = {}
        for name in task._fields:
            if name not in ['uuid', 'worker']:
                response[name] = getattr(task, name, None)
        response['task-id'] = task.uuid
        if task.worker is not None:
            response['worker'] = task.worker.hostname
        return response

    @classmethod
    @hr_task_filter(name='keep_conn_alive')
    def insert_task_data(cls, mon, task):

        response = TaskUtils.get_task_response(task)
        # get data from response

        args = response.get('args', '')
        kwargs = response.get('kwargs', '')
        if kwargs != '':
            t_index = kwargs.find('run_param')
            if t_index >= 0:
                t_index = t_index - 3
                kwargs = kwargs[0:t_index] + '}'
        kwargs = dict(eval(kwargs))
        tenant_id = kwargs['tenant_id']
        user_id = kwargs['user_id']
        run_control_id = kwargs['run_ctl']
        task_id = response['task-id']
        task_name = response['name']
        # status = response.get('state', 'pending')
        status = cls.__STATUS__['pending']
        sent = response.get('sent', None)
        dtm_sent = None if not sent else datetime.fromtimestamp(sent)
        kwargs = response.get('kwargs', '')

        timestamp = response.get('timestamp', None)
        dtm_timestamp = None if not timestamp else datetime.fromtimestamp(timestamp)

        # db_conn = mon.conn
        # Generate task instance id
        # id_tbl = mon.get_table('hhr_task_ins_id')
        # stmt = select([id_tbl.c.hhr_id]).with_for_update(nowait=False).\
        #     where(id_tbl.c.tenant_id == tenant_id).\
        #     where(id_tbl.c.hhr_type == 'TASK')
        id_sql = text("select hhr_id from boogoo_foundation.hhr_task_ins_id where tenant_id = :b1 and hhr_type = 'TASK'")
        row = mon.new_execute(id_sql, b1=tenant_id).fetchone()
        cur_inst_id = row['hhr_id']
        cur_inst_id += 1

        # Update new instance id back into ID table
        id_upd_sql = text("update boogoo_foundation.hhr_task_ins_id set hhr_id = :b2 where tenant_id = :b1 and hhr_type = 'TASK'")
        # stmt = id_tbl.update().where(id_tbl.c.tenant_id == tenant_id).\
        #     where(id_tbl.c.hhr_type == 'TASK').values(hhr_id=cur_inst_id)
        # db_conn.execute(stmt)
        mon.new_execute(id_upd_sql, b1=tenant_id, b2=cur_inst_id)

        # Insert task row
        task_meta = mon.get_table('hhr_task_meta')
        meta_insert = task_meta.insert()
        values = [
                  {
                   'tenant_id': tenant_id,
                   'hhr_prcs_inst': cur_inst_id,
                   'hhr_runctl_id': run_control_id,
                   'hhr_task_id': task_id,
                   'hhr_task_name': task_name,
                   'hhr_state': status,
                   'hhr_user_id':  user_id,
                   'hhr_dtm_sent': dtm_sent,
                   'hhr_dtm_received': None,
                   'hhr_dtm_started': None,
                   'hhr_dtm_rejected': None,
                   'hhr_dtm_succeeded': None,
                   'hhr_dtm_failed': None,
                   'hhr_dtm_retried': None,
                   'hhr_dtm_revoked': None,
                   # 'hhr_args': args,
                   # 'hhr_kwargs': kwargs[0:125],
                   'hhr_eta': response.get('eta', ''),
                   'hhr_expires': response.get('expires', 0),
                   'hhr_retries': response.get('retries', 0),
                   'hhr_result': response.get('result', None),
                   'hhr_exception': response.get('exception', None),
                   'hhr_timestamp': dtm_timestamp,
                   'hhr_runtime': response.get('runtime', 0),
                   'hhr_traceback': response.get('traceback', None),
                   'hhr_exchange': response.get('exchange', ''),
                   'hhr_routing_key': response.get('routing_key',  ''),
                   'hhr_clock': response.get('clock', 0),
                   'hhr_client': response.get('client',  ''),
                   'hhr_root': response.get('root', None),
                   'hhr_root_id': response.get('root_id', ''),
                   'hhr_parent': response.get('parent', None),
                   'hhr_parent_id': response.get('parent_id',  ''),
                   'hhr_children': response.get('children',  None),
                   'hhr_worker': response.get('worker', ''),
                   }
                 ]
        # db_conn.execute(meta_insert, values)
        mon.new_execute(meta_insert, values)

    @classmethod
    @hr_task_filter(name='keep_conn_alive')
    def update_task_received(cls, mon, task):
        response = TaskUtils.get_task_response(task)

        # get data from response
        task_id = response['task-id']
        # status = response.get('state', 'received')
        status = cls.__STATUS__['received']
        received = response.get('received', None)
        dtm_received = None if not received else datetime.fromtimestamp(received)
        timestamp = response.get('timestamp', None)
        dtm_timestamp = None if not timestamp else datetime.fromtimestamp(timestamp)
        clock = response.get('clock', 0)
        root = response.get('root', '')
        worker = response.get('worker', '')

        kwargs = response.get('kwargs', '')
        if kwargs != '':
            t_index = kwargs.find('run_param')
            if t_index >= 0:
                t_index = t_index - 3
                kwargs = kwargs[0:t_index] + '}'
        kwargs = dict(eval(kwargs))
        tenant_id = kwargs['tenant_id']

        # Update task data
        db_conn = mon.conn
        task_meta = mon.get_table('hhr_task_meta')
        stmt = task_meta.update().\
            where(task_meta.c.tenant_id == tenant_id).\
            where(task_meta.c.hhr_task_id == task_id).\
            values({
                task_meta.c.hhr_state: status,
                task_meta.c.hhr_dtm_received: dtm_received,
                task_meta.c.hhr_timestamp: dtm_timestamp,
                task_meta.c.hhr_clock: clock,
                task_meta.c.hhr_root: root,
                task_meta.c.hhr_worker: worker,
            })
        # db_conn.execute(stmt)
        mon.new_execute(stmt)
        TaskFileUtil.track_task_msg(tenant_id, task_id, '收到任务，等待执行')

    @classmethod
    @hr_task_filter(name='keep_conn_alive')
    def update_task_started(cls, mon, task):
        response = TaskUtils.get_task_response(task)

        # get data from response
        task_id = response['task-id']
        # status = response.get('state', 'started')
        status = cls.__STATUS__['started']
        started = response.get('started', None)
        dtm_started = None if not started else datetime.fromtimestamp(started)
        timestamp = response.get('timestamp', None)
        dtm_timestamp = None if not timestamp else datetime.fromtimestamp(timestamp)
        clock = response.get('clock', 0)
        root = response.get('root', '')

        kwargs = response.get('kwargs', '')
        if kwargs != '':
            t_index = kwargs.find('run_param')
            if t_index >= 0:
                t_index = t_index - 3
                kwargs = kwargs[0:t_index] + '}'
        kwargs = dict(eval(kwargs))
        tenant_id = kwargs['tenant_id']

        # Update task data
        db_conn = mon.conn
        task_meta = mon.get_table('hhr_task_meta')
        stmt = task_meta.update().\
            where(task_meta.c.tenant_id == tenant_id).\
            where(task_meta.c.hhr_task_id == task_id).\
            values({
                task_meta.c.hhr_state: status,
                task_meta.c.hhr_dtm_started: dtm_started,
                task_meta.c.hhr_timestamp: dtm_timestamp,
                task_meta.c.hhr_clock: clock,
                task_meta.c.hhr_root: root,
            })
        # db_conn.execute(stmt)
        mon.new_execute(stmt)
        TaskFileUtil.track_task_msg(tenant_id, task_id, '开始处理任务...')

    @classmethod
    @hr_task_filter(name='keep_conn_alive')
    def update_task_succeeded(cls, mon, task):
        response = TaskUtils.get_task_response(task)

        # even if task was revoked when used with Signal USR1,
        # the task will still be considered as success, so we need to cope with this scenario
        revoked = response.get('revoked', None)
        if revoked is not None:
            return

        # get data from response
        task_id = response['task-id']
        task_name = response['name']
        # status = response.get('state', 'succeeded')
        status = cls.__STATUS__['succeeded']
        received = response.get('received', None)
        dtm_received = None if not received else datetime.fromtimestamp(received)

        started = response.get('started', None)
        dtm_started = None if not started else datetime.fromtimestamp(started)

        succeeded = response.get('succeeded', None)
        dtm_succeeded = None if not succeeded else datetime.fromtimestamp(succeeded)

        result = response.get('result', None)
        runtime = response.get('runtime', 0)
        timestamp = response.get('timestamp', None)
        dtm_timestamp = None if not timestamp else datetime.fromtimestamp(timestamp)
        clock = response.get('clock', 0)
        root = response.get('root', '')

        kwargs = response.get('kwargs', '')
        if kwargs != '':
            t_index = kwargs.find('run_param')
            if t_index >= 0:
                t_index = t_index - 3
                kwargs = kwargs[0:t_index] + '}'
        kwargs = dict(eval(kwargs))
        tenant_id = kwargs['tenant_id']

        # Update task data
        # db_conn = mon.conn
        task_meta = mon.get_table('hhr_task_meta')
        stmt = task_meta.update().\
            where(task_meta.c.tenant_id == tenant_id).\
            where(task_meta.c.hhr_task_id == task_id).\
            values({
                task_meta.c.hhr_state: status,
                task_meta.c.hhr_dtm_received: dtm_received,
                task_meta.c.hhr_dtm_started: dtm_started,
                task_meta.c.hhr_dtm_succeeded: dtm_succeeded,
                task_meta.c.hhr_result: result,
                task_meta.c.hhr_timestamp: dtm_timestamp,
                task_meta.c.hhr_runtime: round(runtime, 2),
                task_meta.c.hhr_clock: clock,
                task_meta.c.hhr_root: root,
            })
        # db_conn.execute(stmt)
        mon.new_execute(stmt)
        TaskFileUtil.track_task_msg(tenant_id, task_id, '成功处理任务!')
        # TaskFileUtil.publish_task_files(task_id, task_name)

        if task_name == 'pycalc':
            stmt = text('select hhr_runctl_id, hhr_user_id from boogoo_foundation.hhr_task_meta where tenant_id = :b2 and hhr_task_id = :b1')
            row = mon.new_execute(stmt, b1=task_id, b2=tenant_id).fetchone()
            if row is not None:
                runctl_id = row['hhr_runctl_id']
                action_user = row['hhr_user_id']
                runctl_lst = runctl_id.split('~')
                tenant_id = runctl_lst[0]
                cal_grp_id = runctl_lst[1]
                # 判断是否所有人员都已经成功计算，更新日历/日历组运行状态
                sql = text("select 'Y' from boogoo_payroll.hhr_py_payee_calc_stat where tenant_id = :b1 and hhr_pycalgrp_id = :b2 "
                           "and (hhr_py_calc_stat <> 'S' and hhr_py_calc_stat <> 'E' and hhr_py_ind_stat <> 'W') ")
                r = mon.new_execute(sql, b1=tenant_id, b2=cal_grp_id).fetchone()
                if r is None:
                    sql = text("update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '4', hhr_calc_dttm = :b3, hhr_calc_user = :b4, "
                               "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
                    mon.new_execute(sql, b1=tenant_id, b2=cal_grp_id, b3=dtm_succeeded, b4=action_user)

    @classmethod
    @hr_task_filter(name='keep_conn_alive')
    def update_task_failed(cls, mon, task):
        response = TaskUtils.get_task_response(task)

        # get data from response
        task_id = response['task-id']
        task_name = response['name']
        # status = response.get('state', 'failure')
        status = cls.__STATUS__['failure']

        sent = response.get('sent', None)
        dtm_sent = None if not sent else datetime.fromtimestamp(sent)

        received = response.get('received', None)
        dtm_received = None if not received else datetime.fromtimestamp(received)

        started = response.get('started', None)
        dtm_started = None if not started else datetime.fromtimestamp(started)

        failed = response.get('failed', None)
        dtm_failed = None if not failed else datetime.fromtimestamp(failed)

        exception = response.get('exception', None)
        if exception is not None:
            exception = exception[0:255]

        timestamp = response.get('timestamp', None)
        dtm_timestamp = None if not timestamp else datetime.fromtimestamp(timestamp)

        traceback = response.get('traceback', None)
        exchange = response.get('exchange', '')
        routing_key = response.get('routing_key', '')
        clock = response.get('clock', 0)
        client = response.get('client', 0)
        root = response.get('root', None)

        kwargs = response.get('kwargs', '')
        if kwargs != '':
            t_index = kwargs.find('run_param')
            if t_index >= 0:
                t_index = t_index - 3
                kwargs = kwargs[0:t_index] + '}'
        kwargs = dict(eval(kwargs))
        tenant_id = kwargs['tenant_id']

        # Update task data
        db_conn = mon.conn
        task_meta = mon.get_table('hhr_task_meta')
        stmt = task_meta.update().\
            where(task_meta.c.tenant_id == tenant_id).\
            where(task_meta.c.hhr_task_id == task_id).\
            values({
                task_meta.c.hhr_state: status,
                task_meta.c.hhr_dtm_sent: dtm_sent,
                task_meta.c.hhr_dtm_received: dtm_received,
                task_meta.c.hhr_dtm_started: dtm_started,
                task_meta.c.hhr_dtm_failed: dtm_failed,
                task_meta.c.hhr_exception: exception,
                task_meta.c.hhr_timestamp: dtm_timestamp,
                task_meta.c.hhr_traceback: traceback,
                task_meta.c.hhr_exchange: exchange,
                task_meta.c.hhr_routing_key: routing_key,
                task_meta.c.hhr_clock: clock,
                task_meta.c.hhr_client: client,
                task_meta.c.hhr_root: root,
            })
        # db_conn.execute(stmt)
        mon.new_execute(stmt)
        TaskFileUtil.track_task_msg(tenant_id, task_id, '任务处理失败!')
        # TaskFileUtil.publish_task_files(task_id, task_name)

    @classmethod
    @hr_task_filter(name='keep_conn_alive')
    def update_task_revoked(cls, mon, task):
        response = TaskUtils.get_task_response(task)

        # get data from response
        task_id = response['task-id']
        task_name = response['name']
        # status = response.get('state', 'revoked')
        status = cls.__STATUS__['revoked']
        received = response.get('received', None)
        dtm_received = None if not received else datetime.fromtimestamp(received)

        started = response.get('started', None)
        dtm_started = None if not started else datetime.fromtimestamp(started)

        revoked = response.get('revoked', None)
        dtm_revoked = None if not revoked else datetime.fromtimestamp(revoked)

        result = response.get('result', None)
        runtime = response.get('runtime', 0)
        runtime = 0 if runtime is None else runtime
        timestamp = response.get('timestamp', None)
        dtm_timestamp = None if not timestamp else datetime.fromtimestamp(timestamp)
        clock = response.get('clock', 0)
        root = response.get('root', '')

        kwargs = response.get('kwargs', '')
        if kwargs != '':
            t_index = kwargs.find('run_param')
            if t_index >= 0:
                t_index = t_index - 3
                kwargs = kwargs[0:t_index] + '}'
        kwargs = dict(eval(kwargs))
        tenant_id = kwargs['tenant_id']

        # Update task data
        db_conn = mon.conn
        task_meta = mon.get_table('hhr_task_meta')
        stmt = task_meta.update().\
            where(task_meta.c.tenant_id == tenant_id).\
            where(task_meta.c.hhr_task_id == task_id).\
            values({
                task_meta.c.hhr_state: status,
                task_meta.c.hhr_dtm_received: dtm_received,
                task_meta.c.hhr_dtm_started: dtm_started,
                task_meta.c.hhr_dtm_revoked: dtm_revoked,
                task_meta.c.hhr_result: result,
                task_meta.c.hhr_timestamp: dtm_timestamp,
                task_meta.c.hhr_runtime: round(runtime, 2),
                task_meta.c.hhr_clock: clock,
                task_meta.c.hhr_root: root,
            })
        # db_conn.execute(stmt)
        mon.new_execute(stmt)
        TaskFileUtil.track_task_msg(tenant_id, task_id, '任务处理被撤销!')
        # TaskFileUtil.publish_task_files(task_id, task_name)


def get_tasks_map():
    """
    获取方法名与方法定义的映射
    :return: 
    """
    task_pool_path = get_task_pool()
    task_router = TaskUtils.router
    if len(task_router) == 0:
        task_router = TaskUtils.get_tasks_registry_router(task_pool_path)[1]
    return task_router


def get_tasks_registry():
    task_pool_path = get_task_pool()
    task_registry = TaskUtils.registry
    if len(task_registry) == 0:
        task_registry = TaskUtils.get_tasks_registry_router(task_pool_path)[0]
    return task_registry
