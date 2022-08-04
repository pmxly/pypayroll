#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from celery import Celery
from celery.events.state import Task
from .dbtask import DatabaseTask
from .taskutils import TaskUtils
from .confreader import conf
import logging
import time
from .utils import get_current_dttm
from sqlalchemy import select
import psutil

logger = logging.getLogger(__name__)


class TaskMonitor(DatabaseTask):
    """
    Desc: Task monitor
    Author: David
    Date: 2018/07/30
    """

    def __init__(self):
        super(TaskMonitor, self).__init__()

    def monitor(self, _app):

        def get_task_by_id(state, task_id):
            if hasattr(Task, '_fields'):  # Old version
                return state.tasks.get(task_id)
            else:
                _fields = Task._defaults.keys()
                task = state.tasks.get(task_id)
                if task is not None:
                    task._fields = _fields
                return task

        state = _app.events.State()

        # Sent when a task message is published
        def announce_sent_tasks(event):
            # 'PENDING'
            state.event(event)
            task = get_task_by_id(state, event['uuid'])
            TaskUtils.insert_task_data(self, task)

        # Sent when the worker receives a task
        def announce_received_tasks(event):
            # 'RECEIVED'
            state.event(event)
            task = get_task_by_id(state, event['uuid'])
            TaskUtils.update_task_received(self, task)

        # Sent just before the worker executes the task
        def announce_started_tasks(event):
            # 'STARTED'
            state.event(event)
            task = get_task_by_id(state, event['uuid'])
            TaskUtils.update_task_started(self, task)

        # Sent if the task executed successfully
        def announce_succeeded_tasks(event):
            # 'SUCCESS'
            state.event(event)
            task = get_task_by_id(state, event['uuid'])
            TaskUtils.update_task_succeeded(self, task)

        # Sent if the execution of the task failed
        def announce_failed_tasks(event):
            # 'FAILURE'
            state.event(event)
            task = get_task_by_id(state, event['uuid'])
            TaskUtils.update_task_failed(self, task)

        # Sent if the task has been revoked
        def announce_revoked_tasks(event):
            # 'REVOKED'
            state.event(event)
            task = get_task_by_id(state, event['uuid'])
            TaskUtils.update_task_revoked(self, task)

        # Sent if the task failed, but will be retried in the future
        def announce_retried_tasks(event):
            # 'RETRIED'
            pass

        def worker_heartbeat(event):
            state.event(event)

            for name, worker in state.workers.items():
                svr_name = name.split('@')[1]
                active = worker.active if worker.active is not None else 0
                processed = worker.processed if worker.processed is not None else 0
                status = 'O' if worker.status_string == 'ONLINE' else 'F'

                svr_tbl = self.get_table('hhr_task_svr_lst')

                stmt = select([svr_tbl.c.hhr_svr_name]).where(svr_tbl.c.hhr_svr_name == svr_name)
                # r = self.conn.execute(stmt)
                r = self.new_execute(stmt)
                row = r.fetchone()

                if row is not None:
                    stmt = svr_tbl.update(). \
                        where(svr_tbl.c.hhr_svr_name == svr_name).\
                        values({
                            svr_tbl.c.hhr_svr_cpu: psutil.cpu_percent(None),
                            svr_tbl.c.hhr_svr_memeory: psutil.virtual_memory().percent,
                            svr_tbl.c.hhr_svr_active: active,
                            svr_tbl.c.hhr_svr_processed: processed,
                            svr_tbl.c.hhr_svr_status: status,
                            svr_tbl.c.hhr_dtm_updated: get_current_dttm(),
                        })
                    # self.conn.execute(stmt)
                    self.new_execute(stmt)
                else:
                    ins = svr_tbl.insert()
                    values = [
                        {'hhr_svr_name': svr_name,
                         'hhr_svr_cpu': psutil.cpu_percent(None),
                         'hhr_svr_memeory': psutil.virtual_memory().percent,
                         'hhr_svr_active': active,
                         'hhr_svr_processed': processed,
                         'hhr_svr_status': status,
                         'hhr_dtm_updated': get_current_dttm(),
                         }
                    ]
                    # self.conn.execute(ins, values)
                    self.new_execute(ins, values)

        def worker_offline(event):
            pass

        try_interval = 1
        while True:
            try_interval *= 2
            with _app.connection() as connection:
                recv = _app.events.Receiver(connection, handlers={
                        'task-sent': announce_sent_tasks,
                        'task-started': announce_started_tasks,
                        'task-received': announce_received_tasks,
                        'task-succeeded': announce_succeeded_tasks,
                        'task-failed': announce_failed_tasks,
                        'task-revoked': announce_revoked_tasks,
                        'task-retried': announce_retried_tasks,
                        # 'worker-heartbeat': worker_heartbeat,
                        'worker-offline': worker_offline,
                        '*': state.event,
                })
                recv.capture(limit=None, timeout=None, wakeup=True)
            # try:
            #     try_interval *= 2
            #     with _app.connection() as connection:
            #         recv = _app.events.Receiver(connection, handlers={
            #                 'task-sent': announce_sent_tasks,
            #                 'task-started': announce_started_tasks,
            #                 'task-received': announce_received_tasks,
            #                 'task-succeeded': announce_succeeded_tasks,
            #                 'task-failed': announce_failed_tasks,
            #                 'task-revoked': announce_revoked_tasks,
            #                 'task-retried': announce_retried_tasks,
            #                 'worker-heartbeat': worker_heartbeat,
            #                 'worker-offline': worker_offline,
            #                 '*': state.event,
            #         })
            #         recv.capture(limit=None, timeout=None, wakeup=True)
            # except Exception as e:
            #     logger.error("Failed to capture events: '%s', "
            #                  "trying again in %s seconds.",
            #                  e, try_interval)
            #     logger.debug(e, exc_info=True)
            #     time.sleep(try_interval)


if __name__ == '__main__':
    print('Task Monitor Started......')
    app = Celery(broker=conf.get('config', 'broker_url'))
    TaskMonitor().monitor(app)
