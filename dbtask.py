#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from celery import Task
from .dbengine import DataBaseAlchemy
from .utils import TaskFileUtil


class DatabaseTask(Task, DataBaseAlchemy):
    
    def __init__(self):
        super(DatabaseTask, self).__init__()
        self.fu = TaskFileUtil()

    def create_file(self, task, file_name, *args, **kwargs):
        return self.fu.create_task_file(task, file_name, *args, **kwargs)

    def get_excel_file_path(self, task, file_name, *args, **kwargs):
        return self.fu.create_excel_file_path(task, file_name, *args, **kwargs)

    def message(self, msg):
        tenant_id = self.request.kwargs['tenant_id'] if hasattr(self, 'request') else dict(eval(self.kwargs))['tenant_id']
        task_id = self.request.id if hasattr(self, 'request') else self.uuid
        TaskFileUtil.track_task_msg(tenant_id, task_id, msg)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        tenant_id = kwargs['tenant_id']
        TaskFileUtil.publish_task_files(tenant_id, task_id, self.name)
