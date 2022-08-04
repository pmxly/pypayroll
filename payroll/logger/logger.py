# -*- coding: utf-8 -*-


from ...utils import TaskFileUtil

class Logger:
    """
    Desc: 薪资日志工具类
    Author: David
    Date: 2018/08/14
    """

    def __init__(self, tenant_id, task_id):
        self.tenant_id = tenant_id
        self.task_id = task_id

    def message(self, msg):
        """
        将日志写入到进程消息日志表中
        :param msg: 日志内容
        :return:
        """
        TaskFileUtil.track_task_msg(self.tenant_id, self.task_id, msg)
