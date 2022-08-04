# -*- coding: utf-8 -*-

from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
from ..taskutils import get_tasks_map
from celery.result import AsyncResult
from ..permission import permission_check
from ..utils import TaskFileUtil
from random import randint
from ..confreader import conf

from .hr_hub.hr_bus_hub import (call_class_emp_upd,
                                call_class_dept_upd,
                                call_report_to_upd,
                                call_dept_level_upd,
                                call_pt_pt_quota_generate,
                                call_update_emp_account)
import py_eureka_client.eureka_client as eureka_client


flask_app = Flask(__name__)
CORS(flask_app, supports_credentials=True)


process_server_host = conf.get('config', 'server_host')
process_server_port = int(conf.get('config', 'server_port'))
eureka_server = conf.get('config', 'eureka_server')
# The flowing code will register your server to eureka server and also start to send heartbeat every 30 seconds
eureka_client.init(eureka_server=eureka_server,
                   app_name="boogoo-process",
                   # 当前组件的主机名，可选参数，如果不填写会自动计算一个，如果服务和 eureka 服务器部署在同一台机器，请必须填写，否则会计算出 127.0.0.1
                   instance_host=process_server_host,
                   instance_port=process_server_port,
                   # 调用其他服务时的高可用策略，可选，默认为随机
                   ha_strategy=eureka_client.HA_STRATEGY_RANDOM)


@flask_app.route('/hello', methods=['POST'])
@permission_check(permission=1)
def hello():
    return make_response(jsonify({'status': 'OK', 'message': 'hello'}))


@flask_app.route('/process/pay_calc', methods=['POST'])
@permission_check(permission=1)
def pay_calc():
    """
    Desc: 薪资计算进程调度入口
    Author: David
    Date: 2018/08/10
    """
    from .py_hub.py_bus_hub import call_py_calc
    return call_py_calc()


@flask_app.route('/process/verify_formula', methods=['POST'])
@permission_check(permission=1)
def verify_payroll_formula():
    """
    Desc: 公式验证入口
    Author: David
    Date: 2019/02/15
    """
    from .py_hub.py_bus_hub import exec_verify_formula
    return exec_verify_formula()


@flask_app.route('/process/empSecUpd', methods=['POST'])
@permission_check(permission=1)
def emp_sec_upd():
    """
    Desc: 刷新人员行安全性
    Author: David
    Date: 2018/07/30
    """
    return call_class_emp_upd()


@flask_app.route('/process/plistSecUpd', methods=['POST'])
@permission_check(permission=1)
def plist_sec_upd():
    """
    Desc: 按许可权刷新部门安全性
    Author: David
    Date: 2018/07/30
    """
    return call_class_dept_upd()


@flask_app.route('/process/report_to_upd', methods=['POST'])
@permission_check(permission=1)
def report_to_upd():
    """
    Desc: 刷新职位、人员汇报关系
    Author: David
    Date: 2019/01/17
    """
    return call_report_to_upd()


@flask_app.route('/process/dept_level_upd', methods=['POST'])
@permission_check(permission=1)
def dept_level_upd():
    """
    Desc: 刷新部门层级关系
    Author: David
    Date: 2019/01/17
    """
    return call_dept_level_upd()


@flask_app.route('/process/pt_quota_generate', methods=['POST'])
@permission_check(permission=1)
def pt_quota_generate():
    """
    Desc: 生成假期额度
    Author: David
    Date: 2019/01/23
    """
    return call_pt_pt_quota_generate()


@flask_app.route('/process/pyReptDetail', methods=['POST'])
@permission_check(permission=1)
def py_rept_detail():
    """
    Desc: 生成薪资明细表
    Author: David
    Date: 2019/01/16
    """
    from .py_hub.py_rpt_hub import call_py_report_detail
    return call_py_report_detail()


@flask_app.route('/process/pyReptSum', methods=['POST'])
@permission_check(permission=1)
def py_rept_sum():
    """
    Desc: 生成薪资汇总表
    Author: David
    Date: 2019/03/04
    """
    from .py_hub.py_rpt_hub import call_py_report_sum
    return call_py_report_sum()


@flask_app.route('/process/pers_trans_rpt', methods=['POST'])
@permission_check(permission=1)
def pers_trans_rpt_sum():
    """
    Desc: 生成人事异动情况表（大豪）
    Author: 陶雨
    Date: 2019/04/22
    """
    from .hr_hub.hr_rpt_hub import call_pers_trans_rpt
    return call_pers_trans_rpt()


@flask_app.route('/process/upd_emp_account', methods=['POST'])
@permission_check(permission=1)
def upd_emp_account():
    """
    Desc: 创建/更新员工账号
    Author: David
    Date: 2019/03/08
    """
    return call_update_emp_account()


@flask_app.route('/process/om_data_sync', methods=['POST'])
@permission_check(permission=1)
def om_data_sync():
    """
    Desc: 组织数据同步（大豪）
    Author: David
    Date: 2019/03/20
    """
    from .hr_hub.hr_bus_hub import call_om_data_sync
    return call_om_data_sync()


@flask_app.route('/process/pers_data_sync', methods=['POST'])
@permission_check(permission=1)
def pers_data_sync():
    """
    Desc: 人员数据同步（大豪）
    Author: David
    Date: 2019/03/20
    """
    from .hr_hub.hr_bus_hub import call_pers_data_sync
    return call_pers_data_sync()


# --------------------------------------------Core Engine-------------------------------------------- #
@flask_app.route('/process/schedule', methods=['POST'])
@permission_check(permission=1)
def process_schedule():
    """
    Desc: Interface for users to schedule a new task
    Author: David
    Date: 2018/07/30
    :return:Id of the new task or error code wrapped in JSON format
    """

    try:
        request_data = request.get_json(force=True)
        task_name = request_data.get("task_name")
        user_id = request_data.get("user")
        run_ctl_id = request_data.get("runctl")
        server = request_data.get("server")

        kw = dict()
        kw['user_id'] = user_id
        kw['run_ctl'] = run_ctl_id

        tasks_router = get_tasks_map()
        task_handle = tasks_router.get(task_name, None)
        if not task_handle:
            return make_response(
                jsonify(status="ERROR", message="No specified task defined. Please contact your system administrator"))
        if server == '' or server is None or server == ' ':
            # server = "SHARE_Q"
            routing_key = "SHARE_Q"
        else:
            routing_key = server + '_' + str(get_routing_key_inx())
        # result = task_handle.apply_async(kwargs={'user_id': user_id, 'run_ctl': run_ctl_id}, exchange=server,
        #                                  routing_key=routing_key, retry=True)
        result = task_handle.apply_async(kwargs=kw, queue=routing_key, retry=False)
        return make_response(jsonify(status="OK", task_id=result.task_id))
    except Exception as e:
        flask_app.logger.info("Exception in routine: process_schedule")
        return make_response(jsonify(status="ERROR", message='run time error'))


@flask_app.route('/process/revoke', methods=['POST'])
@permission_check(permission=1)
def revoke():
    """
    Desc: Interface for users to revoke a pending or running task
    Author: David
    Date: 2018/07/30
    :return: json data
    """

    try:
        request_data = request.get_json(force=True)
        task_id = request_data.get("task_id")

        AsyncResult(task_id).revoke(terminate=True, signal='USR1')
        # s = app.control.revoke(task_id, terminate=True, signal='USR1')
        return make_response(jsonify(status="OK", task_id=task_id))
    except Exception as e:
        flask_app.logger.info("Exception in routine: revoke")
        return make_response(jsonify(status="ERROR", message='revoke error'))


@flask_app.route('/process/re_pub', methods=['POST'])
@permission_check(permission=1)
def re_pub():
    """
    Desc: Interface for users to re-publish reports for a task that failed to publish reports
    Author: David
    Date: 2018/07/30
    :return: Json data
    """

    try:
        request_data = request.get_json(force=True)
        tenant_id = request_data.get("tenant_id")
        task_id = request_data.get("task_id")
        task_name = request_data.get("task_name")
        TaskFileUtil.track_task_msg(tenant_id, task_id, '重新发布文件报告...')
        TaskFileUtil.publish_task_files(tenant_id, task_id, task_name)
        return make_response(jsonify(status='OK', task_id=task_id))
    except Exception as e:
        flask_app.logger.info("Exception in routine: re_pub")
        return make_response(jsonify(status='ERROR', message='re-publish error'))


def get_routing_key_inx():
    t_inx = (1, 2, 3)
    i = randint(0, len(t_inx) - 1)
    return t_inx[i]


# @flask_app.route('/server/refresh', methods=['GET', 'POST'])
# def server_refresh():
#     app = Celery(broker=conf.get('config', 'broker_url'))
#     get_workers(app)
#
#
# def get_workers(_app):
#     _inspect_methods = ('stats', 'active_queues', 'registered', 'scheduled',
#                         'active', 'reserved', 'revoked', 'conf')
#     destination = None
#     timeout = 1.0
#     inspect = _app.control.inspect(timeout=timeout, destination=destination)
#     results = []
#
#     for method in _inspect_methods:
#         results.append(getattr(inspect, method)())
#     for i, result in enumerate(results):
#         if result is None:
#             continue
#         for worker_name, response in result.items():
#             if response is not None:
#                 info = {}
#                 info[_inspect_methods[i]] = response
#                 info['curr_dttm'] = get_current_dttm()


# if __name__ == 'hhr.flaskcelery.tasksrouter':
#     flask_app.run(host='0.0.0.0', port=5000, debug=True)

# --------------------------------------------Core Engine-------------------------------------------- #
