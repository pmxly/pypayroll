# -*- coding: utf-8 -*-

from flask import jsonify, request, make_response
from datetime import datetime
from ...taskutils import get_tasks_map
from ...utils import redis_master


def call_class_emp_upd():
    """刷新人员行安全性"""

    request_data = request.get_json(force=True)
    task_name = "class_emp_upd"
    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)

    tenant_id = request_data.get("tenant_id")
    emp_string = request_data.get("emp_list")

    # 同步实时更新
    if emp_string is None:
        emplid = request_data.get("emplid")
        empl_rcd = request_data.get("empl_rcd")
        if (emplid is not None) and (empl_rcd is not None):
            result = task_handle(**{'tenant_id': tenant_id, 'emplid': emplid, 'empl_rcd': empl_rcd})
            if result == 1 is not None:
                return make_response(jsonify(status='OK'))
            else:
                return make_response(jsonify(status='ERROR'))
        else:
            return make_response(jsonify(status='ERROR'))

    # 异步更新
    else:
        user_id = request_data.get("user")
        run_ctl_id = 'class_emp_upd_' + (datetime.now().strftime('%Y%m%d%H%M%S'))
        result = task_handle.apply_async(kwargs={'user_id': user_id, 'run_ctl': run_ctl_id,
                                                 'tenant_id': tenant_id, 'emp_string': emp_string},
                                         queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
        else:
            return make_response(jsonify(status='ERROR'))


def call_class_dept_upd():
    """调用刷新部门安全性进程"""

    request_data = request.get_json(force=True)

    task_name = "class_dept_upd"
    run_ctl_id = 'class_dept_upd' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")
    plist = request_data.get("plist")

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)

    try:
        result = task_handle.apply_async(kwargs={'user_id': user_id, 'run_ctl': run_ctl_id,
                                                 'tenant_id': tenant_id, 'plist': plist},
                                         queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
    except Exception:
        return make_response(jsonify(status='ERROR'))


def call_report_to_upd():
    """调用刷新职位、人员汇报关系进程"""

    request_data = request.get_json(force=True)

    task_name = "report_to_upd"
    run_ctl_id = 'report_to_upd_' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")
    eff_dt = request_data.get("eff_dt", None)

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)

    try:
        result = task_handle.apply_async(
            kwargs={'user_id': user_id, 'run_ctl': run_ctl_id, 'tenant_id': tenant_id, 'eff_dt': eff_dt},
            queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
    except Exception:
        return make_response(jsonify(status='ERROR'))


def call_dept_level_upd():
    """调用刷新部门层级关系进程"""

    request_data = request.get_json(force=True)

    task_name = "dept_level_upd"
    run_ctl_id = 'dept_level_upd_' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")
    effdt_string = request_data.get("effdt_string", '')
    sync_all_flag = request_data.get('sync_all_flag', 'N')

    redis_master.set("dept_level_upd_effdt_string", effdt_string)

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)
    try:
        result = task_handle.apply_async(
            kwargs={'user_id': user_id, 'run_ctl': run_ctl_id, 'tenant_id': tenant_id, 'sync_all_flag': sync_all_flag},
            queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
    except Exception:
        return make_response(jsonify(status='ERROR'))


def call_update_emp_account():
    """调用处理员工账号的进程"""

    request_data = request.get_json(force=True)

    task_name = "update_emp_acc"
    run_ctl_id = 'update_emp_acc_' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")
    emp_id = request_data.get("emp_id")

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)

    try:
        result = task_handle.apply_async(
            kwargs={'user_id': user_id, 'run_ctl': run_ctl_id, 'tenant_id': tenant_id, 'emp_id': emp_id},
            queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
    except Exception:
        return make_response(jsonify(status='ERROR'))


def call_pt_pt_quota_generate():
    """调用生成假期额度进程"""

    request_data = request.get_json(force=True)

    task_name = "pt_quota_generate"
    run_ctl_id = 'pt_quota_generate_' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)

    try:
        result = task_handle.apply_async(kwargs={'user_id': user_id, 'run_ctl': run_ctl_id, 'tenant_id': tenant_id},
                                         queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
    except Exception:
        return make_response(jsonify(status='ERROR'))


def call_om_data_sync():
    """调用组织数据同步进程"""

    request_data = request.get_json(force=True)

    task_name = "om_data_sync"
    run_ctl_id = 'om_data_sync_' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")
    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)

    try:
        result = task_handle.apply_async(kwargs={'user_id': user_id, 'run_ctl': run_ctl_id, 'tenant_id': tenant_id},
                                         queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
    except Exception:
        return make_response(jsonify(status='ERROR'))


def call_pers_data_sync():
    """调用人员数据同步进程"""

    request_data = request.get_json(force=True)

    task_name = "pers_data_sync"
    run_ctl_id = 'pers_data_sync_' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)

    try:
        result = task_handle.apply_async(kwargs={'user_id': user_id, 'run_ctl': run_ctl_id, 'tenant_id': tenant_id},
                                         queue='SHARE_Q', retry=False)
        if result.task_id is not None:
            return make_response(jsonify(status='OK'))
    except Exception as e:
        return make_response(jsonify(status='ERROR'))
