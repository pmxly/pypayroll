from flask import jsonify, request, make_response
from datetime import datetime
from ...taskutils import get_tasks_map


def call_py_report_detail():
    """调用生成薪资明细表进程"""

    request_data = request.get_json(force=True)

    task_name = "py_report_detail"
    user_id = request_data.get("user")
    run_ctl_id = 'py_report_detail_' + (datetime.now().strftime('%Y%m%d%H%M%S'))
    tenant_id = request_data.get("tenant_id")
    lang = request_data.get("lang")
    tmpl_cd = request_data.get("tmpl_cd")
    py_cal_id = request_data.get("py_cal_id")
    py_group_id = request_data.get("py_group_id")
    start_date = request_data.get("start_date")
    end_date = request_data.get("end_date")
    dept_cd = request_data.get("dept_cd")
    has_child_dept = request_data.get("has_child_dept")
    emp_id = request_data.get("emp_id")
    emp_rcd = request_data.get("emp_rcd")
    col_trac_detail = request_data.get("col_trac_detail")

    kw = dict()
    kw['user_id'] = user_id
    kw['run_ctl'] = run_ctl_id
    kw['tenant_id'] = tenant_id
    kw['lang'] = lang
    kw['tmpl_cd'] = tmpl_cd
    kw['py_cal_id'] = py_cal_id
    kw['py_group_id'] = py_group_id
    kw['start_date'] = start_date
    kw['end_date'] = end_date
    kw['dept_cd'] = dept_cd
    kw['has_child_dept'] = has_child_dept
    kw['emp_id'] = emp_id
    kw['emp_rcd'] = emp_rcd
    kw['col_trac_detail'] = col_trac_detail

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)
    try:
        if (py_cal_id is not None) or ((start_date is not None) and (end_date is not None)):
            result = task_handle.apply_async(kwargs=kw, queue='SHARE_Q', retry=False)
            if result.task_id is not None:
                return make_response(jsonify({'status': 'OK', 'task_id':result.task_id}))
        else:
            return make_response(jsonify(status='ERROR'))
    except Exception:
        return make_response(jsonify(status='ERROR'))


def call_py_report_sum():
    """调用生成薪资汇总报表进程"""

    request_data = request.get_json(force=True)

    task_name = "py_report_sum"
    user_id = request_data.get("user")
    run_ctl_id = 'py_report_sum_' + (datetime.now().strftime('%Y%m%d%H%M%S'))
    tenant_id = request_data.get("tenant_id")
    lang = request_data.get("lang")
    tmpl_cd = request_data.get("tmpl_cd")
    py_cal_id = request_data.get("py_cal_id")
    py_group_id = request_data.get("py_group_id")
    start_date = request_data.get("start_date")
    end_date = request_data.get("end_date")
    dept_cd = request_data.get("dept_cd")
    has_child_dept = request_data.get("has_child_dept")
    tree_lvl = request_data.get("tree_lvl")

    kw = dict()
    kw['user_id'] = user_id
    kw['run_ctl'] = run_ctl_id
    kw['tenant_id'] = tenant_id
    kw['lang'] = lang
    kw['tmpl_cd'] = tmpl_cd
    kw['py_cal_id'] = py_cal_id
    kw['py_group_id'] = py_group_id
    kw['start_date'] = start_date
    kw['end_date'] = end_date
    kw['dept_cd'] = dept_cd
    kw['has_child_dept'] = has_child_dept
    kw['tree_lvl'] = tree_lvl

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)
    try:
        if (py_cal_id is not None) or ((start_date is not None) and (end_date is not None)):
            result = task_handle.apply_async(kwargs=kw, queue='SHARE_Q', retry=False)
            if result.task_id is not None:
                return make_response(jsonify({'status': 'OK', 'task_id':result.task_id}))
        else:
            return make_response(jsonify(status='ERROR'))
    except Exception:
        return make_response(jsonify(status='ERROR'))
