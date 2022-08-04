from flask import jsonify, request, make_response
from datetime import datetime
from ...taskutils import get_tasks_map


def call_pers_trans_rpt():
    """调用生成人事异动情况表进程"""

    request_data = request.get_json(force=True)

    task_name = "pers_trans_rpt"
    user_id = request_data.get("user")
    run_ctl_id = 'pers_trans_rpt_' + (datetime.now().strftime('%Y%m%d%H%M%S'))
    tenant_id = request_data.get("tenant_id")
    lang = request_data.get("lang")
    start_date = request_data.get("start_date")
    end_date = request_data.get("end_date")
    dept_cd = request_data.get("dept_cd")
    has_child_dept = request_data.get("has_child_dept")
    company = request_data.get("company")
    hr_sub_range = request_data.get("hr_sub_range")
    emp_class = request_data.get("emp_class")
    emp_sub_class = request_data.get("emp_sub_class")

    kw = dict()
    kw['user_id'] = user_id
    kw['run_ctl'] = run_ctl_id
    kw['tenant_id'] = tenant_id
    kw['lang'] = lang
    kw['start_date'] = start_date
    kw['end_date'] = end_date
    kw['dept_cd'] = dept_cd
    kw['has_child_dept'] = has_child_dept
    kw['company'] = company
    kw['hr_sub_range'] = hr_sub_range
    kw['emp_class'] = emp_class
    kw['emp_sub_class'] = emp_sub_class

    tasks_router = get_tasks_map()
    task_handle = tasks_router.get(task_name, None)
    try:
        if (start_date is not None) and (end_date is not None):
            result = task_handle.apply_async(kwargs=kw, queue='SHARE_Q', retry=False)
            if result.task_id is not None:
                return make_response(jsonify(status='OK'))
        else:
            return make_response(jsonify(status='ERROR'))
    except Exception:
        return make_response(jsonify(status='ERROR'))
