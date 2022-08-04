from flask import jsonify, request, make_response
from datetime import datetime
from ...taskutils import get_tasks_map


def call_py_calc():
    from ...payroll.pyexecute.pymain import schedule_py_cal_prcs
    from ...payroll.pysysutils.func_lib_01 import ins_py_cal_task, del_py_cal_task, get_paygrp_by_cal_id
    from ...payroll.pysysutils.func_lib_01 import update_run_status, del_payee_calc_stat
    from ...payroll.pysysutils import global_variables as gv
    gv.init_flask()

    db = gv.get_flask_db()

    request_data = request.get_json(force=True)

    user_id = request_data.get("user")
    tenant_id = request_data.get("tenant_id")
    cal_id = request_data.get("cal_id")

    # 日历组ID和日历ID相同
    cal_grp_id = cal_id
    run_ctl_id = str(tenant_id) + '~' + cal_id + '~' + (datetime.now().strftime('%Y%m%d%H%M%S'))

    identify_flag = re_iden_flag = py_calc_flag = cancel_iden_flag = re_calc_flag = re_iden_calc_flag \
        = finish_flag = cancel_calc_flag = cancel_flag = close_flag = ''

    py_action = request_data.get("action")
    if py_action == 'IDENTIFY':
        identify_flag = 'Y'
    elif py_action == 'RE_IDEN':
        re_iden_flag = 'Y'
    elif py_action == 'PY_CALC':
        py_calc_flag = 'Y'
    elif py_action == 'CANCEL_IDEN':
        cancel_iden_flag = 'Y'
    elif py_action == 'RE_CALC':
        re_calc_flag = 'Y'
    elif py_action == 'RE_IDEN_CALC':
        re_iden_calc_flag = 'Y'
    elif py_action == 'FINISH':
        finish_flag = 'Y'
    elif py_action == 'CANCEL_CALC':
        cancel_calc_flag = 'Y'
    elif py_action == 'CANCEL_BOTH':
        cancel_flag = 'Y'
    elif py_action == 'CLOSE':
        close_flag = 'Y'
    else:
        return make_response(jsonify(status='ERROR', message='action is not supported!'))

    # 全局过程日志开关
    log_flag = request_data.get("log_flag")

    pay_group_id = get_paygrp_by_cal_id(db, tenant_id, cal_id)

    run_param = {'operator_user_id': user_id, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id,
                 'pay_group_id': pay_group_id,
                 'cal_id': cal_id, 'run_ctl_id': run_ctl_id, 'identify_emp_flag': identify_flag,
                 're_iden_flag': re_iden_flag, 'py_calculate_flag': py_calc_flag, 're_calc_flag': re_calc_flag,
                 're_iden_calc_flag': re_iden_calc_flag, 'cancel_iden_flag': cancel_iden_flag,
                 'cancel_calc_flag': cancel_calc_flag,
                 'cancel_flag': cancel_flag, 'finish_flag': finish_flag, 'close_flag': close_flag,
                 'log_flag': log_flag}
    # 需要记录过程日志的任职记录串
    log_emp_rec_str = request_data.get("log_emp_rec_str")
    run_param['log_emp_rec_str'] = log_emp_rec_str

    # 重新标记&计算标志
    if re_iden_calc_flag == 'Y':
        re_iden_flag = 'Y'
        run_param['re_iden_flag'] = re_iden_flag
        re_calc_flag = 'Y'
        run_param['re_calc_flag'] = re_calc_flag

    # 标记人员 或 重新标记
    if identify_flag == 'Y' or re_iden_flag == 'Y':
        task_name = "pyidentify"
        run_param['task_name'] = task_name
        kw = dict()
        kw['tenant_id'] = tenant_id
        kw['user_id'] = user_id
        kw['run_ctl'] = run_ctl_id
        kw['run_param'] = run_param

        tasks_router = get_tasks_map()
        task_handle = tasks_router.get(run_param['task_name'], None)
        # trans = db.conn.begin()
        try:
            if cal_grp_id is not None:
                if pay_group_id in ['CA', 'CM', 'CC']:
                    queue = 'DEDICATED_Q'
                else:
                    queue = 'SHARE_Q'
                result = task_handle.apply_async(kwargs=kw, queue=queue, retry=False)
                if result.task_id is not None:
                    del_py_cal_task(db, tenant_id, cal_grp_id, cal_id)
                    ins_py_cal_task(db, tenant_id, cal_grp_id, cal_id, run_ctl_id, task_name, user_id, 1)
                    # trans.commit()
                    db.conn.close()
                    return make_response(jsonify(status='OK'))
            else:
                # trans.rollback()
                db.conn.close()
                return make_response(jsonify(status='ERROR'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))

    # 薪资计算
    elif py_calc_flag == 'Y':
        task_name = "pycalc"

        # trans = db.conn.begin()
        try:
            # 删除薪资计算运行控制ID日志记录
            del_py_cal_task(db, tenant_id, cal_grp_id, cal_id, task_name)

            # 删除标记/计算结果、标记/计算消息表中当前日历对应的记录（仅薪资计算记录）
            del_payee_calc_stat(db, tenant_id, cal_grp_id, user_id, 'PAY', run_ctl_id=run_ctl_id)
            # 删除人员薪资计算结果（多个表）/ 更新薪资计算目录
            # del_py_calc_rslt(db, tenant_id, cal_grp_id, user_id)
            # 删除人员薪资计算过程日志
            # del_py_calc_log(db, tenant_id, cal_grp_id)
            # 将人员追溯触发器表中“处理日历组”为当前日历组的“触发器状态”还原为U（未处理），并清空处理日历组
            # u = text(
            #     "update HHR_PY_RTO_TRGR set HHR_TRGR_STATUS = 'U', HHR_PYCALGRP_ID = '' where tenant_id = :b1 and HHR_PYCALGRP_ID = :b2 ")
            # db.conn.execute(u, b1=tenant_id, b2=cal_grp_id)

            run_param['task_name'] = task_name

            # 获取待处理人员列表
            run_param['payees'] = []
            prcs_num = schedule_py_cal_prcs(run_param)
            ins_py_cal_task(db, tenant_id, cal_grp_id, cal_id, run_ctl_id, task_name, user_id, prcs_num)

            # trans.commit()
            db.conn.close()
            return make_response(jsonify(status='OK'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))

    # 重新计算
    elif re_calc_flag == 'Y':
        emp_rec_str = request_data.get("emp_rec_str")
        run_param['emp_rec_str'] = emp_rec_str
        task_name = "pycalc"

        # trans = db.conn.begin()
        try:
            # 删除薪资计算运行控制ID日志记录
            del_py_cal_task(db, tenant_id, cal_grp_id, cal_id, task_name)

            # 删除标记/计算结果、标记/计算消息表中当前日历对应的记录（仅薪资计算记录）
            del_payee_calc_stat(db, tenant_id, cal_grp_id, user_id, 'PAY', run_ctl_id=run_ctl_id,
                                emp_rec_str=emp_rec_str)
            # 删除人员薪资计算结果（多个表）/ 更新薪资计算目录
            # del_py_calc_rslt(db, tenant_id, cal_grp_id, user_id)
            # 删除人员薪资计算过程日志
            # del_py_calc_log(db, tenant_id, cal_grp_id)
            # 将人员追溯触发器表中“处理日历组”为当前日历组的“触发器状态”还原为U（未处理），并清空处理日历组
            # u = text(
            #     "update HHR_PY_RTO_TRGR set HHR_TRGR_STATUS = 'U', HHR_PYCALGRP_ID = '' where tenant_id = :b1 and HHR_PYCALGRP_ID = :b2 ")
            # db.conn.execute(u, b1=tenant_id, b2=cal_grp_id)

            run_param['task_name'] = task_name

            # 获取待处理人员列表
            run_param['payees'] = []
            prcs_num = schedule_py_cal_prcs(run_param)
            ins_py_cal_task(db, tenant_id, cal_grp_id, cal_id, run_ctl_id, task_name, user_id, prcs_num)

            # trans.commit()
            db.conn.close()
            return make_response(jsonify(status='OK'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))

    # 取消标记
    elif cancel_iden_flag == 'Y':
        # trans = db.conn.begin()
        try:
            del_py_cal_task(db, tenant_id, cal_grp_id, cal_id)

            val = {'db': db, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id, 'action_flag': 'CANCEL_IDEN',
                   'action_user': user_id}
            update_run_status(**val)
            # trans.commit()
            db.conn.close()
            return make_response(jsonify(status='OK'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))
    # 取消计算
    elif cancel_calc_flag == 'Y':
        # trans = db.conn.begin()
        try:
            del_py_cal_task(db, tenant_id, cal_grp_id, cal_id, "pycalc")

            val = {'db': db, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id, 'action_flag': 'CANCEL_CALC',
                   'action_user': user_id}
            update_run_status(**val)
            # trans.commit()
            db.conn.close()
            return make_response(jsonify(status='OK'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))
    # 取消标记&计算
    elif cancel_flag == 'Y':
        # trans = db.conn.begin()
        try:
            del_py_cal_task(db, tenant_id, cal_grp_id, cal_id)

            val = {'db': db, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id, 'action_flag': 'CANCEL_BOTH',
                   'action_user': user_id}
            update_run_status(**val)
            # trans.commit()
            db.conn.close()
            return make_response(jsonify(status='OK'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))
    # 完成计算
    elif finish_flag == 'Y':
        # trans = db.conn.begin()
        try:
            val = {'db': db, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id, 'action_flag': 'FINISH',
                   'action_user': user_id}
            update_run_status(**val)
            # trans.commit()
            db.conn.close()
            return make_response(jsonify(status='OK'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))
    # 关闭计算
    elif close_flag == 'Y':
        # trans = db.conn.begin()
        try:
            val = {'db': db, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id, 'action_flag': 'CLOSE',
                   'action_user': user_id}
            update_run_status(**val)
            # trans.commit()
            db.conn.close()
            return make_response(jsonify(status='OK'))
        except Exception:
            # trans.rollback()
            db.conn.close()
            return make_response(jsonify(status='ERROR'))


def exec_verify_formula():
    from ...payroll.pyformulas.formula_verify.verify_formula import verify_formula

    request_data = request.get_json(force=True)
    tenant_id = request_data.get("tenant_id")
    formula_id = request_data.get("formula_id")

    try:
        verify_formula(tenant_id, formula_id)
        return make_response(jsonify(status='OK'))
    except Exception as e:
        return make_response(jsonify({'status': 'ERROR', 'message': repr(e)}))
