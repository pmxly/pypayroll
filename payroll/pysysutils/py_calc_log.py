# coding:utf-8
"""
薪资计算过程日志工具函数
create by David on 2018/11/30
"""

from ..pysysutils import global_variables as gv
from sqlalchemy.sql import text
from ..pyexecute.pycalculate.table.wt_log import WTLog
from ..pyexecute.pycalculate.table.wc_log import WCLog
from ..pyexecute.pycalculate.table.vr_log import VRLog
from ..pyexecute.pycalculate.table.fc_param_log import ParamLog

import datetime


# def log(**kw):
#     """记录薪资计算过程日志"""
#
#     def _log(func):
#         # log_record = gv.get_run_var_value('LOGGER')
#
#         def __log_active(*args, **kwargs):
#             # 处理前
#             trace_dic = args[0].trace_dic
#             trace_dic['option'] = 'IN'
#             ins_elem_log_data(**trace_dic)
#
#             # 处理中
#             ret = func(*args, **kwargs)
#
#             # 处理后
#             trace_dic['option'] = 'OUT'
#             trace_dic['args'] = args
#             ins_elem_log_data(**trace_dic)
#
#             return ret
#
#         def __log_inactive(*args, **kwargs):
#             # try:
#             #     print('-----current 累计 id: ' + args[0].acc_code)
#             # except AttributeError:
#             #     try:
#             #         print('-----current element id: ' + args[0].id)
#             #     except AttributeError:
#             #         print('-----current 薪资项目 id: ' + args[0].pin_id)
#
#             print('*******func********' + str(func))
#             ret = func(*args, **kwargs)
#             return ret
#
#         if gv.get_run_var_value('LOG_FLAG') == 'Y':
#             return __log_active
#         else:
#             return __log_inactive
#
#     return _log


def log(**kw):
    """记录薪资计算过程日志"""

    if gv.get_run_var_value('LOG_FLAG') == 'Y':
        # print('========_log_enable=========' + str(gv.get_run_var_value('LOG_FLAG')))
        def _log_enable(func):

            def __log_active(*args, **kwargs):
                # 处理前
                trace_dic = args[0].trace_dic
                trace_dic['option'] = 'IN'
                ins_elem_log_data(**trace_dic)

                # 处理中
                ret = func(*args, **kwargs)
                # 处理后
                trace_dic['option'] = 'OUT'
                trace_dic['args'] = args
                ins_elem_log_data(**trace_dic)
                return ret
            return __log_active
        return _log_enable
    else:
        def _log_disable(func):
            # print('========_log_disable=========' + str(gv.get_run_var_value('LOG_FLAG')))
            def __log_inactive(*args, **kwargs):
                # starttime = datetime.datetime.now()
                # try:
                #     print('-----current 累计 id: ' + args[0].acc_code)
                # except AttributeError:
                #     try:
                #         print('-----current element id: ' + args[0].id)
                #     except AttributeError:
                #         print('-----current 薪资项目 id: ' + args[0].pin_id)
                #
                # print('*******func********' + str(func))
                ret = func(*args, **kwargs)
                # endtime = datetime.datetime.now()
                # print("=====耗时=======" + str((endtime - starttime).microseconds))
                # gv.set_run_var_value('LOG_TREE_CAL_NUM', int((endtime - starttime).microseconds) + gv.get_run_var_value('LOG_TREE_CAL_NUM'))
                # print("=====累计花费时间=======" + str(gv.get_run_var_value('LOG_TREE_CAL_NUM')))
                return ret
            return __log_inactive
        return _log_disable


def pop_node_list():
    log_flag = gv.get_run_var_value('LOG_FLAG')
    if log_flag == 'Y':
        node_lst = gv.get_run_var_value('LOG_TREE_NODE_LIST')
        node_lst.pop()


def ins_log_tree(tree_node, node_type, node_desc):
    """
    Created by David on 2018/11/30
    插入过程日志树
    :param tree_node: 树节点名称
    :param node_type: 节点类型(CAL-日历, PRC-运行过程, FM-公式, FC-函数, WC-薪资项目累计,
                               WT-薪资项目, VR-变量, PA-参数, IN-输入, OUT-输出)
    :param node_desc: 节点描述
    :return: 插入节点的编号
    """

    catalog = gv.get_run_var_value('PY_CATALOG')
    cur_cal_id = catalog.f_cal_id
    cal_id = catalog.cal_id
    tenant_id = catalog.tenant_id
    emp_id = catalog.emp_id
    emp_rcd = catalog.emp_rcd
    seq_num = catalog.seq_num

    node_lst = gv.get_run_var_value('LOG_TREE_NODE_LIST')
    tree_node_num = gv.get_run_var_value('LOG_TREE_NEXT_NUM')

    if node_type == 'CAL':
        p_node_num = None
    else:
        p_node_num = node_lst[len(node_lst) - 1]

    db = gv.get_db()
    log_sql = text("insert into boogoo_payroll.hhr_py_log_tree(tenant_id, hhr_empid, hhr_emp_rcd, hhr_seq_num, hhr_tree_node_num, "
                   "hhr_tree_node, hhr_description, hhr_tree_node_type, hhr_p_node_num, hhr_f_cal_id, hhr_py_cal_id) "
                   "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10, :b11) ")
    db.conn.execute(log_sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=tree_node_num,
                    b6=tree_node, b7=node_desc, b8=node_type, b9=p_node_num, b10=cur_cal_id, b11=cal_id)

    gv.set_run_var_value('LOG_TREE_NEXT_NUM', tree_node_num + 10)
    node_lst.append(tree_node_num)
    return tree_node_num


def add_fc_log_item(fc_obj, item_type, item_id, item_data=None):
    """
    添加函数中需要记录日志的WT、WC、VR
    :param fc_obj: 函数对象
    :param item_type: 条目类型(WT/WC/VR)
    :param item_id: 条目ID
    :param item_data: 条目数据（字典/元组/列表）
    :return: None
    """

    log_flag = gv.get_run_var_value('LOG_FLAG')
    if log_flag == 'Y':
        item_lst = fc_obj.trace_dic[item_type]
        if item_id != '':
            if item_id not in item_lst:
                item_lst.append(item_id)
        elif fc_obj.id == 'FC_BR':
            for tu in item_data:
                item_id = tu[1]
                if item_id not in item_lst:
                    fc_obj.trace_dic[item_type].append(item_id)
        elif type(item_data) is list:
            for item_id in item_data:
                if item_id not in item_lst:
                    fc_obj.trace_dic[item_type].append(item_id)
        elif (item_data is not None) and (len(item_data) > 0):
            for k in item_data.keys():
                if k not in item_lst:
                    fc_obj.trace_dic[item_type].append(k)


def set_var_node_data(var_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option):
    """添加变量日志节点和数据"""

    for var_id in var_lst:
        var_obj = gv.get_var_obj(var_id)
        if var_obj is None:
            continue

        # 将变量添加到日志树结构中
        vr_node_num = ins_log_tree(var_id, 'VR', var_obj.desc)
        # 插入节点后需要将其从节点列表中弹出，返回上一级
        pop_node_list()

        # 插入变量日志数据表
        var_type = var_obj.data_type
        prc_flag = var_obj.has_covered
        var_val_char = var_dt = var_val_dec = None
        if var_type == 'string':
            var_val_char = var_obj.value
            var_dt = None
            var_val_dec = 0
        elif var_type == 'datetime':
            var_val_char = ''
            var_dt = var_obj.value
            var_val_dec = 0
        elif var_type == 'float':
            var_val_char = ''
            var_dt = None
            var_val_dec = var_obj.value

        v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                 'tree_node_num': vr_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id, 'in_out_flag': option,
                 'var_id': var_id, 'var_type': var_type, 'var_val_char': var_val_char,
                 'var_dt': var_dt, 'var_val_dec': var_val_dec, 'prc_flag': prc_flag}
        VRLog(**v_dic).insert()


def set_wt_node_data(pin_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option):
    """添加薪资项目日志节点和数据"""

    for pin_cd in pin_lst:
        if pin_cd not in gv.get_pin_dic():
            continue
        pin_obj = gv.get_pin(pin_cd)

        # 将薪资项目添加到日志树结构中
        wt_node_num = ins_log_tree(pin_cd, 'WT', pin_obj.description)
        pop_node_list()
        # 插入薪资项目日志数据表
        segment = pin_obj.segment
        for seg in segment.values():
            v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                     'tree_node_num': wt_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id, 'in_out_flag': option, 'seg': seg}
            WTLog(**v_dic).insert()


def set_wc_node_data(pin_acc_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option):
    """添加薪资项目累计日志节点和数据"""

    for acc_cd in pin_acc_lst:
        if acc_cd not in gv.get_pin_acc_dic():
            continue
        wc_obj = gv.get_pin_acc(acc_cd)
        acc_type = wc_obj.acc_type
        acc_desc = wc_obj.description

        # 1.将薪资项目累计节点插入到日志树结构中
        wc_node_num = ins_log_tree(acc_cd, 'WC', acc_desc)

        # 插入薪资项目累计日志数据表
        if acc_type != 'P':
            seg = wc_obj.segment['*']
            pg_currency = wc_obj.pg_currency

            v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id,
                     'emp_rcd': emp_rcd, 'seq_num': seq_num, 'tree_node_num': wc_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id,
                     'in_out_flag': option, 'acc_cd': wc_obj.acc_code, 'acc_type': wc_obj.acc_type, 'add_year': wc_obj.acc_year_seq[0],
                     'add_num': wc_obj.acc_year_seq[1], 'amt': seg.amt, 'currency': pg_currency,
                     'quantity': seg.quantity, 'quantity_unit': seg.quantity_unit}
            WCLog(**v_dic).insert()
        elif acc_type:
            segment = wc_obj.segment
            for seg_num, seg in segment.items():
                v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                         'tree_node_num': wc_node_num, 'segment_num': seg_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id,
                         'in_out_flag': option, 'seg': seg}
                WTLog(**v_dic).insert()

        # 记录薪资项目累计中使用的薪资项目的数据
        pins = wc_obj.pins
        for pin_acc_child in pins:
            if pin_acc_child.is_valid:
                pin_cd = pin_acc_child.pin_code
                pin_obj = gv.get_pin(pin_cd)
                # 将薪资项目添加到日志树结构中
                wt_node_num = ins_log_tree(pin_cd, 'WT', pin_obj.description)
                pop_node_list()
                # 插入薪资项目日志数据表
                segment = pin_obj.segment
                for seg in segment.values():
                    v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                             'tree_node_num': wt_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id, 'in_out_flag': option, 'seg': seg}

                    WTLog(**v_dic).insert()

        # 2.弹出该薪资项目累计节点
        pop_node_list()


def process_wc_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic):
    """处理薪资项目累计的过程日志"""

    elem_id = trace_dic['id']
    elem_desc = trace_dic['desc']
    elem_type = trace_dic['type']
    option = trace_dic['option']

    wc_obj = trace_dic['wc_obj']
    acc_type = wc_obj.acc_type

    # 薪资项目累计不记录处理之前的数据
    if option == 'IN':
        # 先将当前薪资项目累计节点插入到日志树结构中
        ins_log_tree(elem_id, elem_type, elem_desc)
    elif option == 'OUT':
        wc_node_num = node_lst[len(node_lst) - 1]
        if acc_type != 'P':
            seg = wc_obj.segment['*']
            pg_currency = wc_obj.pg_currency

            v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id,
                     'emp_rcd': emp_rcd, 'seq_num': seq_num, 'tree_node_num': wc_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id,
                     'in_out_flag': option, 'acc_cd': wc_obj.acc_code, 'acc_type': wc_obj.acc_type, 'add_year': wc_obj.acc_year_seq[0],
                     'add_num': wc_obj.acc_year_seq[1], 'amt': seg.amt, 'currency': pg_currency,
                     'quantity': seg.quantity, 'quantity_unit': seg.quantity_unit}
            WCLog(**v_dic).insert()
        elif acc_type == 'P':
            segment = wc_obj.segment
            for seg_num, seg in segment.items():
                v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                         'tree_node_num': wc_node_num, 'segment_num': seg_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id,
                         'in_out_flag': option, 'seg': seg}
                WTLog(**v_dic).insert()

        # 记录薪资项目累计中使用的薪资项目的数据
        pins = wc_obj.pins
        for pin_acc_child in pins:
            if pin_acc_child.is_valid:
                pin_cd = pin_acc_child.pin_code
                pin_obj = gv.get_pin(pin_cd)
                # 将薪资项目添加到日志树结构中
                wt_node_num = ins_log_tree(pin_cd, 'WT', pin_obj.description)
                # 插入节点后需要将其从节点列表中弹出，确保当前父节点为该WC，以处理下一个薪资项目
                pop_node_list()

                # 插入薪资项目日志数据表
                segment = pin_obj.segment
                for seg in segment.values():
                    v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                             'tree_node_num': wt_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id, 'in_out_flag': option, 'seg': seg}

                    WTLog(**v_dic).insert()

        # 处理完该元素计算之后，从节点列表中弹出薪资项目累计节点
        pop_node_list()


def process_wt_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic):
    """处理带公式的薪资项目的过程日志"""

    elem_id = trace_dic['id']
    elem_desc = trace_dic['desc']
    elem_type = trace_dic['type']
    option = trace_dic['option']

    # 带公式的薪资项目不记录处理之前的数据
    if option == 'IN':
        # 先将当前薪资项目节点插入到日志树结构中
        ins_log_tree(elem_id, elem_type, elem_desc)
    elif option == 'OUT':
        wt_node_num = node_lst[len(node_lst) - 1]
        wt_obj = trace_dic['wt_obj']
        if wt_obj.formula_id:
            # 插入日志数据表
            segment = wt_obj.segment
            for seg in segment.values():
                v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                         'tree_node_num': wt_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id, 'in_out_flag': option, 'seg': seg}

                WTLog(**v_dic).insert()
        pop_node_list()


def process_fm_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic):
    """处理公式的过程日志"""

    elem_id = trace_dic['id']
    elem_desc = trace_dic['desc']
    elem_type = trace_dic['type']
    option = trace_dic['option']

    fm_obj = trace_dic['fm_obj']
    var_lst = fm_obj.variable_list
    pin_lst = fm_obj.pin_list
    pin_acc_lst = fm_obj.pin_acc_list

    if option == 'IN':
        # 先将当前公式节点插入到日志树结构中
        ins_log_tree(elem_id, elem_type, elem_desc)

        # 插入输入节点
        ins_log_tree('fm_in', 'IN', '输入')

    elif option == 'OUT':
        # 弹出输入节点
        pop_node_list()

        # 插入输出节点
        ins_log_tree('fm_out', 'OUT', '输出')

    # 记录公式中使用的变量VR
    set_var_node_data(var_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option)

    # 记录公式中使用的薪资项目WT
    set_wt_node_data(pin_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option)

    # 记录公式中使用的薪资项目累计WC
    set_wc_node_data(pin_acc_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option)

    # 公式处理完之后需要从节点列表中弹出该公式节点
    if option == 'OUT':
        # 弹出输出节点
        pop_node_list()
        # 弹出该公式节点
        pop_node_list()


def process_fc_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic):
    """处理函数的过程日志"""

    elem_id = trace_dic['id']
    elem_desc = trace_dic['desc']
    elem_type = trace_dic['type']
    option = trace_dic['option']

    # 函数不记录处理之前的数据
    if option == 'IN':
        # 先将当前函数节点插入到日志树结构中
        ins_log_tree(elem_id, elem_type, elem_desc)
    elif option == 'OUT':
        args = trace_dic['args']
        param_lst = trace_dic['PA']
        # 如果函数有参数(除self之外)，则插入参数节点
        if len(args) > 1 and (len(param_lst) >= 1):
            ins_log_tree('fc_param', 'PAR', '参数')
            # 以字典形式组合参数名和参数值
            param_dic = dict(zip(param_lst, args[1:]))
            for pa_name, pa_val in param_dic.items():
                # 插入参数节点
                pa_node_num = ins_log_tree('fc_param', 'PA', pa_name)
                pop_node_list()
                # 插入参数数据
                if pa_val is None:
                    pa_val = ''
                v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                         'tree_node_num': pa_node_num, 'cal_id': cal_id, 'cur_cal_id': cur_cal_id, 'in_out_flag': option,
                         'param_id': pa_name, 'param_val': str(pa_val)}

                ParamLog(**v_dic).insert()
            pop_node_list()

        var_lst = trace_dic['VR']
        if len(var_lst) > 0:
            set_var_node_data(var_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option)
        pin_lst = trace_dic['WT']
        if len(pin_lst) > 0:
            set_wt_node_data(pin_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option)
        pin_acc_lst = trace_dic['WC']
        if len(pin_acc_lst) > 0:
            set_wc_node_data(pin_acc_lst, tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, option)

        # 弹出该函数节点
        pop_node_list()


def ins_elem_log_data(**trace_dic):
    """
    Created by David on 2018/11/30
    插入运行过程子元素处理的薪资项目累计、薪资项目、函数、变量(WC/WT/FC/VR)
    :return:
    """

    catalog = gv.get_run_var_value('PY_CATALOG')
    node_lst = gv.get_run_var_value('LOG_TREE_NODE_LIST')

    tenant_id = catalog.tenant_id
    emp_id = catalog.emp_id
    emp_rcd = catalog.emp_rcd
    seq_num = catalog.seq_num

    cur_cal_id = catalog.f_cal_id
    cal_id = catalog.cal_id

    elem_type = trace_dic['type']

    # 如果当前执行的是薪资项目累计
    if elem_type == 'WC':
        process_wc_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic)

    # 如果当前执行的是带公式的薪资项目
    elif elem_type == 'WT':
        process_wt_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic)

    # 如果当前执行的是公式
    elif elem_type == 'FM':
        process_fm_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic)

    # 如果当前执行的是函数
    elif elem_type == 'FC':
        process_fc_log(tenant_id, emp_id, emp_rcd, seq_num, cal_id, cur_cal_id, node_lst, trace_dic)
