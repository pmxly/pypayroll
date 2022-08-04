# coding:utf-8
"""
运行过程处理函数
"""
from .run_type import RunType
from ..pyformulas import create_formula as formula
from ..pysysutils import global_variables as gv
from importlib import import_module
from copy import deepcopy
from ..pypinaccum.pinaccum import get_accumulate_in_dev, get_new_accumulate
from ..pysysutils.py_calc_log import ins_log_tree, pop_node_list


def load_run_type_elements(process_child):
    """
    根据运行类型加载运行类型元素
    :param process_child:原型类型元素
    :return:
    """
    if process_child.chi_type == "FM":
        """实例化公式对象"""
        process_child.element_entity = formula.create(process_child.tenant_id, process_child.chi_id)
        # formula.validate_pins(process_child.element_entity)
    elif process_child.chi_type == "FC":
        """实例化函数对象"""
        module_meta = import_module('hhr.payroll.pyfunctions.' + process_child.chi_id)
        class_meta = getattr(module_meta, 'PyFunction')
        process_child.element_entity = class_meta()
    elif process_child.chi_type == "WT":
        """实例化薪资项目"""
        pin_dic = gv.get_pin_dic()
        if process_child.chi_id in pin_dic:
            process_child.element_entity = pin_dic[process_child.chi_id]
    elif process_child.chi_type == "WC":
        """实例化薪资项目累计"""
        pg_currency = gv.get_var_value('VR_PG_CURRENCY')
        f_end_date = gv.get_var_value('VR_F_PERIOD_END')
        log_flag = gv.get_run_var_value('LOG_FLAG')
        pre_log_flag = gv.get_run_var_value('PRE_LOG_FLAG')
        # 如果当前过程日志开关与上一个人员的过程日志开关不一致，则生成新的累计对象
        if log_flag != pre_log_flag:
            pin_acc = get_new_accumulate(process_child.tenant_id, process_child.chi_id, process_child.country, pg_currency, f_end_date)
        else:
            pin_acc = get_accumulate_in_dev(process_child.tenant_id, process_child.chi_id, process_child.country, pg_currency, f_end_date)

        new_pin_acc = deepcopy(pin_acc)

        for child in new_pin_acc.pins:
            if gv.pin_in_dic(child.pin_code):
                if not child.variable_id:
                    child.is_valid = True
                elif gv.get_var_value(child.variable_id) == 1:
                    child.is_valid = True
        process_child.element_entity = new_pin_acc


def exec_run_type(tenant_id, run_type_id):
    """
    运行过程处理函数
    :param tenant_id:租户ID
    :param run_type_id:运行类型ID
    :return:
    """

    catalog = gv.get_run_var_value('PY_CATALOG')
    cal_obj = gv.get_run_var_value('CUR_CAL_OBJ')

    f_bgn_date = catalog.f_prd_bgn_dt
    f_end_date = catalog.f_prd_end_dt

    run_type_dic = gv.get_run_var_value('RUN_TYPE_DIC')
    if run_type_dic is None:
        run_type_dic = {}
    if run_type_id not in run_type_dic:
        run_type = RunType(tenant_id, run_type_id)
        run_type_dic[run_type_id] = run_type
        gv.set_run_var_value('RUN_TYPE_DIC', run_type_dic)
    else:
        run_type = run_type_dic[run_type_id]
    # 将当前日历记录到过程日志树
    log_flag = gv.get_run_var_value('LOG_FLAG')
    if log_flag == 'Y':
        tree_node = cal_obj.cal_id
        ins_log_tree(tree_node, 'CAL', cal_obj.description)

    for run_type_child in run_type.data:
        if run_type_child.active == 'A':
            run_process = run_type_child.run_process

            if log_flag == 'Y':
                tree_node = run_process.id
                ins_log_tree(tree_node, 'PRC', run_process.description)

            for element in run_process.data:
                if (f_bgn_date <= element.end_date
                        and f_end_date >= element.start_date):

                    process_element = deepcopy(element)
                    load_run_type_elements(process_element)

                    # if log_flag == 'Y':
                    #     elem_id = element.chi_id
                    #     elem_type = element.chi_type
                    #     elem_desc = ''
                    #     if elem_type == 'FM' or elem_type == 'FC':
                    #         elem_desc = process_element.desc
                    #     elif elem_type == 'WC' or elem_type == 'WT':
                    #         elem_desc = process_element.description
                    #     ins_log_tree(elem_id, elem_type, elem_desc)

                    process_element.element_exec()

                    # 弹出过程元素节点
                    # pop_node_list()

            # 弹出当前运行过程节点，以处理下一个运行过程
            pop_node_list()

    # 弹出日历节点
    pop_node_list()
