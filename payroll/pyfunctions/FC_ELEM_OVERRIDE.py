# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from sqlalchemy.sql import text
from ..pysysutils.py_calc_log import add_fc_log_item
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 支持元素覆盖函数
    Author: David
    Date: 2018/09/06
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_ELEM_OVERRIDE'
        self.country = 'CHN'
        self.desc = '支持元素覆盖函数'
        self.descENG = 'Support element override function'
        self.func_type = 'B'  # 无返回值
        self.instructions = '支持元素覆盖函数，无需传入参数。'
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        # seq_num = catalog.seq_num
        f_cal_id = catalog.f_cal_id

        if catalog.new_entry_flg == 'Y':
            f_cal_id = ''

        f_end_date = catalog.f_prd_end_dt

        # 根据人员编码、任职记录号、历经期日历编码从支持元素覆盖表中取值
        temp_lst = []
        s1 = "select '1', hhr_start_dt, hhr_element_id, hhr_elem_val_num, hhr_elem_val_char, hhr_elem_val_dt from " \
             "boogoo_payroll.hhr_py_pye_sovr where tenant_id = :b1 " \
             "and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_element_type = 'VR' and (hhr_py_cal_id <> '' and hhr_py_cal_id is not Null) and hhr_py_cal_id = :b4  " \
             "union select '3', hhr_start_dt, hhr_element_id, hhr_elem_val_num, hhr_elem_val_char, hhr_elem_val_dt " \
             "from boogoo_payroll.hhr_py_pye_sovr where tenant_id = :b1 " \
             "and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_element_type = 'VR' and hhr_start_dt <= :b5 and " \
             "(hhr_end_dt >= :b5 or hhr_end_dt is null) and (hhr_py_cal_id = '' or hhr_py_cal_id is Null) "
        order_by = " order by 1 "

        # 如果当前期间是更正类型，还需要合并被更正日历上的支持元素（同一个支持元素取数优先级：更正日历、被更正日历、不带日历的）
        upd_cal_id = gv.get_run_var_value('UPD_CAL_ID')
        if upd_cal_id:
            s2 = "union select '2', hhr_start_dt, hhr_element_id, hhr_elem_val_num, hhr_elem_val_char, hhr_elem_val_dt from " \
                 "boogoo_payroll.hhr_py_pye_sovr where tenant_id = :b1 " \
                 "and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_element_type = 'VR' and hhr_py_cal_id = :b6 "
            stmt = s1 + s2 + order_by
        else:
            stmt = s1 + order_by

        rs = db.conn.execute(text(stmt), b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=f_cal_id, b5=f_end_date, b6=upd_cal_id).fetchall()
        for row in rs:
            elem_id = row['hhr_element_id']
            if elem_id not in temp_lst:
                temp_lst.append(elem_id)
            else:
                continue
            elem_val_num = row['hhr_elem_val_num']
            elem_val_char = row['hhr_elem_val_char']
            elem_val_dt = row['hhr_elem_val_dt']
            var_obj = gv.get_var_obj(elem_id)
            if var_obj is None:
                print('变量' + str(elem_id) + '不存在')
                continue
            var_type = var_obj.data_type
            elem_val = None
            if var_type == 'datetime':
                elem_val = elem_val_dt
            elif var_type == 'float':
                elem_val = elem_val_num
            elif var_type == 'string':
                elem_val = elem_val_char

            if var_obj is not None:
                var_obj.value = elem_val
                var_obj.has_covered = 'Y'

            add_fc_log_item(self, 'VR', elem_id)

            # 变量数据存入表
            # keys = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd,
            #         'seq_num': seq_num, 'var_id': elem_id, 'action': 'U'}
            # Var(**keys).update(HHR_VARVAL_CHAR=elem_val_char, HHR_VARVAL_DATE=elem_val_dt,
            #                    HHR_VARVAL_DEC=elem_val_num, HHR_PRCS_FLAG='C')
