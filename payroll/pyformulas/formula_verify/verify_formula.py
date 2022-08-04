# coding:utf-8
from ....dbengine import DataBaseAlchemy
from copy import deepcopy
from sqlalchemy import select
from sqlalchemy import text
from decimal import Decimal
from .mock_function import *
from .mock_var import get_variable_dic
from .mock_pin import get_pin_dic
from .mock_acc import get_pin_acc_dic
from .mock_function import gv
from datetime import timedelta

one_day = timedelta(days=1)

db = DataBaseAlchemy()


def get_formula(tenant_id, formula_id):
    class_string_m = """
class FormulaObject():

    def __init__(self):
        self.tenant_id = ''
        # 公式ID
        self.id = ''
    
    def formula_mock_exec(self):
        var = get_variable_dic(self.tenant_id)
        pin_dic = get_pin_dic(self.tenant_id)
        pin_acc_dic = get_pin_acc_dic(self.tenant_id)
        # print('current formula id:' + self.id)
        %1

class_meta=FormulaObject()
    """

    conn = db.conn
    sel_sql = text("select hhr_cum_code from boogoo_payroll.hhr_py_formula_temp where tenant_id = :b1 and hhr_formula_id = :b2")
    result = conn.execute(sel_sql, b1=tenant_id, b2=formula_id).fetchone()

    if result is not None:
        cus_code_str = result['hhr_cum_code']
        cus_code_str = cus_code_str.replace('\n', '\n        ')
        cus_code_str = cus_code_str.replace('\t', '    ')
        class_string = class_string_m.replace('%1', trans_cus_code(cus_code_str))
        
        del_sql = text("delete from boogoo_payroll.hhr_py_formula_temp where tenant_id = :b1 and hhr_formula_id = :b2")
        conn.execute(del_sql, b1=tenant_id, b2=formula_id)
        conn.close()

        _locals = locals()
        exec(class_string, globals(), _locals)
        formula_class = _locals['class_meta']
        formula_class.tenant_id = tenant_id
        formula_class.id = formula_id

        return formula_class
    else:
        pass


# 处理用户自定义的公式代码
def trans_cus_code(code_string):
    """处理用户自己写的代码，替换为可执行代码"""
    code_string = code_string.replace('VR[', 'var[')
    code_string = code_string.replace('WT[', "pin_dic[")
    code_string = code_string.replace('WC[', "pin_acc_dic[")
    code_string = code_string.replace('from', "# ")
    code_string = code_string.replace('import', "# ")
    code_string = code_string.replace('replace', " ")
    code_string = code_string.replace('exit', " ")
    code_string = code_string.replace('eval', " ")
    code_string = code_string.replace('exec', " ")
    code_string = code_string.replace('compile', " ")
    code_string = code_string.replace('__import__', " ")
    code_string = code_string.replace('globals', " ")
    code_string = code_string.replace('locals', " ")
    code_string = code_string.replace('raw_input', " ")
    code_string = code_string.replace('input', " ")
    return code_string


def copy_wt(src_wt_obj, tgt_wt_obj):
    tgt_pin_id = tgt_wt_obj.pin_id
    tgt_wt_obj = deepcopy(src_wt_obj)
    tgt_wt_obj.pin_id = tgt_pin_id


def verify_formula(tenant_id, formula_id):
    formula_obj = get_formula(tenant_id, formula_id)
    formula_obj.formula_mock_exec()
