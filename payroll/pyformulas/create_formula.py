# coding:utf-8

from ..pysysutils import global_variables as gv
from copy import deepcopy
from sqlalchemy import select
from sqlalchemy import text
from decimal import Decimal
from ..pyfunctions import *
from ..pyfunctions import __all__ as func_list
from ..pysysutils.py_calc_log import log
from datetime import timedelta, date

one_day = timedelta(days=1)


# 根据系统ID和公式ID获取一个实例化的公式
def select_formula(tenant_id, formula_id):
    """根据系统ID和公式ID在数据库中查找出对应公式的配置数据，并实例化"""

    # 基类的字符串，根据数据动态生成公式对象
    class_string_m = """
class FormulaObject():

    def __init__(self):
        # 租户ID
        self.tenant_id = ''
        # 公式唯一id
        self.id = ''
        # 公式所属国家ALL:所有国家 CHN:中国
        self.country = ''
        # 公式描述，用于系统中的描述
        self.desc = ''
        # 公式英文描述，用于系统中的描述
        self.descENG = ''
        # 公式使用中文说明
        self.instructions = ''
        # 公式使用英文说明
        self.instructionsENG = ''
        # 用户编写的原始代码
        self.cus_code_string = ''
    
    @log()
    def formula_exec(self):
        var = gv.get_variable_dic()
        pin_dic = gv.get_pin_dic()
        pin_acc_dic = gv.get_pin_acc_dic()
        # print('current formula id:' + self.id)
        %1

class_meta=FormulaObject()
    """

    db = gv.get_db()
    t = db.get_table('hhr_py_formula', schema_name='boogoo_payroll')

    stmt = select([t.c.tenant_id, t.c.hhr_formula_id, t.c.hhr_country, t.c.hhr_description, t.c.hhr_cum_code],
                  (t.c.tenant_id == tenant_id) & (t.c.hhr_formula_id == formula_id))

    result = db.conn.execute(stmt).fetchone()

    if result is not None:
        cus_code_str = result['hhr_cum_code']
        cus_code_str = cus_code_str.replace('\n', '\n        ')
        cus_code_str = cus_code_str.replace('\t', '    ')
        class_string = class_string_m.replace('%1', trans_cus_code(cus_code_str))
        _locals = locals()
        exec(class_string, globals(), _locals)
        formula_class = _locals['class_meta']
        formula_class.tenant_id = result['tenant_id']
        formula_class.id = result['hhr_formula_id']
        formula_class.country = result['hhr_country']
        formula_class.desc = result['hhr_description']
        formula_class.cus_code_string = result['hhr_cum_code']
        formula_class.function_list = list()
        formula_class.variable_list = list()
        formula_class.pin_list = list()
        formula_class.pin_acc_list = list()

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            formula_class.trace_dic = {
                                'id': formula_class.id,
                                'desc': formula_class.desc,
                                'type': 'FM',
                                'fm_obj': formula_class
                                }
        else:
            formula_class.trace_dic = {}

        add_lists(formula_class)
        return formula_class
    else:
        pass


def validate_pins(formula_class):
    """
    校验公式包含的所有薪资项目是否都在员工的使用范围和通用薪资项目中
    :param formula_class:公式对象
    :return:
    """
    try:
        pin_dic = gv.get_pin_dic()
        for pin_id in formula_class.pin_list:
            if pin_id not in pin_dic:
                raise Exception("薪资项目" + pin_id + ",不在适用范围")
    except Exception:
        raise


def add_lists(formula_class):
    """
    给公式对象添加function_list，variable_list，pin_list。
    :param formula_class:公式对象
    :return:
    """
    sql = text("select hhr_fvp_id,hhr_fvp_type from boogoo_payroll.hhr_py_formula_fvp_list a where tenant_id=:b1 and hhr_formula_id=:b2")

    result = gv.get_db().conn.execute(sql, b1=formula_class.tenant_id, b2=formula_class.id).fetchall()
    if result is not None:
        for result_line in result:
            if result_line['hhr_fvp_type'] == 'FC':
                formula_class.function_list.append(result_line['hhr_fvp_id'])
            elif result_line['hhr_fvp_type'] == 'VR':
                formula_class.variable_list.append(result_line['hhr_fvp_id'])
            elif result_line['hhr_fvp_type'] == 'WT':
                formula_class.pin_list.append(result_line['hhr_fvp_id'])
            elif result_line['hhr_fvp_type'] == 'WC':
                formula_class.pin_acc_list.append(result_line['hhr_fvp_id'])


# 处理用户自定义的公式代码
def trans_cus_code(code_string):
    """处理用户自己写的代码，替换为可执行代码"""

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

    code_string = code_string.replace('VR[', 'var[')
    for func in func_list:
        code_string = code_string.replace(func + "(", func + '.PyFunction().func_exec(')

    return code_string


# 创建公式实例
def create(tenant_id, formula_id):
    formula_dic = gv.get_run_var_value('ALL_FORMULA_DIC')

    log_flag = gv.get_run_var_value('LOG_FLAG')
    pre_log_flag = gv.get_run_var_value('PRE_LOG_FLAG')
    if log_flag != pre_log_flag:
        new_formula = select_formula(tenant_id, formula_id)
        formula_dic[formula_id] = new_formula
        return new_formula
    else:
        """创建公式实例,如果已经存在，则在字典中获取"""
        if formula_id not in formula_dic:
            formula_dic[formula_id] = select_formula(tenant_id, formula_id)
        return formula_dic[formula_id]


def copy_wt(src_wt_obj, tgt_wt_obj):
    tgt_pin_id = tgt_wt_obj.pin_id
    tgt_wt_obj = deepcopy(src_wt_obj)
    tgt_wt_obj.pin_id = tgt_pin_id
