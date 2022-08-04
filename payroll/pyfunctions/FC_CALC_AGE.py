# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.func_lib_02 import round_rule
import datetime
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 计算年龄
    Author: David
    Date: 2018/10/31
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_CALC_AGE'
        self.country = 'CHN'
        self.desc = '计算年龄'
        self.descENG = 'Calculate age function'
        self.func_type = 'A'
        self.instructions = "计算年龄函数。参数1为出生日期，参数2为历经期结束日期。" \
                            "日期格式'2018/10/19'或者'2018-10-19'，或者直接传入日期变量，参数3为取整规则ID。" \
                            "该函数会直接将值存入变量VR_AGE。"
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
                'VR': ['VR_AGE'],
                'PA': ['birth_dt', 'f_period_end_dt', 'rule_id']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, birth_dt, f_period_end_dt, rule_id):
        """
        :param birth_dt: 出生日期
        :param f_period_end_dt: 历经期结束日期
        :param rule_id: 取整规则ID
        :return: 年数
        """

        if isinstance(birth_dt, str) and isinstance(f_period_end_dt, str):
            try:
                birth_dt = datetime.datetime.strptime(birth_dt, "%Y-%m-%d")
                f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    birth_dt = datetime.datetime.strptime(birth_dt, "%Y/%m/%d")
                    f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y/%m/%d")
                except (TypeError, ValueError):
                    return 0
        elif not isinstance(birth_dt, datetime.date) or not isinstance(f_period_end_dt, datetime.date):
            return 0

        days = (f_period_end_dt - birth_dt).days
        years = round_rule(rule_id, days / 365)
        gv.set_var_value('VR_AGE', years)
        return years
