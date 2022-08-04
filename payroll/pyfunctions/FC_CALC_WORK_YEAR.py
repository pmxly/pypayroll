# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.func_lib_02 import round_rule
import datetime
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 计算工龄
    Author: David
    Date: 2018/09/10
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_CALC_WORK_YEAR'
        self.country = 'CHN'
        self.desc = '计算工龄'
        self.descENG = 'Calculate work year function'
        self.func_type = 'A'
        self.instructions = "计算工龄函数。参数1为首次工作日期，参数2为历经期结束日期。" \
                            "日期格式'2018/10/19'或者'2018-10-19'，或者直接传入日期变量，参数3为调整工龄扣除，参数4为取整规则ID。" \
                            "该函数会直接将值存入变量VR_WORKYEAR。"
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
                'VR': ['VR_WORKYEAR'],
                'PA': ['first_work_dt', 'f_period_end_dt', 'work_deduct', 'rule_id']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, first_work_dt, f_period_end_dt, work_deduct, rule_id):
        """
        :param first_work_dt: 首次工作日期
        :param f_period_end_dt: 历经期结束日期
        :param work_deduct: 调整工龄扣除
        :param rule_id: 取整规则ID
        :return: 年数
        """

        if isinstance(first_work_dt, str) and isinstance(f_period_end_dt, str):
            try:
                first_work_dt = datetime.datetime.strptime(first_work_dt, "%Y-%m-%d")
                f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    first_work_dt = datetime.datetime.strptime(first_work_dt, "%Y/%m/%d")
                    f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y/%m/%d")
                except (TypeError, ValueError):
                    first_work_dt = datetime.datetime(1900, 1, 1)
                    f_period_end_dt = datetime.datetime(1900, 1, 1)
        elif not isinstance(first_work_dt, datetime.date) or not isinstance(f_period_end_dt, datetime.date):
            first_work_dt = datetime.datetime(1900, 1, 1)
            f_period_end_dt = datetime.datetime(1900, 1, 1)

        days = (f_period_end_dt - first_work_dt).days - float(work_deduct)
        years = round_rule(rule_id, days / 365)
        gv.set_var_value('VR_WORKYEAR', years)
        return years
