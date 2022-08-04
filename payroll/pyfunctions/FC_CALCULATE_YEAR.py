# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import round_rule
import datetime
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 计算年数
    Author: David
    Date: 2018/09/10
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_CALCULATE_YEAR'
        self.country = 'CHN'
        self.desc = '计算年数函数'
        self.descENG = 'Calculate year function'
        self.func_type = 'A'
        self.instructions = "计算年数函数，保留2位小数。参数1为起始日期，参数2为终止日期。" \
                            "日期格式'2018/10/19'或者'2018-10-19'，或者直接传入日期。参数3为取整规则ID。"
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
                'PA': ['bgn_dt', 'end_dt', 'rule_id']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, bgn_dt, end_dt, rule_id):
        """
        :param bgn_dt: 起始日期
        :param end_dt: 终止日期
        :param rule_id: 取整规则ID
        :return: 年数
        """

        if isinstance(bgn_dt, str) and isinstance(end_dt, str):
            try:
                bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y-%m-%d")
                end_dt = datetime.datetime.strptime(end_dt, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y/%m/%d")
                    end_dt = datetime.datetime.strptime(end_dt, "%Y/%m/%d")
                except (TypeError, ValueError):
                    bgn_dt = datetime.datetime(1900, 1, 1)
                    end_dt = datetime.datetime(1900, 1, 1)
        elif not isinstance(bgn_dt, datetime.date) or not isinstance(end_dt, datetime.date):
            bgn_dt = datetime.datetime(1900, 1, 1)
            end_dt = datetime.datetime(1900, 1, 1)

        days = (end_dt - bgn_dt).days + 1
        ret = round_rule(rule_id, days / 365)
        if ret is None:
            ret = 0
        return ret

