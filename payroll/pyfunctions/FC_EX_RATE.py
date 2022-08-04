# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import ex_currency
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 汇率转换函数
    Author: David
    Date: 2018/09/06
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_EX_RATE'
        self.country = 'CHN'
        self.desc = '汇率转换函数'
        self.descENG = 'Exchange rate function'
        self.func_type = 'A'
        self.instructions = '汇率转换函数，参数1为源货币，参数2为目标货币，参数3为源货币金额，返回目标货币金额'
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
                'PA': ['from_cur', 'to_cur', 'from_amt']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, from_cur, to_cur, from_amt):
        """
        :param from_cur: 源货币
        :param to_cur: 目标货币
        :param from_amt: 源货币金额
        :return:to_amt 目标货币金额
        """
        return ex_currency(from_cur, to_cur, from_amt)
