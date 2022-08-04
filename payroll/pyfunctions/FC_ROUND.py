# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import round_rule
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 取整函数
    Author: David
    Date: 2018/09/06
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_ROUND'
        self.country = 'CHN'
        self.desc = '取整函数'
        self.descENG = 'Round function'
        self.func_type = 'A'
        self.instructions = '取整函数，参数1为数量输入值，参数2为取整规则。'
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
                'PA': ['input_val', 'rule_id']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, input_val, rule_id):
        """
        :param input_val: 数量输入值
        :param rule_id: 取整规则
        :return: 取整后的数值
        """

        return round_rule(rule_id, input_val)

