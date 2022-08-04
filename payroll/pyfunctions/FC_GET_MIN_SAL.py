# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_min_mon_sal
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 获取最低工资函数
    Author: David
    Date: 2019/01/07
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_GET_MIN_SAL'
        self.country = 'CHN'
        self.desc = '获取最低工资函数'
        self.descENG = '获取最低工资函数'
        self.func_type = 'A'
        self.instructions = '获取最低工资函数，参数1为缴纳地，参数2为年度，参数3为结束日期，返回缴纳地工资水平对象'
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
                'PA': ['area', 'year', 'end_dt']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, area, year, end_dt):
        """
        :param area: 缴纳地
        :param year: 年度
        :param end_dt: 结束日期
        """
        return get_min_mon_sal(area, year, end_dt)
