# coding:utf-8

from ..pyexecute.pycalculate.seg.seg_info import SegInfo
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 期间分段处理函数
    Author: David
    Date: 2019/02/28
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_PERIOD_SEG'
        self.country = 'CHN'
        self.desc = '期间分段处理函数'
        self.descENG = '期间分段处理函数'
        self.func_type = 'B'
        self.instructions = "期间分段处理函数。"
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
        # 初始化分段信息，并放进全局变量中
        gv.set_run_var_value('SEG_INFO_OBJ', SegInfo())
