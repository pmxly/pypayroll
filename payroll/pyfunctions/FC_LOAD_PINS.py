# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from ..pypins.pin_util import init_emp_pin_dic


class PyFunction(FunctionObject):
    """
    Desc: 加载系统薪资项目和适用范围薪资项目函数
    Author: David
    Date: 2019/02/28
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_LOAD_PINS'
        self.country = 'CHN'
        self.desc = '加载薪资项目函数'
        self.descENG = '加载薪资项目函数'
        self.func_type = 'B'
        self.instructions = "加载薪资项目函数。"
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
        # 根据历经期日历，适用资格组，通用薪资项目初始化薪资项目
        catalog = gv.get_run_var_value('PY_CATALOG')
        init_emp_pin_dic(catalog)
