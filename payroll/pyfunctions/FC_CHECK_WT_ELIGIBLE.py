# coding:utf-8

from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 人事事件检查函数
    Author: David
    Date: 2018/09/20
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_CHECK_WT_ELIGIBLE'
        self.country = 'CHN'
        self.desc = '检查薪资项目是否适用函数'
        self.descENG = '检查薪资项目是否适用函数'
        self.func_type = 'A'
        self.instructions = '检查薪资项目是否适用。传入薪资项目编码，返回Y或N。'
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
                'PA': ['wt_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, wt_cd):
        """
        :param wt_cd: 薪资项目
        :return: Y-代表薪资项目适用，N-代表薪资项目不适用
        """

        ret = gv.pin_in_dic(wt_cd)
        if ret:
            return 'Y'
        else:
            return 'N'

