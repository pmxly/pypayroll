# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pyexecute.pycalculate.pt.pt_process import PTProcess
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 获取考勤结果对象列表
    Author: David
    Date: 2018/12/05
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_GET_ABS_LST'
        self.country = 'CHN'
        self.desc = '获取考勤结果对象列表函数'
        self.descENG = '获取考勤结果对象列表函数'
        self.func_type = 'A'
        self.instructions = "获取考勤结果对象列表函数"
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
        abs_obj_lst = []
        vr_cal_ptpy = gv.get_var_value('VR_CAL_PTPY')
        if vr_cal_ptpy == 'Y':
            pt_process_obj = gv.get_run_var_value('PT_PROCESS_OBJ')
            if pt_process_obj is None:
                catalog = gv.get_run_var_value('PY_CATALOG')
                pt_process_obj = PTProcess(fc_obj=self, catalog=catalog)
                gv.set_run_var_value('PT_PROCESS_OBJ', pt_process_obj)
            else:
                pt_process_obj.fc_obj = self
            abs_obj_lst = pt_process_obj.abs_obj_lst
            return abs_obj_lst
        else:
            return abs_obj_lst
