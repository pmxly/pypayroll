# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pyexecute.pycalculate.pt.pt_process import PTProcess
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 获取考勤记录
    Author: David
    Date: 2018/10/31
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_GET_PT_REC'
        self.country = 'CHN'
        self.desc = '获取考勤记录函数'
        self.descENG = '获取考勤记录函数'
        self.func_type = 'B'
        self.instructions = "获取考勤记录函数。"
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
                'VR': ['VR_CAL_PTPY', 'VR_PG_PTPERIOD', 'VR_PG_NONUM', 'VR_F_PERIOD', 'VR_F_PERIOD_YEAR', 'VR_F_PERIOD_NO', 'VR_F_PERIOD_BEG',
                       'VR_F_PERIOD_END', 'VR_PT_PERIOD', 'VR_PT_PERIOD_YEAR', 'VR_PT_PERIOD_YEAR', 'VR_PT_PERIOD_NO',
                       'VR_PT_PERIOD_BEG', 'VR_PT_PERIOD_END'],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        # 判断当前任职记录上是否勾选了"处理考勤"状态，勾选的情况下才处理考勤
        vr_cal_ptpy = gv.get_var_value('VR_CAL_PTPY')
        if vr_cal_ptpy == 'Y':
            pt_process_obj = gv.get_run_var_value('PT_PROCESS_OBJ')
            if pt_process_obj is None:
                catalog = gv.get_run_var_value('PY_CATALOG')
                pt_process_obj = PTProcess(fc_obj=self, catalog=catalog)
                gv.set_run_var_value('PT_PROCESS_OBJ', pt_process_obj)
            else:
                pt_process_obj.fc_obj = self
            pt_process_obj.get_pt_rec()
        else:
            pass


