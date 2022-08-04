# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pyexecute.pycalculate.pt.pt_process import PTProcess
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 汇总考勤项目
    Author: David
    Date: 2018/12/05
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_SUM_PT_CD'
        self.country = 'CHN'
        self.desc = '汇总考勤项目函数'
        self.descENG = '汇总考勤项目函数'
        self.func_type = 'A'
        self.instructions = "汇总考勤项目函数，返回总数量、单位。"
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
                'PA': ['pt_code']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, pt_code):
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

            return pt_process_obj.sum_pt_cd(pt_code)
        else:
            return 0, None

