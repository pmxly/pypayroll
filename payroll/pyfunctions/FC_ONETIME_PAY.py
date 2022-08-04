# coding:utf-8

from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
from ..pypins.pin_process import PinProcess
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 一次性支付扣除处理函数
    Author: David
    Date: 2018/09/19
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_ONETIME_PAY'
        self.country = 'CHN'
        self.desc = '一次性支付扣除处理函数'
        self.descENG = '一次性支付扣除处理函数'
        self.func_type = 'B'
        self.instructions = "一次性支付扣除处理函数，无需传入参数。"
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
        # 按历经期处理一次性支付扣除
        catalog = gv.get_run_var_value('PY_CATALOG')
        cur_cal = gv.get_run_var_value('CUR_CAL_OBJ')
        pin_process_obj = gv.get_run_var_value('PIN_PROCESS_OBJ')
        if pin_process_obj is None:
            pin_process_obj = PinProcess(fc_obj=self, catalog=catalog, cal=cur_cal)
            gv.set_run_var_value('PIN_PROCESS_OBJ', pin_process_obj)
        else:
            pin_process_obj.fc_obj = self
        pin_process_obj.process_onetime_pins()

