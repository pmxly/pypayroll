# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 通用追溯处理函数
    Author: David
    Date: 2019/06/09
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG', 'pin_code']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_COMM_RETRO'
        self.country = 'CHN'
        self.desc = '通用追溯处理函数'
        self.descENG = '通用追溯处理函数'
        self.func_type = 'B'
        self.instructions = "通用追溯处理函数。参数为薪资项目编码。"
        self.instructionsENG = self.instructions

        self.pin_code = ''

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [self.pin_code],
                'VR': [],
                'PA': ['pin_code']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, pin_code):
        # 被追溯的期间（期间状态R）与历史序号对应的期间之间需进行比较，计算追溯金额
        period_status = gv.get_var_value('VR_PERIOD_STATUS')
        if period_status == 'R':
            catalog = gv.get_run_var_value('PY_CATALOG')
            if type(pin_code) is list:
                pin_cd_list = pin_code
            else:
                pin_cd_list = [pin_code]
            result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=pin_cd_list)
            for pin_cd in pin_cd_list:
                if pin_cd in gv.get_pin_dic():
                    pin_obj = gv.get_pin(pin_cd)
                    # 比较对应薪资项目的金额（分段时先累计后比较），用本次计算的值减去历史计算的值，差额放入追溯列（分段时差额放在最后一个分段）
                    pin_amt = 0
                    seg = None
                    if pin_obj.seg_flag == 'Y':
                        for seg in pin_obj.segment.values():
                            pin_amt += seg.amt
                    else:
                        seg = pin_obj.segment['*']
                        pin_amt = seg.amt
                    pin_hist_amt = result_dic.get(pin_cd, 0)
                    seg.retro_amt = pin_amt - pin_hist_amt