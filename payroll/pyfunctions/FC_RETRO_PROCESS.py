# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 追溯处理函数
    Author: David
    Date: 2018/11/2
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_RETRO_PROCESS'
        self.country = 'CHN'
        self.desc = '追溯处理函数'
        self.descENG = '追溯处理函数'
        self.func_type = 'B'
        self.instructions = "追溯处理函数。无需传入参数"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': ['WC_TOTAL'],
                'WT': [],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):

        # 被追溯的期间（期间状态R）与历史序号对应的期间之间需进行比较，计算追溯金额
        # 追溯对象：总额累计中的薪资项目
        period_status = gv.get_var_value('VR_PERIOD_STATUS')
        if period_status == 'R':
            catalog = gv.get_run_var_value('PY_CATALOG')

            process_wc_lst = ['WC_TOTAL']
            for wc_cd in process_wc_lst:
                wc_acc = gv.get_pin_acc(wc_cd)
                if wc_acc is None:
                    continue
                pin_acc_childs = wc_acc.pins
                pin_cd_list = []
                pin_obj_list = []
                for child in pin_acc_childs:
                    if child.is_valid:
                        pin_cd = child.pin_code
                        pin_obj = gv.get_pin(pin_cd)
                        pin_cd_list.append(pin_cd)
                        pin_obj_list.append(pin_obj)
                result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=pin_cd_list)
                # 比较对应薪资项目的金额（分段时先累计后比较），用本次计算的值减去历史计算的值，差额放入追溯列（分段时差额放在最后一个分段）
                for pin_obj in pin_obj_list:
                    pin_id = pin_obj.pin_id
                    pin_amt = 0
                    seg = None
                    if pin_obj.seg_flag == 'Y':
                        for seg in pin_obj.segment.values():
                            pin_amt += seg.amt
                    else:
                        seg = pin_obj.segment['*']
                        pin_amt = seg.amt
                    pin_hist_amt = result_dic.get(pin_id, 0)
                    seg.retro_amt = pin_amt - pin_hist_amt
