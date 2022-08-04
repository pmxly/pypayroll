# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 总额计算函数
    Author: David
    Date: 2018/11/2
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_WT_TOTAL'
        self.country = 'CHN'
        self.desc = '总额计算函数'
        self.descENG = '总额计算函数'
        self.func_type = 'B'
        self.instructions = "总额计算函数。无需传入参数"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': ['WC_TOTAL'],
                'WT': ['WT_TOTAL_IN', 'WT_DEBT_IN', 'WT_DEBT', 'WT_TOTAL'],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        wc_total = gv.get_pin_acc('WC_TOTAL').segment['*'].amt
        wt_total_in = gv.get_pin('WT_TOTAL_IN').segment['*'].amt
        wt_debt_in = gv.get_pin('WT_DEBT_IN').segment['*'].amt

        total_amt = wc_total + wt_total_in - wt_debt_in

        # 当total_amt<0时，将total_amt*-1后放入“欠款”（WT_DEBT），总额（WT_TOTAL）=0
        if total_amt < 0:
            gv.get_pin('WT_DEBT').segment['*'].amt = total_amt * -1
            gv.get_pin('WT_TOTAL').segment['*'].amt = 0
        # 当total_amt>=0时，总额（WT_TOTAL）=total_amt,无欠款
        else:
            gv.get_pin('WT_DEBT').segment['*'].amt = 0
            gv.get_pin('WT_TOTAL').segment['*'].amt = total_amt

        gv.get_pin('WT_DEBT').segment['*'].currency = vr_pg_currency
        gv.get_pin('WT_TOTAL').segment['*'].currency = vr_pg_currency
