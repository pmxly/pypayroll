# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 计算总额流出函数
    Author: David
    Date: 2018/11/2
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_WT_TOTAL_OUT'
        self.country = 'CHN'
        self.desc = '计算总额流出函数'
        self.descENG = '计算总额流出函数'
        self.func_type = 'B'
        self.instructions = "计算总额流出函数。无需传入参数"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': ['WT_TOTAL_OUT', 'WT_TOTAL'],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        period_status = gv.get_var_value('VR_PERIOD_STATUS')
        catalog = gv.get_run_var_value('PY_CATALOG')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        # 当前期间（期间状态N）：无总额流出
        if period_status == 'N':
            gv.get_pin('WT_TOTAL_OUT').segment['*'].amt = 0
        # 更正期间（期间状态C）：无总额流出
        elif period_status == 'C':
            gv.get_pin('WT_TOTAL_OUT').segment['*'].amt = 0
        # 被追溯的期间（期间状态R）：比较总额（WT_TOTAL）与历史序号对应期间的总额，差额部分流出（WT_TOTAL_OUT）
        elif period_status == 'R':
            wt_total = gv.get_pin('WT_TOTAL').segment['*'].amt
            result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=['WT_TOTAL'])
            hist_wt_total = result_dic.get('WT_TOTAL', 0)
            gap = wt_total - hist_wt_total
            gv.get_pin('WT_TOTAL_OUT').segment['*'].amt = gap

        gv.get_pin('WT_TOTAL_OUT').segment['*'].currency = vr_pg_currency
