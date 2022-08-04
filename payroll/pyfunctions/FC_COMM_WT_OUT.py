# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 通用流出处理函数
    Author: David
    Date: 2019/06/09
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG', 'wc_code', 'pin_code']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_COMM_WT_OUT'
        self.country = 'CHN'
        self.desc = '通用流出处理函数'
        self.descENG = '通用流出处理函数'
        self.func_type = 'B'
        self.instructions = "通用流出处理函数。参数1为累计项目编码，参数2为薪资项目编码。"
        self.instructionsENG = self.instructions

        self.wc_code = ''
        self.pin_code = ''

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [self.wc_code],
                'WT': [self.pin_code],
                'VR': [],
                'PA': ['wc_code', 'pin_code']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, wc_code, pin_code):
        self.wc_code = wc_code
        self.pin_code = pin_code

        catalog = gv.get_run_var_value('PY_CATALOG')
        period_status = gv.get_var_value('VR_PERIOD_STATUS')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        # 当前期间和更正期间无流出
        # 被追溯的期间（期间状态R）：比较本次累计的金额与历史序号对应期间累计的金额(累计类型为P，所以存储在薪资项目结果表中)，差额部分流出。本次累计-历史序号的累计
        if period_status == 'R':
            pin_acc_list = [wc_code]
            result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=pin_acc_list)

            # 累计金额(个人)流出
            gap_amt = gv.get_pin_acc(wc_code).segment['*'].amt - result_dic.get(wc_code, 0)
            if gv.pin_in_dic(pin_code):
                gv.get_pin(pin_code).segment['*'].amt = gap_amt
                gv.get_pin(pin_code).segment['*'].currency = vr_pg_currency
