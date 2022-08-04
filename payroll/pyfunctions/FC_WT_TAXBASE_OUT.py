# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 税基流出函数
    Author: David
    Date: 2018/11/1
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_WT_TAXBASE_OUT'
        self.country = 'CHN'
        self.desc = '税基流出函数'
        self.descENG = '税基流出函数'
        self.func_type = 'B'
        self.instructions = "税基流出函数。无需传入参数"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': ['WC_TAXBASE01_EE', 'WC_TAXBASE01_ER', 'WC_TAXBASE02_EE', 'WC_TAXBASE02_ER', 'WC_TAXBASE03_EE', 'WC_TAXBASE03_ER',
                       'WC_TAXBASE04_EE', 'WC_TAXBASE04_ER', 'WC_TAXBASE05_EE', 'WC_TAXBASE05_ER'],
                'WT': ['WT_TAXBASE01EE_OUT', 'WT_TAXBASE01ER_OUT', 'WT_TAXBASE02EE_OUT', 'WT_TAXBASE02ER_OUT',
                       'WT_TAXBASE03EE_OUT', 'WT_TAXBASE03ER_OUT', 'WT_TAXBASE04EE_OUT', 'WT_TAXBASE04ER_OUT',
                       'WT_TAXBASE05EE_OUT', 'WT_TAXBASE05ER_OUT'],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        catalog = gv.get_run_var_value('PY_CATALOG')
        period_status = gv.get_var_value('VR_PERIOD_STATUS')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        # 当前期间和更正期间无税基流出
        # 被追溯的期间（期间状态R）：比较本次累计的税基与历史序号对应期间累计的税基(累计类型为P，所以存储在薪资项目结果表中)，差额部分流出。本次累计-历史序号的累计
        if period_status == 'R':
            pin_acc_list = ['WC_TAXBASE01_EE', 'WC_TAXBASE01_ER', 'WC_TAXBASE02_EE', 'WC_TAXBASE02_ER',
                            'WC_TAXBASE03_EE', 'WC_TAXBASE03_ER', 'WC_TAXBASE04_EE', 'WC_TAXBASE04_ER', 'WC_TAXBASE05_EE', 'WC_TAXBASE05_ER']
            result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=pin_acc_list)

            # 工资税基(个人)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE01_EE').segment['*'].amt - result_dic.get('WC_TAXBASE01_EE', 0)
            gv.get_pin('WT_TAXBASE01EE_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE01EE_OUT').segment['*'].currency = vr_pg_currency

            # 工资税基(公司)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE01_ER').segment['*'].amt - result_dic.get('WC_TAXBASE01_ER', 0)
            gv.get_pin('WT_TAXBASE01ER_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE01ER_OUT').segment['*'].currency = vr_pg_currency

            # 年终奖税基(个人)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE02_EE').segment['*'].amt - result_dic.get('WC_TAXBASE02_EE', 0)
            gv.get_pin('WT_TAXBASE02EE_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE02EE_OUT').segment['*'].currency = vr_pg_currency

            # 年终奖税基(公司)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE02_ER').segment['*'].amt - result_dic.get('WC_TAXBASE02_ER', 0)
            gv.get_pin('WT_TAXBASE02ER_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE02ER_OUT').segment['*'].currency = vr_pg_currency

            # 离职补偿金税基(个人)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE03_EE').segment['*'].amt - result_dic.get('WC_TAXBASE03_EE', 0)
            gv.get_pin('WT_TAXBASE03EE_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE03EE_OUT').segment['*'].currency = vr_pg_currency

            # 离职补偿金税基(公司)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE03_ER').segment['*'].amt - result_dic.get('WC_TAXBASE03_ER', 0)
            gv.get_pin('WT_TAXBASE03ER_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE03ER_OUT').segment['*'].currency = vr_pg_currency

            # 其他所得税基(个人)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE04_EE').segment['*'].amt - result_dic.get('WC_TAXBASE04_EE', 0)
            gv.get_pin('WT_TAXBASE04EE_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE04EE_OUT').segment['*'].currency = vr_pg_currency

            # 其他所得税基(公司)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE04_ER').segment['*'].amt - result_dic.get('WC_TAXBASE04_ER', 0)
            gv.get_pin('WT_TAXBASE04ER_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE04ER_OUT').segment['*'].currency = vr_pg_currency

            # 劳务报酬税基(个人)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE05_EE').segment['*'].amt - result_dic.get('WC_TAXBASE05_EE', 0)
            gv.get_pin('WT_TAXBASE05EE_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE05EE_OUT').segment['*'].currency = vr_pg_currency

            # 劳务报酬税基(公司)流出
            gap_amt = gv.get_pin_acc('WC_TAXBASE05_ER').segment['*'].amt - result_dic.get('WC_TAXBASE05_ER', 0)
            gv.get_pin('WT_TAXBASE05ER_OUT').segment['*'].amt = gap_amt
            gv.get_pin('WT_TAXBASE05ER_OUT').segment['*'].currency = vr_pg_currency
