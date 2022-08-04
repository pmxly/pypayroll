# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log, add_fc_log_item


class PyFunction(FunctionObject):
    """
    Desc: 计税金额函数
    Author: David
    Date: 2018/11/1
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_TAX_BASE'
        self.country = 'CHN'
        self.desc = '计税金额函数'
        self.descENG = '计税金额函数'
        self.func_type = 'B'
        self.instructions = "计税金额函数"
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
        catalog = gv.get_run_var_value('PY_CATALOG')
        period_status = gv.get_var_value('VR_PERIOD_STATUS')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币
        
        # 被追溯的期间（期间状态R）：计税金额维持不变，即直接取历史序号对应的期间的计税金额
        if period_status == 'R':
            pin_list = ['WT_TAXBASE01_EE', 'WT_TAXBASE01_ER', 'WT_TAXBASE02_EE', 'WT_TAXBASE02_ER', 'WT_TAXBASE03_EE',
                        'WT_TAXBASE03_ER', 'WT_TAXBASE04_EE', 'WT_TAXBASE04_ER', 'WT_TAXBASE05_EE', 'WT_TAXBASE05_ER', 'WT_TAXBASE_EE', 'WT_TAXBASE_ER']
            result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=pin_list)
            for pin_cd, pin_amt in result_dic.items():
                gv.get_pin(pin_cd).segment['*'].amt = pin_amt
                gv.get_pin(pin_cd).segment['*'].currency = vr_pg_currency
            add_fc_log_item(self, 'WT', '', pin_list)
        else:
            vr_taxtype = gv.get_var_value('VR_TAXTYPE')

            if vr_taxtype == '2':
                # 工资计税(个人)
                # 税基+税基流入，<0时赋值0
                temp_val = gv.get_pin_acc('WC_TAXBASE01_EE').segment['*'].amt + gv.get_pin('WT_TAXBASE01EE_IN').segment['*'].amt
                temp_val = 0 if temp_val < 0 else temp_val
                gv.get_pin('WT_TAXBASE01_EE').segment['*'].amt = temp_val
                gv.get_pin('WT_TAXBASE01_EE').segment['*'].currency = vr_pg_currency

                # 工资计税(公司)
                temp_val = gv.get_pin_acc('WC_TAXBASE01_ER').segment['*'].amt + gv.get_pin('WT_TAXBASE01ER_IN').segment['*'].amt
                temp_val = 0 if temp_val < 0 else temp_val
                gv.get_pin('WT_TAXBASE01_ER').segment['*'].amt = temp_val
                gv.get_pin('WT_TAXBASE01_ER').segment['*'].currency = vr_pg_currency

            # 年终奖计税(个人)
            temp_val = gv.get_pin_acc('WC_TAXBASE02_EE').segment['*'].amt + 0
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE02_EE').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE02_EE').segment['*'].currency = vr_pg_currency

            # 年终奖计税(公司)
            temp_val = gv.get_pin_acc('WC_TAXBASE02_ER').segment['*'].amt + 0
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE02_ER').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE02_ER').segment['*'].currency = vr_pg_currency

            # 离职补偿金计税(个人)
            temp_val = gv.get_pin_acc('WC_TAXBASE03_EE').segment['*'].amt + 0
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE03_EE').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE03_EE').segment['*'].currency = vr_pg_currency

            # 离职补偿金计税(公司)
            temp_val = gv.get_pin_acc('WC_TAXBASE03_ER').segment['*'].amt + 0
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE03_ER').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE03_ER').segment['*'].currency = vr_pg_currency

            # 其他所得计税(个人)
            temp_val = gv.get_pin_acc('WC_TAXBASE04_EE').segment['*'].amt + gv.get_pin('WT_TAXBASE04EE_IN').segment['*'].amt
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE04_EE').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE04_EE').segment['*'].currency = vr_pg_currency

            # 其他所得计税(公司)
            temp_val = gv.get_pin_acc('WC_TAXBASE04_ER').segment['*'].amt + gv.get_pin('WT_TAXBASE04ER_IN').segment['*'].amt
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE04_ER').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE04_ER').segment['*'].currency = vr_pg_currency

            # 劳务报酬计税(个人)
            temp_val = gv.get_pin_acc('WC_TAXBASE05_EE').segment['*'].amt + gv.get_pin('WT_TAXBASE05EE_IN').segment['*'].amt
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE05_EE').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE05_EE').segment['*'].currency = vr_pg_currency

            # 劳务报酬计税(公司)
            temp_val = gv.get_pin_acc('WC_TAXBASE05_ER').segment['*'].amt + gv.get_pin('WT_TAXBASE05ER_IN').segment['*'].amt
            temp_val = 0 if temp_val < 0 else temp_val
            gv.get_pin('WT_TAXBASE05_ER').segment['*'].amt = temp_val
            gv.get_pin('WT_TAXBASE05_ER').segment['*'].currency = vr_pg_currency

            if vr_taxtype == '1':
                # 计税金额(个人)(负值也继续保留)
                temp_val = gv.get_pin_acc('WC_TAXBASE01_EE').segment['*'].amt + gv.get_pin('WT_TAXBASE01EE_IN').segment['*'].amt
                gv.get_pin('WT_TAXBASE_EE').segment['*'].amt = temp_val
                gv.get_pin('WT_TAXBASE_EE').segment['*'].currency = vr_pg_currency

                # 计税金额(公司)(负值也继续保留)
                temp_val = gv.get_pin_acc('WC_TAXBASE01_ER').segment['*'].amt + gv.get_pin('WT_TAXBASE01ER_IN').segment['*'].amt
                gv.get_pin('WT_TAXBASE_ER').segment['*'].amt = temp_val
                gv.get_pin('WT_TAXBASE_ER').segment['*'].currency = vr_pg_currency

            add_fc_log_item(self, 'WC', '', ['WC_TAXBASE01_EE', 'WC_TAXBASE01_ER', 'WC_TAXBASE02_EE', 'WC_TAXBASE02_ER', 'WC_TAXBASE03_EE', 'WC_TAXBASE03_ER',
                                             'WC_TAXBASE04_EE', 'WC_TAXBASE04_ER', 'WC_TAXBASE05_EE', 'WC_TAXBASE05_ER'])
            add_fc_log_item(self, 'WT', '', ['WT_TAXBASE01EE_IN', 'WT_TAXBASE01ER_IN', 'WT_TAXBASE04EE_IN', 'WT_TAXBASE04ER_IN',
                                             'WT_TAXBASE05EE_IN', 'WT_TAXBASE05ER_IN'])
