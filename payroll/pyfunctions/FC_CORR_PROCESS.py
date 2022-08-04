# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 更正处理函数
    Author: David
    Date: 2018/11/2
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_CORR_PROCESS'
        self.country = 'CHN'
        self.desc = '更正处理函数'
        self.descENG = '更正处理函数'
        self.func_type = 'B'
        self.instructions = "更正处理函数。无需传入参数"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': ['WT_TAX01_EE', 'WT_TAX01_ER', 'WT_TAX02_EE', 'WT_TAX02_ER', 'WT_TAX03_EE', 'WT_TAX03_ER', 'WT_TAX04_EE', 'WT_TAX04_ER', 'WT_NET',
                       'WT_TAX01_CORR_EE', 'WT_TAX01_CORR_ER', 'WT_TAX02_CORR_EE', 'WT_TAX02_CORR_ER', 'WT_TAX03_CORR_EE', 'WT_TAX03_CORR_ER',
                       'WT_TAX04_CORR_EE', 'WT_TAX04_CORR_ER', 'WT_NET_CORR'],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        # 比较的薪资项目：
        # WT_TAX01_EE	工资税
        # WT_TAX01_ER	工资税(公司)
        # WT_TAX02_EE	年终奖税
        # WT_TAX02_ER	年终奖税(公司)
        # WT_TAX03_EE	离职补偿金税
        # WT_TAX03_ER	离职补偿金税(公司)
        # WT_TAX04_EE	其他所得税
        # WT_TAX04_ER	其他所得税(公司)
        # WT_NET	    实发

        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        # 更正期间（期间状态C）与历史序号对应的期间之间需进行比较，计算个税/实发差额
        period_status = gv.get_var_value('VR_PERIOD_STATUS')
        if period_status == 'C':
            catalog = gv.get_run_var_value('PY_CATALOG')
            pin_list = ['WT_TAX01_EE', 'WT_TAX01_ER', 'WT_TAX02_EE', 'WT_TAX02_ER',
                        'WT_TAX03_EE', 'WT_TAX03_ER', 'WT_TAX04_EE', 'WT_TAX04_ER', 'WT_NET', 'WT_TAX05_EE', 'WT_TAX05_ER', 'WT_TAX_EE', 'WT_TAX_ER']
            result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=pin_list)
            wt_tax01_ee = gv.get_pin('WT_TAX01_EE').segment['*'].amt
            wt_tax01_er = gv.get_pin('WT_TAX01_ER').segment['*'].amt
            wt_tax02_ee = gv.get_pin('WT_TAX02_EE').segment['*'].amt
            wt_tax02_er = gv.get_pin('WT_TAX02_ER').segment['*'].amt
            wt_tax03_ee = gv.get_pin('WT_TAX03_EE').segment['*'].amt
            wt_tax03_er = gv.get_pin('WT_TAX03_ER').segment['*'].amt
            wt_tax04_ee = gv.get_pin('WT_TAX04_EE').segment['*'].amt
            wt_tax04_er = gv.get_pin('WT_TAX04_ER').segment['*'].amt
            wt_net = gv.get_pin('WT_NET').segment['*'].amt
            wt_tax05_ee = gv.get_pin('WT_TAX05_EE').segment['*'].amt
            wt_tax05_er = gv.get_pin('WT_TAX05_ER').segment['*'].amt
            wt_tax_ee = gv.get_pin('WT_TAX_EE').segment['*'].amt
            wt_tax_er = gv.get_pin('WT_TAX_ER').segment['*'].amt

            h_wt_tax01_ee = result_dic.get('WT_TAX01_EE', 0)
            h_wt_tax01_er = result_dic.get('WT_TAX01_ER', 0)
            h_wt_tax02_ee = result_dic.get('WT_TAX02_EE', 0)
            h_wt_tax02_er = result_dic.get('WT_TAX02_ER', 0)
            h_wt_tax03_ee = result_dic.get('WT_TAX03_EE', 0)
            h_wt_tax03_er = result_dic.get('WT_TAX03_ER', 0)
            h_wt_tax04_ee = result_dic.get('WT_TAX04_EE', 0)
            h_wt_tax04_er = result_dic.get('WT_TAX04_ER', 0)
            h_wt_net = result_dic.get('WT_NET', 0)
            h_wt_tax05_ee = result_dic.get('WT_TAX05_EE', 0)
            h_wt_tax05_er = result_dic.get('WT_TAX05_ER', 0)
            h_wt_tax_ee = result_dic.get('WT_TAX_EE', 0)
            h_wt_tax_er = result_dic.get('WT_TAX_ER', 0)

            gv.get_pin('WT_TAX01_CORR_EE').segment['*'].amt = wt_tax01_ee - h_wt_tax01_ee
            gv.get_pin('WT_TAX01_CORR_ER').segment['*'].amt = wt_tax01_er - h_wt_tax01_er
            gv.get_pin('WT_TAX02_CORR_EE').segment['*'].amt = wt_tax02_ee - h_wt_tax02_ee
            gv.get_pin('WT_TAX02_CORR_ER').segment['*'].amt = wt_tax02_er - h_wt_tax02_er
            gv.get_pin('WT_TAX03_CORR_EE').segment['*'].amt = wt_tax03_ee - h_wt_tax03_ee
            gv.get_pin('WT_TAX03_CORR_ER').segment['*'].amt = wt_tax03_er - h_wt_tax03_er
            gv.get_pin('WT_TAX04_CORR_EE').segment['*'].amt = wt_tax04_ee - h_wt_tax04_ee
            gv.get_pin('WT_TAX04_CORR_ER').segment['*'].amt = wt_tax04_er - h_wt_tax04_er
            gv.get_pin('WT_NET_CORR').segment['*'].amt = wt_net - h_wt_net
            gv.get_pin('WT_TAX05_CORR_EE').segment['*'].amt = wt_tax05_ee - h_wt_tax05_ee
            gv.get_pin('WT_TAX05_CORR_ER').segment['*'].amt = wt_tax05_er - h_wt_tax05_er
            gv.get_pin('WT_TAX_CORR_EE').segment['*'].amt = wt_tax_ee - h_wt_tax_ee
            gv.get_pin('WT_TAX_CORR_ER').segment['*'].amt = wt_tax_er - h_wt_tax_er

            gv.get_pin('WT_TAX01_CORR_EE').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX01_CORR_ER').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX02_CORR_EE').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX02_CORR_ER').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX03_CORR_EE').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX03_CORR_ER').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX04_CORR_EE').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX04_CORR_ER').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_NET_CORR').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX05_CORR_EE').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX05_CORR_ER').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX_CORR_EE').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAX_CORR_ER').segment['*'].currency = vr_pg_currency
