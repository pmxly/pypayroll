from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    薪资项目流入
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_WT_IN'
        self.country = 'CHN'
        self.desc = '薪资项目流入函数'
        self.descENG = '薪资项目流入函数'
        self.func_type = 'B'
        self.instructions = "处理欠款，总额，税基的流入。无需传入参数"
        self.instructionsENG = self.instructions
        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': ['WT_DEBT', 'WT_DEBT_IN', 'WT_TOTAL_IN', 'WT_TAXBASE01EE_IN', 'WT_TAXBASE01ER_IN',
                       'WT_TAXBASE04EE_IN', 'WT_TAXBASE04ER_IN'],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        cur_catalog = gv.get_run_var_value('PY_CATALOG')
        cur_type = gv.get_run_var_value('CUR_CALCULATE_TYPE')
        retro_catalog_list = gv.get_run_var_value('RETRO_CATALOG_LIST')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        """处理欠款WT_DEBT********************start"""
        return_dic = get_db_pin_by_catalog(catalog=cur_catalog, seqnum=cur_catalog.prev_seq, pin_code_list=['WT_DEBT'])
        wt_obj = gv.get_pin('WT_DEBT_IN')
        if wt_obj is not None:
            wt_obj.segment['*'].amt = return_dic.get('WT_DEBT', 0)
            wt_obj.segment['*'].currency = vr_pg_currency
        """处理欠款WT_DEBT********************end"""

        """总额流入WT_TOTAL_IN和税基********************start"""
        if cur_type == 'R':
            return_dic = get_db_pin_by_catalog(catalog=cur_catalog, seqnum=cur_catalog.hist_seq, pin_code_list=['WT_TOTAL_IN'])
            gv.get_pin('WT_TOTAL_IN').segment['*'].amt = return_dic.get('WT_TOTAL_IN', 0)
            gv.get_pin('WT_TOTAL_IN').segment['*'].currency = vr_pg_currency
        else:
            wt_total_out = 0
            wt_tax01ee_out = 0
            wt_tax01er_out = 0
            wt_tax02ee_out = 0
            wt_tax02er_out = 0
            wt_tax03ee_out = 0
            wt_tax03er_out = 0
            wt_tax04ee_out = 0
            wt_tax04er_out = 0

            wt_tax05ee_out = 0
            wt_tax05er_out = 0

            if retro_catalog_list is not None:
                for retro_catalog in retro_catalog_list:
                    pin_list = ['WT_TOTAL_OUT', 'WT_TAXBASE01EE_OUT', 'WT_TAXBASE01ER_OUT', 'WT_TAXBASE02EE_OUT',
                                'WT_TAXBASE02ER_OUT', 'WT_TAXBASE03EE_OUT', 'WT_TAXBASE03ER_OUT', 'WT_TAXBASE04EE_OUT',
                                'WT_TAXBASE04ER_OUT', 'WT_TAXBASE05EE_OUT', 'WT_TAXBASE05ER_OUT']

                    result_dic = get_db_pin_by_catalog(catalog=retro_catalog, seqnum=retro_catalog.seq_num,
                                                       pin_code_list=pin_list)
                    wt_total_out += result_dic.get('WT_TOTAL_OUT', 0)

                    wt_tax01ee_out += result_dic.get('WT_TAXBASE01EE_OUT', 0)
                    wt_tax01er_out += result_dic.get('WT_TAXBASE01ER_OUT', 0)
                    wt_tax02ee_out += result_dic.get('WT_TAXBASE02EE_OUT', 0)
                    wt_tax02er_out += result_dic.get('WT_TAXBASE02ER_OUT', 0)
                    wt_tax03ee_out += result_dic.get('WT_TAXBASE03EE_OUT', 0)
                    wt_tax03er_out += result_dic.get('WT_TAXBASE03ER_OUT', 0)
                    wt_tax04ee_out += result_dic.get('WT_TAXBASE04EE_OUT', 0)
                    wt_tax04er_out += result_dic.get('WT_TAXBASE04ER_OUT', 0)

                    wt_tax05ee_out += result_dic.get('WT_TAXBASE05EE_OUT', 0)
                    wt_tax05er_out += result_dic.get('WT_TAXBASE05ER_OUT', 0)

            gv.get_pin('WT_TOTAL_IN').segment['*'].amt = wt_total_out
            # 年终奖/离职补偿金的税基流出，统一流入工资税基
            gv.get_pin('WT_TAXBASE01EE_IN').segment['*'].amt = wt_tax01ee_out + wt_tax02ee_out + wt_tax03ee_out
            gv.get_pin('WT_TAXBASE01ER_IN').segment['*'].amt = wt_tax01er_out + wt_tax02er_out + wt_tax03er_out
            gv.get_pin('WT_TAXBASE04EE_IN').segment['*'].amt = wt_tax04ee_out
            gv.get_pin('WT_TAXBASE04ER_IN').segment['*'].amt = wt_tax04er_out

            # 劳务报酬税基(个人)流入
            gv.get_pin('WT_TAXBASE05EE_IN').segment['*'].amt = wt_tax05ee_out
            # 劳务报酬税基(公司)流入
            gv.get_pin('WT_TAXBASE05ER_IN').segment['*'].amt = wt_tax05er_out

            # 赋值历经期薪资组货币
            gv.get_pin('WT_TOTAL_IN').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAXBASE01EE_IN').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAXBASE01ER_IN').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAXBASE04EE_IN').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAXBASE04ER_IN').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAXBASE05EE_IN').segment['*'].currency = vr_pg_currency
            gv.get_pin('WT_TAXBASE05ER_IN').segment['*'].currency = vr_pg_currency

        """总额流入WT_TOTAL_IN和税基********************end"""
