# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pyexecute.pycalculate.siphf.siphf_process import SiPhfProcess
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 计算社保/公积金
    Author: David
    Date: 2018/10/11
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_CALC_SI_PHF'
        self.country = 'CHN'
        self.desc = '计算社保/公积金函数'
        self.descENG = '计算社保/公积金函数'
        self.func_type = 'B'
        self.instructions = "计算社保/公积金函数"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': ['WT_TAXBASE_PHF_EE', 'WT_TAXBASE_PHF_ER', 'WT_TAXFREE_PHF_EE', 'WT_TAXFREE_PHF_ER'],
                'VR': ['VR_SIPHFID', 'VR_TAXAREA', 'VR_F_PERIOD_YEAR', 'VR_F_PERIOD_NO', 'VR_PG_CURRENCY'],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        # 当员工在此任职上缴交社保公积金（VR_SIPHFID=Y）时，计算公积金的具体金额
        cur_cal = gv.get_run_var_value('CUR_CAL_OBJ')
        # 非周期的运行类型不包括基础薪酬、经常性支付/扣除、考勤处理、社保&公积金、最低工资处理

        """【逻辑调整】
        Date: 2021/04/14
        社保公积金函数（FC_CALC_SI_PHF）中最后一部分是处理公积金超额计税的，这部分的调整，即使VR_SIPHFID不是Y也正常处理公积金超额计税部分的逻辑。
        【超额计税部分的处理不需要受条件控制】
        前面计算部分的逻辑（计算社保公积金金额）不受影响，还是原来的逻辑，继续受条件控制。
        """
        # if cur_cal.run_type_entity.cycle == 'C':
        #     vr_si_phf = gv.get_var_value('VR_SIPHFID')
        #     if vr_si_phf == 'Y':
        #         # 按历经期处理社保公积金
        #         catalog = gv.get_run_var_value('PY_CATALOG')
        #         phf_process_obj = SiPhfProcess(fc_obj=self, catalog=catalog)
        #         phf_process_obj.process_si_phf()

        if cur_cal.run_type_entity.cycle == 'C':
            # 按历经期处理社保公积金
            catalog = gv.get_run_var_value('PY_CATALOG')
            phf_process_obj = SiPhfProcess(fc_obj=self, catalog=catalog)
            phf_process_obj.process_si_phf()
