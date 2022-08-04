import datetime
from ...pyexecute.pycalculate.table.dept_assoc import DeptAssoc
from ...pyexecute.pycalculate.table.posn_assoc import PosnAssoc
from ...pyexecute.pycalculate.table.posn import Posn
from ...pyexecute.pycalculate.table.job_seg import JobSeg
from ...pyexecute.pycalculate.table.emp_cst_cntr import EmpCostCenter


class ReturnObj:

    def __init__(self):
        self.acc_type = ''
        self.acc_year = 1900
        self.acc_num = 0

        self.amt = 0
        self.currency = ''

        self.avg_anl_sal = 1
        self.avg_mon_sal = 1
        self.min_mon_sal = 1


class gv:

    @staticmethod
    def get_run_var_value(a):
        if a == 'SEG_INFO_OBJ' or a == 'PREV_SEG_INFO_OBJ':
            seg_info = ReturnObj()
            seg_info.period_seg_dic = {}
            seg_info.job_seg_dic = {}
            return seg_info
        else:
            return {}


def FC_ASGN_PT_CD(pt_code, pt_date, quantity, unit):
    if isinstance(pt_date, str):
        try:
            pt_date = datetime.datetime.strptime(pt_date, "%Y-%m-%d")
        except (TypeError, ValueError):
            try:
                pt_date = datetime.datetime.strptime(pt_date, "%Y/%m/%d")
            except (TypeError, ValueError):
                raise Exception('Invalid date parameter')
    elif not isinstance(pt_date, datetime.date):
        raise Exception('Invalid date parameter')


def FC_BASE_SAL():
    pass


def FC_BR(bracket_cd):
    pass


def FC_CALC_AGE(birth_dt, f_period_end_dt, rule_id):
    if isinstance(birth_dt, str) and isinstance(f_period_end_dt, str):
        try:
            birth_dt = datetime.datetime.strptime(birth_dt, "%Y-%m-%d")
            f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y-%m-%d")
        except (TypeError, ValueError):
            try:
                birth_dt = datetime.datetime.strptime(birth_dt, "%Y/%m/%d")
                f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y/%m/%d")
            except (TypeError, ValueError):
                raise Exception('Invalid date parameter')
    elif not isinstance(birth_dt, datetime.date) or not isinstance(f_period_end_dt, datetime.date):
        raise Exception('Invalid date parameter')
    return 0


def FC_CALC_COMP_YEAR(hire_dt, f_period_end_dt, rule_id):
    if isinstance(hire_dt, str) and isinstance(f_period_end_dt, str):
        try:
            hire_dt = datetime.datetime.strptime(hire_dt, "%Y-%m-%d")
            f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y-%m-%d")
        except (TypeError, ValueError):
            try:
                hire_dt = datetime.datetime.strptime(hire_dt, "%Y/%m/%d")
                f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y/%m/%d")
            except (TypeError, ValueError):
                raise Exception('Invalid date parameter')
    elif not isinstance(hire_dt, datetime.date) or not isinstance(f_period_end_dt, datetime.date):
        raise Exception('Invalid date parameter')
    return 0


def FC_CALC_SI_PHF():
    pass


def FC_CALC_SPCL_INSUR(insur_type, pin_ee_cd, pin_er_cd):
    pass


def FC_CALC_TAX():
    pass


def FC_CALC_WORK_YEAR(first_work_dt, f_period_end_dt, work_deduct, rule_id):
    if isinstance(first_work_dt, str) and isinstance(f_period_end_dt, str):
        try:
            first_work_dt = datetime.datetime.strptime(first_work_dt, "%Y-%m-%d")
            f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y-%m-%d")
        except (TypeError, ValueError):
            try:
                first_work_dt = datetime.datetime.strptime(first_work_dt, "%Y/%m/%d")
                f_period_end_dt = datetime.datetime.strptime(f_period_end_dt, "%Y/%m/%d")
            except (TypeError, ValueError):
                raise Exception('Invalid date parameter')
    elif not isinstance(first_work_dt, datetime.date) or not isinstance(f_period_end_dt, datetime.date):
        raise Exception('Invalid date parameter')
    return 0


def FC_CALCULATE_YEAR(bgn_dt, end_dt, rule_id):
    if isinstance(bgn_dt, str) and isinstance(end_dt, str):
        try:
            bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y-%m-%d")
            end_dt = datetime.datetime.strptime(end_dt, "%Y-%m-%d")
        except (TypeError, ValueError):
            try:
                bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y/%m/%d")
                end_dt = datetime.datetime.strptime(end_dt, "%Y/%m/%d")
            except (TypeError, ValueError):
                raise Exception('Invalid date parameter')
    elif not isinstance(bgn_dt, datetime.date) or not isinstance(end_dt, datetime.date):
        raise Exception('Invalid date parameter')
    return 0


def FC_CHECK_ACTION(action, bgn_dt, end_dt):
    if isinstance(bgn_dt, str) and isinstance(end_dt, str):
        try:
            bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y-%m-%d")
            end_dt = datetime.datetime.strptime(end_dt, "%Y-%m-%d")
        except (TypeError, ValueError):
            try:
                bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y/%m/%d")
                end_dt = datetime.datetime.strptime(end_dt, "%Y/%m/%d")
            except (TypeError, ValueError):
                raise Exception('Invalid date parameter')
    elif not isinstance(bgn_dt, datetime.date) or not isinstance(end_dt, datetime.date):
        raise Exception('Invalid date parameter')
    return -1


def FC_CHECK_WT_ELIGIBLE(wt_cd):
    return 'Y'


def FC_COMM_RETRO(pin_code):
    pass


def FC_COMM_WT_IN(src_pin_cd, tgt_pin_cd, tuple_data_list=None):
    pass


def FC_COMM_WT_OUT(wc_code, pin_code):
    pass


def FC_COPY_HIST_VR(src_var_cd, trg_var_cd):
    pass


def FC_COPY_HIST_WT(src_pin_cd, tgt_pin_cd):
    pass


def FC_CORR_PROCESS():
    pass


def FC_DETAIL_PAY():
    pass


def FC_ELEM_OVERRIDE():
    pass


def FC_EMP_COMPANY():
    return JobSeg()


def FC_EMP_JOB_DATA_FLD(field, to_date=None):
    return ''


def FC_EVAL_PT_CD():
    return None


def FC_EX_RATE(from_cur, to_cur, from_amt):
    return 0


def FC_GET_ABS_LST():
    return []


def FC_GET_ACC_CAL(acc_type):
    return ReturnObj()


def FC_GET_DEPT_ASSOC(dept_cd, to_date=None):
    return DeptAssoc(**{})


def FC_GET_DEPT_ASSOC_FLD(dept_cd, field_name, to_date=None):
    return None


def FC_GET_DEPT_POSN(to_date=None):
    return '', ''


def FC_GET_DISP_COMPANY(to_date=None):
    return None


def FC_GET_EDU_LEVEL(option):
    return None


def FC_GET_EMP_COST_CENTER(to_date=None):
    return [EmpCostCenter(**{})]


def FC_GET_HIRE_REASON():
    return None


def FC_GET_JOBDATA(seg_num=0):
    pass


def FC_GET_MATERNITY_LEAVE():
    return None, 0


def FC_GET_MIN_SAL(area, year, end_dt):
    return ReturnObj()


def FC_GET_MULTI_ABILITY(to_date=None):
    return [(None, None)]


def FC_GET_OCCUP_INJURY_BGN_DT():
    return None


def FC_GET_ORG_COST_CENTER(dept_cd, to_date=None):
    return '', '', ''


def FC_GET_PERF_INFO(eval_type, year, period):
    return 0, None, 0, 0, 0


def FC_GET_POSN_ASSOC(posn_cd, to_date=None):
    return PosnAssoc(**{})


def FC_GET_POSN_INFO(posn_cd, to_date=None):
    return Posn(**{})


def FC_GET_POSN_JOB(posn_cd, to_date=None):
    return '', '', ''


def FC_GET_PREV_ACC(acc_cd):
    return ReturnObj()


def FC_GET_PREV_MONTHS_SAL(to_date, mon_count, pin_cd):
    return 0, 0


def FC_GET_PREV_PIN(pin_cd):
    return ReturnObj()


def FC_GET_PREV_VAR(var_cd):
    return None


def FC_GET_PT_REC():
    pass


def FC_GET_PT_REC_MONTH(month_flag):
    return []


def FC_GET_RENTAL_INFO(to_date=None):
    return None


def FC_GET_SAL_LOCK_STATUS():
    return 'N'


def FC_GET_SEG(seg_num=0):
    pass


def FC_GET_SEGFACT(seg_rule_cd, seg_num=0):
    pass


def FC_GET_SKILL_LEVEL(to_date=None):
    return None


def FC_LOAD_PINS():
    pass


def FC_ONETIME_PAY():
    pass


def FC_PERIOD_SEG():
    pass


def FC_PREV_PERIOD_SEG():
    pass


def FC_RAISE_EXCEPTION(error_txt):
    pass


def FC_RECURRING_PAY():
    pass


def FC_RETRO_PROCESS():
    pass


def FC_ROUND(input_val, rule_id):
    return 0


def FC_SPLIT_FACTOR():
    pass


def FC_SUM_PT_CD(pt_code):
    return 0, None


def FC_TAX_BASE():
    pass


def FC_WA(wa_id):
    pass


def FC_WT_IN():
    pass


def FC_WT_NET():
    pass


def FC_WT_TAXBASE_OUT():
    pass


def FC_WT_TOTAL():
    pass


def FC_WT_TOTAL_OUT():
    pass


def FC_YT_ABS_PTCODE(pt_code):
    pass


def FC_YT_ATT_PTCODE(att_type, pt_code):
    pass


def FC_YT_CONSTANT_STD(item_cd):
    pass
