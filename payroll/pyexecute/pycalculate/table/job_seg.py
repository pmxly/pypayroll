# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class JobSeg:
    """
    Desc: 任职记录分段表 HHR_PY_RSLT_JOB
    Author: David
    Date: 2018/08/27
    """

    __slots__ = [
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'segment_num',
        'eff_seq',
        'action_cd',
        'reason_cd',
        'relation',
        'hr_status',
        'emp_class',
        'sub_emp_class',
        'company_cd',
        'position_cd',
        'dept_cd',
        'bu_cd',
        'location',
        'direct',
        'indirect',
        'job_cd',
        'group_cd',
        'sequence_cd',
        'rank_cd',

        'job_data_attr01',
        'job_data_attr03',
        'job_data_attr09',
        'job_data_attr10',
        'job_type_cd',

        'action_type_code',
        'action_reason_code',
        'py_group_id',
        'apply_scope',
        'tax_location',
        'maintain_si_phf',
        'prcs_pt_rslt',
        'resident_type',
        'tax_type',
        'tax_year_mapping',
        'pay_method',
        'per_rank',
        'py_curve_type',
        'year_salary',
        'sal_plan_cd',
        'sal_grade_cd',
        'sal_step_cd',
        'currency',
        'probation_pay_ratio',
        'probation_retain_pay_ratio',
        'calc_method',
        'rank_pay_ratio',
        'retain_pay_ratio',
        'perf_pay_ratio',
        'probation_flag'

        # 'py_group_id',
        # 'apply_scope',
        # 'tax_location',
        # 'maintain_si_phf',
        # 'sal_plan_cd',
        # 'sal_grade_cd',
        # 'sal_step_cd'
    ]

    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.segment_num = kwargs.get('segment_num', 0)
        self.eff_seq = kwargs.get('eff_seq', 0)
        self.action_cd = kwargs.get('action_cd', '')
        self.reason_cd = kwargs.get('reason_cd', '')
        self.relation = kwargs.get('relation', '')
        self.hr_status = kwargs.get('hr_status', '')
        self.emp_class = kwargs.get('emp_class', '')
        self.sub_emp_class = kwargs.get('sub_emp_class', '')
        self.company_cd = kwargs.get('company_cd', '')
        self.position_cd = kwargs.get('position_cd', '')
        self.dept_cd = kwargs.get('dept_cd', '')
        self.bu_cd = kwargs.get('bu_cd', '')
        self.location = kwargs.get('location', '')
        self.direct = kwargs.get('direct', '')
        self.indirect = kwargs.get('indirect', '')
        self.job_cd = kwargs.get('job_cd', '')
        self.group_cd = kwargs.get('group_cd', '')
        self.sequence_cd = kwargs.get('sequence_cd', '')
        self.rank_cd = kwargs.get('rank_cd', '')

        self.job_data_attr01 = kwargs.get('job_data_attr01', '')
        self.job_data_attr03 = kwargs.get('job_data_attr03', 'N')
        self.job_data_attr09 = kwargs.get('job_data_attr09', '')
        self.job_data_attr10 = kwargs.get('job_data_attr10', '')
        self.job_type_cd = kwargs.get('job_type_cd', '')

        self.action_type_code = kwargs.get('action_type_code', '')
        self.action_reason_code = kwargs.get('action_reason_code', '')
        self.py_group_id = kwargs.get('py_group_id', '')
        self.apply_scope = kwargs.get('apply_scope', '')
        self.tax_location = kwargs.get('tax_location', '')
        self.maintain_si_phf = kwargs.get('maintain_si_phf', '')
        self.prcs_pt_rslt = kwargs.get('prcs_pt_rslt', '')
        self.resident_type = kwargs.get('resident_type', '')
        self.tax_type = kwargs.get('tax_type', '')
        self.tax_year_mapping = kwargs.get('tax_year_mapping', '')
        self.pay_method = kwargs.get('pay_method', '')
        self.per_rank = kwargs.get('per_rank', '')
        self.py_curve_type = kwargs.get('py_curve_type', '')
        self.year_salary = kwargs.get('year_salary', 0)
        self.sal_plan_cd = kwargs.get('sal_plan_cd', '')
        self.sal_grade_cd = kwargs.get('sal_grade_cd', '')
        self.sal_step_cd = kwargs.get('sal_step_cd', '')
        self.currency = kwargs.get('currency', '')
        self.probation_pay_ratio = kwargs.get('probation_pay_ratio', 0)
        self.probation_retain_pay_ratio = kwargs.get('probation_retain_pay_ratio', 0)
        self.calc_method = kwargs.get('calc_method', '')
        self.rank_pay_ratio = kwargs.get('rank_pay_ratio', 0)
        self.retain_pay_ratio = kwargs.get('retain_pay_ratio', 0)
        self.perf_pay_ratio = kwargs.get('perf_pay_ratio', 0)
        self.probation_flag = kwargs.get('probation_flag', '')

    def insert(self):
        db = gv.get_db()
        run_parm = gv.get_run_var_value('RUN_PARM')
        seg = db.get_table('hhr_py_rslt_job', schema_name='boogoo_payroll')
        ins = seg.insert()
        val = [
            {'tenant_id': self.tenant_id,
             'hhr_empid': self.emp_id,
             'hhr_emp_rcd': self.emp_rcd,
             'hhr_seq_num': self.seq_num,
             'hhr_segment_num': self.segment_num,
             'hhr_efft_seq': self.eff_seq,
             'hhr_action_type_code': self.action_cd,
             'hhr_action_reason_code': self.reason_cd,
             'hhr_relation': self.relation,
             'hhr_status': self.hr_status,
             'hhr_emp_class': self.emp_class,
             'hhr_sub_emp_class': self.sub_emp_class,
             'hhr_company_code': self.company_cd,
             'hhr_posn_code': self.position_cd,
             'hhr_dept_code': self.dept_cd,
             'hhr_bu_code': self.bu_cd,
             'hhr_location': self.location,
             'hhr_direct_rpt_posn': self.direct,
             'hhr_dotted_rpt_posn': self.indirect,
             'hhr_job_code': self.job_cd,
             'hhr_group_code': self.group_cd,
             'hhr_sequence_code': self.sequence_cd,
             'hhr_rank_code': self.rank_cd,

             'hhr_org_per_jobdata_attr01': self.job_data_attr01,
             'hhr_org_per_jobdata_attr03': self.job_data_attr03,
             'hhr_org_per_jobdata_attr09': self.job_data_attr09,
             'hhr_org_per_jobdata_attr10': self.job_data_attr10,
             'hhr_job_type_cd': self.job_type_cd,

             'hhr_action_type_code1': self.action_type_code,
             'hhr_action_reason_code2': self.action_reason_code,
             'hhr_pygroup_id': self.py_group_id,
             'hhr_app_scope_code': self.apply_scope,
             'hhr_tax_location': self.tax_location,
             'hhr_maintn_si_phf': self.maintain_si_phf,
             'hhr_prcs_pt_rslt': self.prcs_pt_rslt,
             'hhr_resident_type': self.resident_type,
             'hhr_tax_type': self.tax_type,
             'hhr_tax_year_mapping': self.tax_year_mapping,
             'hhr_pay_method': self.pay_method,
             'hhr_per_rank': self.per_rank,
             'hhr_py_curve_type': self.py_curve_type,
             'hhr_year_salary': self.year_salary,
             'hhr_sal_plan_cd': self.sal_plan_cd,
             'hhr_sal_grade_cd': self.sal_grade_cd,
             'hhr_sal_step_cd': self.sal_step_cd,
             'hhr_currency': self.currency,
             'hhr_probation_pay_ratio': self.probation_pay_ratio,
             'hhr_probation_retain_pay_ratio': self.probation_retain_pay_ratio,
             'hhr_calc_method': self.calc_method,
             'hhr_rank_pay_ratio': self.rank_pay_ratio,
             'hhr_retain_pay_ratio': self.retain_pay_ratio,
             'hhr_perf_pay_ratio': self.perf_pay_ratio,
             'hhr_probation_flag': self.probation_flag,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': run_parm['operator_user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': run_parm['operator_user_id'],
             }]
        db.conn.execute(ins, val)
