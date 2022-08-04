# coding:utf-8
"""
薪资计算过程中的员工实体对象
"""
from sqlalchemy import text
from ...pysysutils import global_variables as gv


class EmpEntity:
    def __init__(self, emp):
        # 租户ID
        self.tenant_id = ''
        # 日历组ID
        self.cal_grp_id = ''
        # 日历ID
        self.cal_id = ''
        # 员工id
        self.emp_id = ''
        # 员工记录
        self.emp_rcd = ''
        # 员工姓名
        self.name = ''
        # 员工性别
        self.sex = ''
        # 国家
        self.national = ''
        # 身份证类型
        self.id_type = ''
        # 身份证号码
        self.id_no = ''
        # 生日
        self.birthday = None
        # 首次工作日期
        self.first_work_day = None
        # 调整工龄扣除
        self.work_deduct = 0
        # 民族
        self.ethnic = ''
        # 婚姻状态 Y 已婚，N：未婚
        self.marriage = ''
        # 政治面貌
        self.politic = ''
        # 籍贯
        self.native = ''
        # 户口
        self.hukou = ''
        # 宗教信仰
        self.religion = ''
        # 残障人士
        self.disabled = ''
        # 首次雇佣日期
        self.first_hire_date = None
        # 入职日期
        self.hire_date = None
        # 最后工作日
        self.last_date = None
        # 离职日期
        self.term_date = None

        # 初次雇佣日期
        self.orig_hire_date = None
        # 是否覆盖初次雇佣
        self.cover_orig_hire = 'N'
        # 集团司龄起算日期
        self.group_begin_date = None
        # 是否覆盖集团司龄起算
        self.cover_group_begin = 'N'
        # 公司司龄起算日期
        self.comp_begin_date = None
        # 是否覆盖公司司龄起算
        self.cover_comp_begin = 'N'
        # 调整公司司龄扣除
        self.company_age_deduction = 0
        # 调整集团司龄扣除
        self.group_age_deduction = 0

        # 入司时间
        self.job_date01 = None
        # 视同入司时间
        self.job_date02 = None
        # 用工开始时间
        self.job_date03 = None
        # 实习开始时间
        self.job_date04 = None
        # 实习生转派遣工时间
        self.job_date05 = None
        # 未被买断的兼并前单位入司时间
        self.job_date06 = None
        # 日期07
        self.job_date07 = None
        # 日期08
        self.job_date08 = None
        # 日期09
        self.job_date09 = None
        # 日期10
        self.job_date10 = None

        self.tenant_id = emp.tenant_id
        self.cal_id = emp.cal_id
        self.emp_id = emp.emp_id
        self.emp_rcd = emp.emp_rcd
        # 任职信息（分段）
        self.job_data_dic = {}
        sql = text(
            'SELECT a.hhr_emp_name, a.hhr_gender, a.hhr_country, a.hhr_certificate_type, a.hhr_certificate_num, a.hhr_birth_date, '
            'b.hhr_firstworking_date, b.hhr_yearsworking_deduction, b.hhr_nation, b.hhr_marital_status, b.hhr_politics_status, '
            'b.hhr_account_type, b.hhr_religion, dt.hhr_orig_hire_date, dt.hhr_cover_orig_hire, dt.hhr_group_begin_date, '
            'dt.hhr_cover_group_begin, dt.hhr_comp_begin_date, dt.hhr_cover_comp_begin, dt.hhr_comp_age_dedu, dt.hhr_group_age_dedu, '
            'dt.hhr_last_hire_date, dt.hhr_termination_date, dt.hhr_last_date_worked, '
            'dt.hhr_org_per_job_date01, dt.hhr_org_per_job_date02, dt.hhr_org_per_job_date03, dt.hhr_org_per_job_date04, dt.hhr_org_per_job_date05, '
            'dt.hhr_org_per_job_date06, dt.hhr_org_per_job_date07, dt.hhr_org_per_job_date08, dt.hhr_org_per_job_date09, dt.hhr_org_per_job_date10 '
            'FROM boogoo_corehr.hhr_org_per_biog a LEFT JOIN boogoo_corehr.hhr_org_per_additional b ON b.tenant_id = a.tenant_id '
            'AND b.hhr_empid = a.hhr_empid LEFT JOIN boogoo_corehr.hhr_org_per_job_date dt ON dt.tenant_id = a.tenant_id '
            'AND dt.hhr_empid = a.hhr_empid WHERE a.tenant_id = :b1 AND a.hhr_empid = :b2')
        self.db = gv.get_db()
        result = self.db.conn.execute(sql, b1=self.tenant_id, b2=self.emp_id).fetchone()
        if result is not None:
            self.name = result['hhr_emp_name']
            self.sex = result['hhr_gender']
            self.national = result['hhr_country']
            self.id_type = result['hhr_certificate_type']
            self.id_no = result['hhr_certificate_num']
            self.birthday = result['hhr_birth_date']
            self.first_work_day = result['hhr_firstworking_date']
            work_deduct = result['hhr_yearsworking_deduction']
            if work_deduct is None:
                work_deduct = 0
            self.work_deduct = work_deduct
            self.ethnic = result['hhr_nation']
            self.marriage = result['hhr_marital_status']
            self.politic = result['hhr_politics_status']
            self.hukou = result['hhr_account_type']
            self.religion = result['hhr_religion']

            # 初次雇佣日期
            self.orig_hire_date = result['hhr_orig_hire_date']
            # 是否覆盖初次雇佣
            self.cover_orig_hire = result['hhr_cover_orig_hire']
            # 集团司龄起算日期
            self.group_begin_date = result['hhr_group_begin_date']
            # 是否覆盖集团司龄起算
            self.cover_group_begin = result['hhr_cover_group_begin']
            # 公司司龄起算日期
            self.comp_begin_date = result['hhr_comp_begin_date']
            # 是否覆盖公司司龄起算
            self.cover_comp_begin = result['hhr_cover_comp_begin']
            # 调整公司司龄扣除
            self.company_age_deduction = result['hhr_comp_age_dedu']
            # 调整集团司龄扣除
            self.group_age_deduction = result['hhr_group_age_dedu']

            # 入职日期
            self.hire_date = result['hhr_last_hire_date']
            # 离职日期
            self.term_date = result['hhr_termination_date']
            # 最后工作日
            self.last_date = result['hhr_last_date_worked']
            self.job_date01 = result['hhr_org_per_job_date01']
            self.job_date02 = result['hhr_org_per_job_date02']
            self.job_date03 = result['hhr_org_per_job_date03']
            self.job_date04 = result['hhr_org_per_job_date04']
            self.job_date05 = result['hhr_org_per_job_date05']
            self.job_date06 = result['hhr_org_per_job_date06']
            self.job_date07 = result['hhr_org_per_job_date07']
            self.job_date08 = result['hhr_org_per_job_date08']
            self.job_date09 = result['hhr_org_per_job_date09']
            self.job_date10 = result['hhr_org_per_job_date10']

        # catalog = gv.get_run_var_value('PY_CATALOG')
        # sql = text("SELECT a.hhr_action_type_code, a.hhr_orig_hire_date, a.hhr_last_hire_date, a.hhr_last_date_worked, a.hhr_termination_date "
        #            "FROM boogoo_corehr.hhr_org_per_jobdata a WHERE a.tenant_id = :b1 AND a.hhr_empid = :b2 AND a.hhr_job_indicator = 'P' "
        #            "AND now() BETWEEN a.hhr_efft_date AND a.hhr_efft_end_date AND a.hhr_efft_seq = (SELECT max(a1.hhr_efft_seq) "
        #            "FROM boogoo_corehr.hhr_org_per_jobdata a1 WHERE a1.tenant_id = a.tenant_id AND a1.hhr_empid = a.hhr_empid "
        #            "AND a1.hhr_emp_rcd = a.hhr_emp_rcd AND a1.hhr_efft_date = a.hhr_efft_date )")
        # result = self.db.conn.execute(sql, b1=self.tenant_id, b2=self.emp_id,
        #                               b3=catalog.f_prd_end_dt).fetchone()
        # if result is not None:
        #     self.first_hire_date = result['hhr_orig_hire_date']
        #     self.hire_date = result['hhr_last_hire_date']
        #     self.last_date = result['hhr_last_date_worked']
        #     self.term_date = result['hhr_termination_date']
