# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from sqlalchemy.sql import text
from ..table.period_seg import PeriodSeg
from ..table.job_seg import JobSeg
from ..table.work_cal import WorkCal
from ..table.seg_factor import SegFactor
from datetime import timedelta, datetime
from decimal import Decimal

one_day = timedelta(days=1)


class SegItem(object):
    """
    Desc: 分段条目
    Author: David
    Date: 2018/08/23
    """

    __slots__ = ['segment_num', 'bgn_dt', 'end_dt',
                 'numerator_val', 'denominator_val', 'factor_val']

    def __init__(self, **kwargs):
        self.segment_num = kwargs.get('segment_num', 0)
        self.bgn_dt = kwargs.get('bgn_dt', None)
        self.end_dt = kwargs.get('end_dt', None)
        self.numerator_val = kwargs.get('numerator_val', 0)
        self.denominator_val = kwargs.get('denominator_val', 0)
        factor_val = Decimal(str(kwargs.get('factor_val', 0)))
        self.factor_val = factor_val


class SegInfo(object):
    """
    Desc: 分段信息
    Author: David
    Date: 2018/08/23
    """

    __slots__ = ['seg_rule_list', 'period_seg_lst', 'period_seg_dic',
                 'job_seg_dic', 'wk_cal_list', 'seg_fact_dic', 'seg_items_dic_']

    def __init__(self):
        self.seg_rule_list = []  # 分段规则列表
        self.period_seg_lst = []  # 期间分段列表
        self.period_seg_dic = {}  # 期间分段字典
        self.job_seg_dic = {}  # 任职信息分段列表
        self.wk_cal_list = []  # 工作日历列表
        self.seg_fact_dic = {}  # 分段因子字典-记录分段规则在各分段上的因子值
        self.seg_items_dic_ = {}  # 分段条目对象字典,key为分段规则ID或者*,value为{1:SegItem1, 2:SegItem2}形式的字典
        self.process_seg()

    @property
    def seg_items_dic(self):
        if len(self.seg_items_dic_) == 0:
            for rule in self.seg_rule_list:
                rule_cd = rule['rule_cd']
                seg_fact_list = self.seg_fact_dic[rule_cd]
                temp_dic = {}
                for ps in self.period_seg_lst:
                    data = {}
                    segment_num = ps.segment_num
                    data['segment_num'] = segment_num
                    data['bgn_dt'] = ps.seg_bgn_dt
                    data['end_dt'] = ps.seg_end_dt
                    for fact in seg_fact_list:
                        if fact.segment_num == segment_num:
                            data['numerator_val'] = fact.numerator_val
                            data['denominator_val'] = fact.denominator_val
                            data['factor_val'] = fact.factor_val
                            break
                    item = SegItem(**data)
                    temp_dic[segment_num] = item
                self.seg_items_dic_[rule_cd] = temp_dic

            temp_dic = {}
            for ps in self.period_seg_lst:
                data = {}
                segment_num = ps.segment_num
                data['segment_num'] = segment_num
                data['bgn_dt'] = ps.seg_bgn_dt
                data['end_dt'] = ps.seg_end_dt
                item = SegItem(**data)
                temp_dic[segment_num] = item
            self.seg_items_dic_['*'] = temp_dic
        return self.seg_items_dic_

    def build_job_seg(self, segment_num, seg_end_dt, job_sql, period_len):
        """
        Desc: 针对每个分段，获取任职记录
        Author: David
        Date: 2018/08/29
        :return:None
        """
        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num
        r = db.conn.execute(job_sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seg_end_dt).fetchone()
        if r is not None:
            eff_seq = r['hhr_efft_seq']
            action_cd = r['hhr_action_type_code']
            reason_cd = r['hhr_action_reason_code']
            relation = r['hhr_relation']
            hr_status = r['hhr_status']
            emp_class = r['hhr_emp_class']
            emp_sub_class = r['hhr_sub_emp_class']
            company_cd = r['hhr_company_code']
            position_cd = r['hhr_posn_code']
            dept_cd = r['hhr_dept_code']
            bu_cd = r['hhr_bu_code']
            location = r['hhr_location']
            direct = r['hhr_direct_rpt_posn']
            indirect = r['hhr_dotted_rpt_posn']
            job_cd = r['hhr_job_code']
            group_cd = r['hhr_group_code']
            sequence_cd = r['hhr_sequence_code']
            rank_cd = r['hhr_rank_code']

            job_data_attr01 = r['hhr_org_per_jobdata_attr01']
            job_data_attr03 = r['hhr_org_per_jobdata_attr03']
            job_data_attr09 = r['hhr_org_per_jobdata_attr09']
            job_data_attr10 = r['hhr_org_per_jobdata_attr10']
            job_type_cd = r['hhr_org_job_attr01']

            prob_bgn_date = r['prob_bgn_date']

            v_dict = {'tenant_id': tenant_id,
                      'emp_id': emp_id,
                      'emp_rcd': emp_rcd,
                      'seq_num': seq_num,
                      'segment_num': segment_num,
                      'eff_seq': eff_seq,
                      'action_cd': action_cd,
                      'reason_cd': reason_cd,
                      'relation': relation,
                      'hr_status': hr_status,
                      'emp_class': emp_class,
                      'sub_emp_class': emp_sub_class,
                      'company_cd': company_cd,
                      'position_cd': position_cd,
                      'dept_cd': dept_cd,
                      'bu_cd': bu_cd,
                      'location': location,
                      'direct': direct,
                      'indirect': indirect,
                      'job_cd': job_cd,
                      'group_cd': group_cd,
                      'sequence_cd': sequence_cd,
                      'rank_cd': rank_cd,

                      'job_data_attr01': job_data_attr01,
                      'job_data_attr03': job_data_attr03,
                      'job_data_attr09': job_data_attr09,
                      'job_data_attr10': job_data_attr10,
                      'job_type_cd': job_type_cd
                      }

            if segment_num == period_len:
                gv.set_var_value('VR_ACTION', action_cd)  # 操作
                gv.set_var_value('VR_ACREASON', reason_cd)  # 原因
                gv.set_var_value('VR_RELATION', relation)  # 聘用关系
                gv.set_var_value('VR_HRSTATUS', hr_status)  # 人员状态
                gv.set_var_value('VR_EMPCLASS', emp_class)  # 人员类别
                gv.set_var_value('VR_EMPSUBCLASS', emp_sub_class)  # 人员子类别
                gv.set_var_value('VR_COMPANY', company_cd)  # 公司
                gv.set_var_value('VR_POSITION', position_cd)  # 职位
                gv.set_var_value('VR_DEPARTMENT', dept_cd)  # 部门
                gv.set_var_value('VR_BU', bu_cd)  # 业务单位
                gv.set_var_value('VR_LOCATION', location)  # 工作地点
                gv.set_var_value('VR_RPTPOST', direct)  # 直接汇报职位
                gv.set_var_value('VR_INRPTPOST', indirect)  # 虚线汇报职位
                gv.set_var_value('VR_JOB', job_cd)  # 职务
                gv.set_var_value('VR_JOBGROUP', group_cd)  # 族群
                gv.set_var_value('VR_JOBFAMILY', sequence_cd)  # 序列
                gv.set_var_value('VR_JOBGRADE', rank_cd)  # 职级

                gv.set_var_value('VR_JOBDATA01', job_data_attr01)  # 人员类别
                gv.set_var_value('VR_JOBDATA03', job_data_attr03)  # 是否大专班
                gv.set_var_value('VR_JOBDATA09', job_data_attr09)  # 员工组
                gv.set_var_value('VR_JOBDATA10', job_data_attr10)  # 员工子组
                gv.set_var_value('VR_JOBTYPE', job_type_cd)  # 职种

            # 根据历经期结束日期，从基础薪酬中获取记录。将各薪资相关字段赋值给相应的变量
            sql = text(
                "select a.hhr_action_type_code, a.hhr_action_reason_code, a.hhr_pygroup_id, a.hhr_app_scope_code, a.hhr_tax_location, a.hhr_maintn_si_phf, a.hhr_prcs_pt_rslt, "
                "a.hhr_resident_type, a.hhr_tax_type, a.hhr_tax_year_mapping, a.hhr_pay_method, "
                "a.hhr_per_rank, a.hhr_py_curve_type, a.hhr_year_salary, a.hhr_sal_plan_cd, a.hhr_sal_grade_cd, a.hhr_sal_step_cd, a.hhr_currency, "
                "a.hhr_probation_pay_ratio, a.hhr_probation_retain_pay_ratio, a.hhr_calc_method, "
                "a.hhr_rank_pay_ratio, a.hhr_retain_pay_ratio, a.hhr_perf_pay_ratio "
                "from boogoo_payroll.hhr_py_assign_pg a where a.tenant_id = :b1 "
                "and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 and a.hhr_efft_date <= :b4 order by hhr_efft_date desc, hhr_efft_seq desc ")
            r = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seg_end_dt).fetchone()
            if r is not None:
                v_dict['action_type_code'] = r['hhr_action_type_code']
                v_dict['action_reason_code'] = r['hhr_action_reason_code']
                v_dict['py_group_id'] = r['hhr_pygroup_id']
                v_dict['apply_scope'] = r['hhr_app_scope_code']
                v_dict['tax_location'] = r['hhr_tax_location']
                v_dict['maintain_si_phf'] = r['hhr_maintn_si_phf']
                v_dict['prcs_pt_rslt'] = r['hhr_prcs_pt_rslt']
                v_dict['resident_type'] = r['hhr_resident_type']
                v_dict['tax_type'] = r['hhr_tax_type']
                v_dict['tax_year_mapping'] = r['hhr_tax_year_mapping']
                v_dict['pay_method'] = r['hhr_pay_method']
                v_dict['per_rank'] = r['hhr_per_rank']
                v_dict['py_curve_type'] = r['hhr_py_curve_type']
                v_dict['year_salary'] = r['hhr_year_salary']
                v_dict['sal_plan_cd'] = r['hhr_sal_plan_cd']
                v_dict['sal_grade_cd'] = r['hhr_sal_grade_cd']
                v_dict['sal_step_cd'] = r['hhr_sal_step_cd']
                v_dict['currency'] = r['hhr_currency']
                v_dict['probation_pay_ratio'] = 0 if r['hhr_probation_pay_ratio'] is None else r[
                    'hhr_probation_pay_ratio']
                v_dict['probation_retain_pay_ratio'] = 0 if r['hhr_probation_retain_pay_ratio'] is None else r[
                    'hhr_probation_retain_pay_ratio']
                v_dict['calc_method'] = r['hhr_calc_method']
                v_dict['rank_pay_ratio'] = 0 if r['hhr_rank_pay_ratio'] is None else r['hhr_rank_pay_ratio']
                v_dict['retain_pay_ratio'] = 0 if r['hhr_retain_pay_ratio'] is None else r['hhr_retain_pay_ratio']
                v_dict['perf_pay_ratio'] = 0 if r['hhr_perf_pay_ratio'] is None else r['hhr_perf_pay_ratio']

                if segment_num == period_len:
                    # gv.set_var_value('VR_JOBPYGROUP', job_seg.py_group_id       # 任职薪资组
                    gv.set_var_value('VR_SCOPE', r['hhr_app_scope_code'])  # 适用范围
                    gv.set_var_value('VR_TAXAREA', r['hhr_tax_location'])  # 纳税地
                    gv.set_var_value('VR_SIPHFID', r['hhr_maintn_si_phf'])  # 缴交社保公积金
                    gv.set_var_value('VR_CAL_PTPY', r['hhr_prcs_pt_rslt'])  # 处理考勤结果
                    gv.set_var_value('VR_RESIDENT_TYPE', r['hhr_resident_type'])  # 居民类型
                    gv.set_var_value('VR_TAX_TYPE', r['hhr_tax_type'])  # 税类型
                    gv.set_var_value('VR_TAX_MAP', r['hhr_tax_year_mapping'])  # 税务年度映射
                    gv.set_var_value('VR_PY_METHOD', r['hhr_pay_method'])  # 发薪方式
                    gv.set_var_value('VR_PY_GRADE', r['hhr_per_rank'])  # 个人职级
                    gv.set_var_value('VR_PY_CURVE', r['hhr_py_curve_type'])  # 薪酬曲线
                    gv.set_var_value('VR_PYPLAN', r['hhr_sal_plan_cd'])  # 薪资计划
                    gv.set_var_value('VR_PYGRADE', r['hhr_sal_grade_cd'])  # 薪等
                    gv.set_var_value('VR_PYSTEP', r['hhr_sal_step_cd'])  # 薪级

            """
            根据分段结束日期从当前员工的试用期表中取数：hhr_begin_date<=分段结束日期<=hhr_end_date,
            未取到记录，试用期标识=N，
            取到记录(员工可能提前转正)，判断主职是否已转正：hhr_action_type_code='PRO'，取到记录的试用期开始日期<=hhr_efft_date<=分段结束日期，
            未取到转正记录，试用期标识=Y，
            取到转正记录，试用期标识=N
            """
            if prob_bgn_date is None:
                probation_flag = 'N'
            else:
                pro_sql = text(
                    "select j.hhr_empid from boogoo_corehr.hhr_org_per_jobdata j where j.tenant_id = :b1 and j.hhr_empid = :b2 "
                    "and j.hhr_job_indicator = 'P' and j.hhr_action_type_code='PRO' and j.hhr_efft_date BETWEEN :b3 AND :b4")
                r = db.conn.execute(pro_sql, b1=tenant_id, b2=emp_id, b3=prob_bgn_date, b4=seg_end_dt).fetchone()
                if r is not None:
                    probation_flag = 'N'
                else:
                    probation_flag = 'Y'
            v_dict['probation_flag'] = probation_flag

            job_seg = JobSeg(**v_dict)
            self.job_seg_dic[segment_num] = job_seg

    def build_period_seg(self):
        """
        Desc: 根据人员编码、任职记录号，取生效日期>=历经期开始日期,且<=的历经期结束日期所有记录，构造期间分段数据
        Author: David
        Date: 2018/08/29
        :return:None
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num
        country = catalog.f_country
        f_prd_bgn_dt = catalog.f_prd_bgn_dt
        f_prd_end_dt = catalog.f_prd_end_dt
        seg_periods = []

        min_efft_dt = None
        min_efft_dt_sql = text(
            "select min(a.hhr_efft_date) from boogoo_corehr.hhr_org_per_jobdata a where a.tenant_id = :b1 "
            "and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3")
        row = db.conn.execute(min_efft_dt_sql, b1=tenant_id, b2=emp_id, b3=emp_rcd).fetchone()
        if row[0] is not None:
            min_efft_dt = row[0]
        else:
            raise Exception("无法获取最小生效日期")

        # 忽略小于任职最小生效日期的记录
        tri_sql = text(
            "select distinct hhr_trgr_effdt from boogoo_payroll.hhr_py_seg_trgr where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
            "and hhr_trgr_effdt > :b4 and hhr_trgr_effdt <= :b5 and hhr_country = :b6 and hhr_seg_trgr_status = 'A' and hhr_trgr_effdt >=:b7 "
            "order by hhr_trgr_effdt ")
        rs = db.conn.execute(tri_sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=f_prd_bgn_dt,
                             b5=f_prd_end_dt, b6=country, b7=min_efft_dt).fetchall()
        if f_prd_bgn_dt < min_efft_dt:
            bgn_dt = min_efft_dt
        else:
            bgn_dt = f_prd_bgn_dt
        for row in rs:
            if row is not None:
                eff_dt = row['hhr_trgr_effdt']
                if bgn_dt < eff_dt:
                    seg_periods.append((bgn_dt, eff_dt - one_day))
                    bgn_dt = eff_dt
        seg_periods.append((bgn_dt, f_prd_end_dt))

        job_sql = text(
            "select a.hhr_efft_seq,a.hhr_action_type_code,a.hhr_action_reason_code,a.hhr_relation,a.hhr_status,a.hhr_emp_class,a.hhr_sub_emp_class,a.hhr_company_code, "
            "a.hhr_posn_code,a.hhr_dept_code,a.hhr_bu_code,a.hhr_location,a.hhr_direct_rpt_posn,a.hhr_dotted_rpt_posn,ps.hhr_job_code,p.hhr_group_code,p.hhr_sequence_code, "
            "a.hhr_rank_code, "
            "a.hhr_org_per_jobdata_attr01, a.hhr_org_per_jobdata_attr03, a.hhr_org_per_jobdata_attr09, a.hhr_org_per_jobdata_attr10, p.hhr_org_job_attr01, "
            "pb.hhr_begin_date prob_bgn_date "
            "from boogoo_corehr.hhr_org_per_jobdata a "
            
            "LEFT JOIN boogoo_corehr.hhr_org_posn ps ON ps.tenant_id = a.tenant_id and ps.hhr_posn_code = a.hhr_posn_code "
            "AND a.hhr_efft_date BETWEEN ps.hhr_efft_date AND ps.hhr_efft_end_date "

            "left join boogoo_corehr.hhr_org_job p on p.tenant_id = ps.tenant_id and p.hhr_job_code = ps.hhr_job_code "
            "and ps.hhr_efft_date between p.hhr_efft_date and p.hhr_efft_end_date "

            "left join boogoo_corehr.hhr_org_per_prob pb on pb.tenant_id = a.tenant_id and pb.hhr_empid = a.hhr_empid "
            "and :b4 between pb.hhr_begin_date and pb.hhr_end_date "

            "where a.tenant_id = :b1 and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 and a.hhr_efft_date <= :b4 "
            "order by a.hhr_efft_date desc, a.hhr_efft_seq desc")

        seg_num = 0
        period_len = len(seg_periods)
        for date_tuple in seg_periods:
            seg_num += 1
            v_dict = {'tenant_id': tenant_id,
                      'emp_id': emp_id,
                      'emp_rcd': emp_rcd,
                      'seq_num': seq_num,
                      'segment_num': seg_num,
                      'seg_bgn_dt': date_tuple[0],
                      'seg_end_dt': date_tuple[1],
                      'cal_days': 0,
                      'work_days': 0,
                      'legal_days': 0,
                      'cal_sum_days': 0,
                      'work_sum_days': 0,
                      'legal_sum_days': 0}
            # 构造期间分段表
            p_seg = PeriodSeg(**v_dict)
            self.period_seg_lst.append(p_seg)
            self.period_seg_dic[seg_num] = p_seg

            # 针对每个期间分段，构造任职分段表
            seg_end_dt = date_tuple[1]
            self.build_job_seg(seg_num, seg_end_dt, job_sql, period_len)
            # print('----build_period_seg-----' + '--' + catalog.f_cal_id + '--' + str(emp_id) + '_' + str(emp_rcd) + '_' + str(date_tuple))

    @staticmethod
    def set_range_stat(d, start_dt, end_dt):
        """
        Desc: 将start_dt和end_dt之间的每一天设置为有效Y或无效N
        Author: David
        Date: 2018/08/28
        """

        status = d[start_dt]
        each_dt = start_dt + one_day
        while each_dt < end_dt:
            if status == 'Y':
                d[each_dt] = 'Y'
            elif status == 'N':
                d[each_dt] = 'N'
            each_dt = each_dt + one_day

    def get_days_hr_status(self):
        """
        Desc: 获取历经期结束日期前员工每一天的在职状态
        Author: David
        Date: 2018/08/29
        :return:每一天的在职状态字典
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        f_prd_end_dt = catalog.f_prd_end_dt

        eff_stat_dict = {}
        min_eff_dt = None
        eff_dt = None

        s3 = text("select a.hhr_efft_date, a.hhr_status from boogoo_corehr.hhr_org_per_jobdata a where a.tenant_id = :b1 "
                  "and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 "
                  "and hhr_efft_date <=:b4 order by hhr_efft_date, hhr_efft_seq desc ")
        rs = db.conn.execute(s3, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=f_prd_end_dt).fetchall()
        i = 0
        for r in rs:
            i += 1
            if i == 1:
                min_eff_dt = r['hhr_efft_date']
            eff_dt = r['hhr_efft_date']
            status = r['hhr_status']
            if eff_dt in eff_stat_dict:
                continue
            eff_stat_dict[eff_dt] = status
        max_eff_dt = eff_dt
        days_stat_dic = eff_stat_dict.copy()
        if max_eff_dt is not None:
            for job_dt in eff_stat_dict.keys():
                if min_eff_dt == job_dt:
                    continue
                else:
                    self.set_range_stat(days_stat_dic, min_eff_dt, job_dt)
                    min_eff_dt = job_dt
            self.set_range_stat(days_stat_dic, max_eff_dt, (f_prd_end_dt + one_day))
        return days_stat_dic

    def get_wrk_plns_dic(self, wk_pln_list):
        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        f_prd_bgn_dt = catalog.f_prd_bgn_dt
        f_prd_end_dt = catalog.f_prd_end_dt

        wrk_plns_dic = {}
        shift_sql = text(
            'select hhr_date, hhr_shift_code, hhr_holiday_type from boogoo_attendance.vw_hhr_work_schedule_detail '
            'where tenant_id = :b1 and hhr_wrk_pln_id = :b2 and hhr_date >= :b3 and hhr_date <= :b4 order by hhr_date ')
        for wk_pln_id in wk_pln_list:
            rs_shift = db.conn.execute(shift_sql, b1=tenant_id, b2=wk_pln_id, b3=f_prd_bgn_dt,
                                       b4=f_prd_end_dt).fetchall()
            if rs_shift is None or len(rs_shift) == 0:
                # 若未取到值，薪资计算报错
                raise Exception('未获取到工作计划' + wk_pln_id + '的数据')
            else:
                for row_shift in rs_shift:
                    shift_dt = row_shift['hhr_date']
                    shift_id = row_shift['hhr_shift_code']
                    holiday_type = row_shift['hhr_holiday_type']
                    if (wk_pln_id, shift_dt) not in wrk_plns_dic:
                        wrk_plns_dic[(wk_pln_id, shift_dt)] = [(shift_id, holiday_type)]
                    else:
                        wrk_plns_dic[(wk_pln_id, shift_dt)].append((shift_id, holiday_type))
        return wrk_plns_dic

    def build_work_cal(self, cal_dic, work_dic, legal_dic, s_cal_dic, s_work_dic, s_legal_dic):
        """
        Desc: 获取工作日历, 从考勤管理数据表获取数据，构造工作日历分段表
        Author: David
        Date: 2018/08/29
        :return:None
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num
        f_prd_bgn_dt = catalog.f_prd_bgn_dt
        f_prd_end_dt = catalog.f_prd_end_dt

        # 获取历经期结束日期前员工每一天的在职状态
        eff_stat_dict = self.get_days_hr_status()

        # 判断变量VR_PSP的值，值为'1'取个人工作日历（排班后），
        # 值为'2'取个人工作计划对应的工作日历，
        # 值为'3'取薪资组工作计划对应的工作日历

        vr_psp = gv.get_var_value('VR_PSP')
        result_lst = []
        # 直接使用薪资组工作计划（整个历经期均为此一个工作计划）
        if vr_psp == '3':
            wrk_pln_id = gv.get_var_value('VR_PG_SCHEDULE')
            shift_sql = text(
                'select hhr_date, hhr_shift_code, hhr_holiday_type from boogoo_attendance.vw_hhr_work_schedule_detail '
                'where tenant_id = :b1 and hhr_wrk_pln_id = :b2 and hhr_date >= :b3 and hhr_date <= :b4 order by hhr_date ')
            rs_shift = db.conn.execute(shift_sql, b1=tenant_id, b2=wrk_pln_id, b3=f_prd_bgn_dt,
                                       b4=f_prd_end_dt).fetchall()
            if rs_shift is None or len(rs_shift) == 0:
                # 若未取到值，薪资计算报错”
                raise Exception('未获取到工作计划' + wrk_pln_id + '的数据')
            else:
                for row_shift in rs_shift:
                    shift_dt = row_shift['hhr_date']
                    shift_id = row_shift['hhr_shift_code']
                    holiday_type = row_shift['hhr_holiday_type']
                    result_lst.append({'shift_dt': shift_dt, 'shift_id': shift_id, 'holiday_type': holiday_type,
                                       'wrk_pln_id': wrk_pln_id})
        # 取人员信息上的工作计划
        elif vr_psp == '2':
            emp_wk_pln_sql = text(
                "select hhr_work_plan_code, hhr_begin_date as hhr_effe_date, hhr_expr_date as hhr_invalid_date from boogoo_attendance.hhr_pt_emp_work_plan where tenant_id = :b1 and hhr_empid = :b2 "
                "and hhr_begin_date <= :b3 and (hhr_expr_date >= :b4 or hhr_expr_date is null ) order by hhr_begin_date asc ")
            emp_wk_pln_rs = db.conn.execute(emp_wk_pln_sql, b1=tenant_id, b2=emp_id, b3=f_prd_end_dt,
                                            b4=f_prd_bgn_dt).fetchall()
            if emp_wk_pln_rs is None or len(emp_wk_pln_rs) == 0:
                raise Exception('未获取到员工' + emp_id + '的工作计划')
            else:

                """ 获取人员信息上的工作计划 """
                i = 0
                wrk_pln_id = ''
                eff_date = None
                invalid_date = None
                emp_wk_pln_dic = {}
                wk_pln_list = []
                for emp_wk_pln_row in emp_wk_pln_rs:
                    wrk_pln_id = emp_wk_pln_row['hhr_work_plan_code']

                    if wrk_pln_id not in wk_pln_list:
                        wk_pln_list.append(wrk_pln_id)

                    eff_date = emp_wk_pln_row['hhr_effe_date']
                    invalid_date = emp_wk_pln_row['hhr_invalid_date']
                    if invalid_date is None:
                        invalid_date = datetime(9999, 12, 31).date()
                    i += 1
                    if i == 1:
                        # 若取到的记录最小生效日期>历经期开始日期，调整为历经期开始日期
                        if eff_date > f_prd_bgn_dt:
                            eff_date = f_prd_bgn_dt
                    emp_wk_pln_dic[eff_date] = (invalid_date, wrk_pln_id)

                # 若取到的记录最大失效日期<历经期结束日期，调整为历经期结束日期
                if invalid_date < f_prd_end_dt:
                    invalid_date = f_prd_end_dt
                    emp_wk_pln_dic[eff_date] = (invalid_date, wrk_pln_id)

                """ 获取该员工所有工作计划在历经期内对应的班次数据 """
                wrk_plns_dic = self.get_wrk_plns_dic(wk_pln_list)

                """ 循环历经期内每一天，获取其工作计划对应的班次数据 """
                each_day = f_prd_bgn_dt
                while each_day <= f_prd_end_dt:

                    # 获取这一天对应的工作计划
                    work_plan_id = ''
                    for k, v in emp_wk_pln_dic.items():
                        from_date = k
                        to_date = v[0]
                        if from_date <= each_day <= to_date:
                            work_plan_id = v[1]
                            break

                    if (work_plan_id, each_day) in wrk_plns_dic:
                        wrk_plns_lst = wrk_plns_dic[(work_plan_id, each_day)]
                        for wrk_plns_tup in wrk_plns_lst:
                            shift_id = wrk_plns_tup[0]
                            holiday_type = wrk_plns_tup[1]
                            result_lst.append({'shift_dt': each_day, 'shift_id': shift_id, 'holiday_type': holiday_type,
                                               'wrk_pln_id': work_plan_id})

                    each_day = each_day + one_day

        # 取人员信息上的排班
        elif vr_psp == '1':
            """ 从员工日程表中获取数据 """
            temp_result_dic = {}
            cal_shift_sql = text(
                "select hhr_date, hhr_shift_id as hhr_shift_code, hhr_holiday_type, hhr_wrk_pln_id from boogoo_attendance.vw_hhr_emp_calendar where tenant_id = :b1 "
                "and hhr_empid = :b2 and hhr_date >= :b3 and hhr_date <= :b4 order by hhr_date ")
            rs_cal_shift = db.conn.execute(cal_shift_sql, b1=tenant_id, b2=emp_id, b3=f_prd_bgn_dt,
                                           b4=f_prd_end_dt).fetchall()
            if rs_cal_shift is None or len(rs_cal_shift) == 0:
                raise Exception('未获取到员工' + emp_id + '的工作计划数据')
            i = 0
            min_date = None
            max_date = None
            for row_cal_shift in rs_cal_shift:
                shift_dt = row_cal_shift['hhr_date']
                i += 1
                if i == 1:
                    min_date = shift_dt
                max_date = shift_dt
                shift_id = row_cal_shift['hhr_shift_code']
                holiday_type = row_cal_shift['hhr_holiday_type']
                wrk_pln_id = row_cal_shift['hhr_wrk_pln_id']
                if shift_dt not in temp_result_dic:
                    temp_result_dic[shift_dt] = [
                        {'shift_dt': shift_dt, 'shift_id': shift_id, 'holiday_type': holiday_type,
                         'wrk_pln_id': wrk_pln_id}]
                else:
                    temp_result_dic[shift_dt].append(
                        {'shift_dt': shift_dt, 'shift_id': shift_id, 'holiday_type': holiday_type,
                         'wrk_pln_id': wrk_pln_id})

            min_wk_plan_id = max_wk_plan_id = None
            wrk_plns_dic = {}
            if min_date != f_prd_bgn_dt or max_date != f_prd_end_dt:
                min_wk_plan_id = max_wk_plan_id = None
                # 获取已取到的记录中日期最小日期的记录对应的工作计划
                temp_result_lst = temp_result_dic[min_date]
                # 同一日期的工作计划是相同的
                for temp_dic in temp_result_lst:
                    min_wk_plan_id = temp_dic['wrk_pln_id']

                # 获取已取到的记录中日期最大日期的记录对应的工作计划
                if min_date != max_date:
                    temp_result_lst = temp_result_dic[max_date]
                    for temp_dic in temp_result_lst:
                        max_wk_plan_id = temp_dic['wrk_pln_id']

                """ 获取工作计划在历经期内对应的班次数据 """
                temp_pln_lst = []
                if min_wk_plan_id and min_wk_plan_id not in temp_pln_lst:
                    temp_pln_lst.append(min_wk_plan_id)
                if max_wk_plan_id and max_wk_plan_id not in temp_pln_lst:
                    temp_pln_lst.append(max_wk_plan_id)
                wrk_plns_dic = self.get_wrk_plns_dic(temp_pln_lst)

            each_day = f_prd_bgn_dt
            while each_day <= f_prd_end_dt:
                # 构造遗漏日期的班次数据（如果存在的话）
                if each_day not in temp_result_dic:
                    # 获取这一天对应的工作计划
                    work_plan_id = None
                    if each_day < min_date:
                        work_plan_id = min_wk_plan_id
                    elif each_day > max_date:
                        work_plan_id = max_wk_plan_id
                    if (work_plan_id, each_day) in wrk_plns_dic:
                        wrk_plns_lst = wrk_plns_dic[(work_plan_id, each_day)]
                        shift_dt = each_day
                        for wrk_plns_tup in wrk_plns_lst:
                            shift_id = wrk_plns_tup[0]
                            holiday_type = wrk_plns_tup[1]
                            if each_day not in temp_result_dic:
                                temp_result_dic[each_day] = [
                                    {'shift_dt': shift_dt, 'shift_id': shift_id, 'holiday_type': holiday_type,
                                     'wrk_pln_id': work_plan_id}]
                            else:
                                temp_result_dic[each_day].append(
                                    {'shift_dt': shift_dt, 'shift_id': shift_id, 'holiday_type': holiday_type,
                                     'wrk_pln_id': work_plan_id})
                for result_dic in temp_result_dic[each_day]:
                    result_lst.append(result_dic)

                each_day = each_day + one_day
        else:
            raise Exception('变量VR_PSP的值为空')

        for row_dic in result_lst:
            shift_dt = row_dic['shift_dt']
            shift_id = row_dic['shift_id']
            if shift_id is None:
                shift_id = ''
            holiday_type = row_dic['holiday_type']
            wrk_pln_id = row_dic['wrk_pln_id']

            # 综合任职记录，入职前、离职后的日期为无效（N）。即人员状态为无效的日期为无效（N）
            if shift_dt not in eff_stat_dict:
                active = 'N'
            else:
                active = eff_stat_dict[shift_dt]
            v_dict = {'tenant_id': tenant_id,
                      'emp_id': emp_id,
                      'emp_rcd': emp_rcd,
                      'seq_num': seq_num,
                      'shift_dt': shift_dt,
                      'shift_id': shift_id,
                      'active': active,
                      'is_legal': '',
                      'work_hours': 0,
                      'days_convert': 0,
                      'shift_type': '',
                      'holiday_type': holiday_type,
                      'wrk_pln_id': wrk_pln_id}
            wk_cal = WorkCal(**v_dict)
            self.wk_cal_list.append(wk_cal)

            # 天数折算默认0
            days_convert = 0
            # 日历日、工作日、法定日：仅考虑各自分段内的“有效（Y）”记录
            if active == 'Y':
                cal_dic[shift_dt] = 'Y'  # 同一日期日历日仅算1天
                # 法定假期
                if holiday_type == 'HD':
                    legal_dic[shift_dt] = 'Y'  # 同一日期若为法定日，法定日仅算1天
                elif holiday_type == 'WD':  # 工作日，不包含法定
                    if shift_dt not in work_dic:
                        work_dic[shift_dt] = []
                    work_dic[shift_dt].append(days_convert)

            if f_prd_bgn_dt <= shift_dt <= f_prd_end_dt:
                s_cal_dic[shift_dt] = 'Y'
                if holiday_type == 'HD':
                    s_legal_dic[shift_dt] = 'Y'
                elif holiday_type == 'WD':
                    if shift_dt not in s_work_dic:
                        s_work_dic[shift_dt] = []
                    s_work_dic[shift_dt].append(days_convert)

    @staticmethod
    def calc_days(bgn_dt, end_dt, cal_dic, legal_dic, work_dic):
        """
        Desc: 计算日历日（或总日历日）、法定日（或总法定日）、工作日（或总工作日）
        Author: David
        Date: 2018/08/28
        """

        cal_cnt = 0
        legal_cnt = 0
        work_cnt = 0
        while bgn_dt <= end_dt:
            # 统计日历日
            if bgn_dt in cal_dic:
                cal_cnt += 1

            # 统计法定日
            if bgn_dt in legal_dic:
                legal_cnt += 1

            # 统计工作日
            if bgn_dt in work_dic:
                # 同一日期仅一个班次时，天数折算没有值（默认0），算1天；有值，按维护的值计算天数
                temp_lst = work_dic[bgn_dt]
                if len(temp_lst) == 1:
                    if temp_lst[0] == 0:
                        work_cnt += 1
                    else:
                        work_cnt += temp_lst[0]
                elif len(temp_lst) > 1:
                    # 同一日期多个班次，天数折算全部没有值（默认0），算1天；
                    # 部分有值，有值部分按维护的值计算天数，无值的部分算1天。全部有值，按维护的值计算天数。
                    all_zero = True
                    temp_work_cnt = 0
                    for m in temp_lst:
                        if m != 0:
                            temp_work_cnt += m
                            all_zero = False
                        else:
                            temp_work_cnt += 1
                    # 全部没有值，算1天
                    if all_zero:
                        work_cnt += 1
                    else:
                        work_cnt = work_cnt + temp_work_cnt
            bgn_dt = bgn_dt + one_day
        return cal_cnt, legal_cnt, work_cnt

    def update_period_seg(self, cal_dic, work_dic, legal_dic, s_cal_dic, s_work_dic, s_legal_dic):
        """
        Desc: 根据工作日历更新分段表
        Author: David
        Date: 2018/08/29
        :return:None
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        f_prd_bgn_dt = catalog.f_prd_bgn_dt
        f_prd_end_dt = catalog.f_prd_end_dt
        s_cal_cnt, s_legal_cnt, s_work_cnt = self.calc_days(f_prd_bgn_dt, f_prd_end_dt, s_cal_dic, s_legal_dic,
                                                            s_work_dic)
        for ps in self.period_seg_lst:
            seg_bgn_dt = ps.seg_bgn_dt
            seg_end_dt = ps.seg_end_dt
            cal_cnt, legal_cnt, work_cnt = self.calc_days(seg_bgn_dt, seg_end_dt, cal_dic, legal_dic, work_dic)
            ps.cal_days = cal_cnt
            ps.legal_days = legal_cnt
            ps.work_days = work_cnt
            ps.cal_sum_days = s_cal_cnt
            ps.legal_sum_days = s_legal_cnt
            ps.work_sum_days = s_work_cnt

    def get_seg_rule_list(self):
        """
        Desc: 获取历经期内所有有效的分段规则
        Author: David
        Date: 2018/08/29
        :return:分段规则列表
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        country = catalog.f_country
        f_prd_bgn_dt = catalog.f_prd_bgn_dt

        dic_key = country + str(f_prd_bgn_dt)
        seg_rules_dic = gv.get_run_var_value('SEG_RULES_DIC')
        if dic_key in seg_rules_dic:
            self.seg_rule_list = seg_rules_dic[dic_key]
            return self.seg_rule_list
        else:
            seg_rules_dic[dic_key] = []
            s = text("select a.hhr_seg_rule_cd, a.hhr_numerator, a.hhr_denominator, a.hhr_fixed_days, a.hhr_priority "
                     "from boogoo_payroll.hhr_py_seg_rule a where a.tenant_id = :b1 and a.hhr_country = :b2 "
                     "and :b3 between a.hhr_efft_date and a.hhr_efft_end_date "
                     "and a.hhr_status = 'Y' ")
            rs = db.conn.execute(s, b1=tenant_id, b2=country, b3=f_prd_bgn_dt).fetchall()
            for r in rs:
                rule_cd = r['hhr_seg_rule_cd']
                numerator = r['hhr_numerator']
                denominator = r['hhr_denominator']
                fixed_days = r['hhr_fixed_days']
                priority = r['hhr_priority']
                seg_rules_dic[dic_key].append({'rule_cd': rule_cd, 'numerator': numerator, 'denominator': denominator,
                                               'fixed_days': fixed_days, 'priority': priority})
            gv.set_run_var_value('SEG_RULES_DIC', seg_rules_dic)
            self.seg_rule_list = seg_rules_dic[dic_key]
        return self.seg_rule_list

    def cal_seg_factors(self):
        """
        Desc: 计算分段因子
        Author: David
        Date: 2018/08/29
        :return:None
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        cur_cal_obj = gv.get_run_var_value('CUR_CAL_OBJ')
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num

        v_dict = {'tenant_id': tenant_id,
                  'emp_id': emp_id,
                  'emp_rcd': emp_rcd,
                  'seq_num': seq_num,
                  'seg_rule_cd': '',
                  'segment_num': 0,
                  'numerator_val': 0,
                  'denominator_val': 0,
                  'factor_val': 0}

        # 处理之前先获取历经期内所有有效的分段规则
        self.get_seg_rule_list()

        for rule in self.seg_rule_list:
            v_dict['seg_rule_cd'] = rule['rule_cd']
            numerator = rule['numerator']
            denominator = rule['denominator']
            fixed_days = rule['fixed_days']
            priority = rule['priority']

            sigma_num_days = 0
            s_days = 0
            denominator_val = 0
            temp_seg_list = []
            for ps in self.period_seg_lst:
                numerator_val = denominator_val = factor_val = 0

                # 计算分子值
                if numerator == 'A':  # A-工作日(含法定)
                    numerator_val = ps.work_days + ps.legal_days
                elif numerator == 'B':  # B-工作日(不含法定)
                    numerator_val = ps.work_days
                elif numerator == 'C':  # C-日历日
                    numerator_val = ps.cal_days

                # 计算分母值
                if denominator == 'A':
                    denominator_val = ps.work_sum_days + ps.legal_sum_days
                elif denominator == 'B':
                    denominator_val = ps.work_sum_days
                elif denominator == 'C':
                    denominator_val = ps.cal_sum_days
                elif denominator == 'D':  # D-固定天数
                    denominator_val = fixed_days

                if denominator_val != 0:
                    factor_val = Decimal(str(numerator_val / denominator_val)).quantize(Decimal('0.000000000'))
                    factor_val = float(factor_val)

                v_dict['segment_num'] = ps.segment_num
                v_dict['numerator_val'] = numerator_val
                v_dict['denominator_val'] = denominator_val
                v_dict['factor_val'] = factor_val
                seg_fact = SegFactor(**v_dict)
                temp_seg_list.append(seg_fact)

                # 当分段规则的分母为固定天数时, 统计各分段分子总和
                if denominator == 'D':
                    sigma_num_days += numerator_val
                    if numerator == 'A':  # A-工作日(含法定)
                        # 统计总工作日+总法定日(每一段都一样，所以只需计算一次)
                        if s_days == 0:
                            s_days = ps.work_sum_days + ps.legal_sum_days
                    elif numerator == 'B':  # B-工作日(不含法定)
                        if s_days == 0:
                            s_days += ps.work_sum_days

            seg_fact_rv = temp_seg_list

            """1.当分段规则的分母为固定天数时，若分子为工作日(含法定)，且(各分段的∑(工作日+法定日)=总工作日+总法定日 OR ∑(分子)>分母)：则计算分段因子时，
               需调整分子，使∑(分子)=分母，即分子之和=固定天数。
               当优先级为期初优先，则差异从最后一个分段向前消除；当优先级为期末优先，则差异从第一个分段向后消除。（分子不能出现负数，最小为0）
               
               2.当分段规则的分母为固定天数时，若分子为工作日(不含法定)，且(各分段的∑(工作日)=总工作日 OR ∑(分子)>分母)：则计算分段因子时，
               需调整分子，使∑(分子)=分母，即分子之和=固定天数。 
               当优先级为期初优先，则差异从最后一个分段向前消除；当优先级为期末优先，则差异从第一个分段向后消除。（分子不能出现负数，最小为0）"""
            # if denominator == 'D' and sigma_num_days == s_days and sigma_num_days != fixed_days:
            if denominator == 'D' and (
                    sigma_num_days == s_days or sigma_num_days > fixed_days) and sigma_num_days != fixed_days:
                # 调整分子，使分子之和=固定天数
                days_gap = fixed_days - sigma_num_days
                if priority == 'F':  # F-期初优先
                    seg_fact_rv = list(reversed(temp_seg_list))
                elif priority == 'L':  # L-期末优先
                    seg_fact_rv = temp_seg_list
                if days_gap > 0:
                    seg_fact_rv[0].numerator_val = seg_fact_rv[0].numerator_val + days_gap
                    f_val = Decimal(str(seg_fact_rv[0].numerator_val / denominator_val)).quantize(
                        Decimal('0.000000000'))
                    seg_fact_rv[0].factor_val = float(f_val)
                else:
                    if abs(days_gap) <= seg_fact_rv[0].numerator_val:
                        seg_fact_rv[0].numerator_val = seg_fact_rv[0].numerator_val - abs(days_gap)
                        f_val = Decimal(str(seg_fact_rv[0].numerator_val / denominator_val)).quantize(
                            Decimal('0.000000000'))
                        seg_fact_rv[0].factor_val = float(f_val)
                    else:
                        for seg in seg_fact_rv:
                            if abs(days_gap) > seg.numerator_val:
                                seg.numerator_val = 0
                                seg.factor_val = 0.000000000
                                days_gap = 0 - (abs(days_gap) - seg.numerator_val)
                            else:
                                seg.numerator_val = seg.numerator_val - abs(days_gap)
                                f_val = Decimal(str(seg.numerator_val / denominator_val)).quantize(
                                    Decimal('0.000000000'))
                                seg.factor_val = float(f_val)
                                break

            # 当分段规则各分段的∑(分子)=分母时，需调整因子值，各分段的∑(因子值)=1，差异在分子最大的分段消除，最大分子超过1个，在前一个分段消除
            sigma_num_days = 0
            sigma_factor = 0
            if priority == 'F':
                temp_seg_list = list(reversed(seg_fact_rv))
            else:
                temp_seg_list = seg_fact_rv
            if len(temp_seg_list) > 0:
                max_numerator = temp_seg_list[0].numerator_val
                max_seg = temp_seg_list[0]
                for seg in temp_seg_list:
                    sigma_num_days += seg.numerator_val
                    sigma_factor += seg.factor_val
                    if seg.numerator_val > max_numerator:
                        max_numerator = seg.numerator_val
                        max_seg = seg

                if sigma_num_days == denominator_val and sigma_num_days != 0:
                    if sigma_factor < 1:
                        factor_gap = 1 - sigma_factor
                        max_seg.factor_val = max_seg.factor_val + factor_gap
            self.seg_fact_dic[rule['rule_cd']] = temp_seg_list

        # 判断薪资组对应的分段规则，若∑(分子)<分母，即∑(因子值)<1，则此人员为月中入离职，将变量SYS_ENTRY_LEAVE赋值1。非月中入离职赋值0
        pg_seg_rule_cd = cur_cal_obj.py_group_entity.seg_rule_cd
        if pg_seg_rule_cd in self.seg_fact_dic:
            pg_seg_fact_list = self.seg_fact_dic[pg_seg_rule_cd]
            sigma_factor = 0
            for ps in pg_seg_fact_list:
                sigma_factor += ps.factor_val
            if sigma_factor < 1:
                gv.set_var_value('VR_ENTRY_LEAVE', 'Y')
            elif sigma_factor == 1:
                gv.set_var_value('VR_ENTRY_LEAVE', 'N')

    def persist_data(self):
        """
        Desc: 将期间分段、任职信息分段、工作日历、分段因子数据写入数据库
        Author: David
        Date: 2018/08/29
        :return:None
        """
        for ps in self.period_seg_lst:
            ps.insert()

        for js in self.job_seg_dic.values():
            js.insert()

        for wc in self.wk_cal_list:
            wc.insert()

        for val_list in self.seg_fact_dic.values():
            for v in val_list:
                v.insert()

    def process_seg(self):
        """
        Desc: 期间分段处理
        Author: David
        Date: 2018/08/23
        """
        # 1.构造期间分段数据
        self.build_period_seg()

        # 2.构造工作日历分段表
        cal_dic = {}  # 日历日
        work_dic = {}  # 工作日
        legal_dic = {}  # 法定日
        s_cal_dic = {}  # 总日历日
        s_work_dic = {}  # 总工作日
        s_legal_dic = {}  # 总法定日

        self.build_work_cal(cal_dic, work_dic, legal_dic, s_cal_dic, s_work_dic, s_legal_dic)

        # 3.根据工作日历更新分段表
        self.update_period_seg(cal_dic, work_dic, legal_dic, s_cal_dic, s_work_dic, s_legal_dic)

        # 4.计算分段因子
        self.cal_seg_factors()

        # 5.数据落地
        self.persist_data()
