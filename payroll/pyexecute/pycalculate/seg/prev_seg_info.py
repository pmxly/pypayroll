# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from sqlalchemy.sql import text
from ..table.period_seg import PeriodSeg
from datetime import timedelta, datetime

one_day = timedelta(days=1)


class PrevSegInfo(object):
    """
    Desc: 上一期间分段信息
    Author: David
    Date: 2018/08/23
    """

    __slots__ = ['f_prd_bgn_dt', 'f_prd_end_dt', 'period_seg_lst', 'period_seg_dic']

    def __init__(self, f_prd_bgn_dt, f_prd_end_dt):
        self.f_prd_bgn_dt = f_prd_bgn_dt
        self.f_prd_end_dt = f_prd_end_dt
        self.period_seg_lst = []  # 期间分段列表
        self.period_seg_dic = {}  # 期间分段字典
        self.process_seg()

    def build_period_seg(self):
        """
        Desc: 根据人员编码、任职记录号，取生效日期>=历经期开始日期,且<=的历经期结束日期所有记录，构造期间分段数据
        Author: David
        Date: 2021/01/28
        :return:None
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num
        country = catalog.f_country
        f_prd_bgn_dt = self.f_prd_bgn_dt
        f_prd_end_dt = self.f_prd_end_dt
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

        seg_num = 0
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
        Date: 2021/01/28
        :return:每一天的在职状态字典
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        f_prd_end_dt = self.f_prd_end_dt

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
        f_prd_bgn_dt = self.f_prd_bgn_dt
        f_prd_end_dt = self.f_prd_end_dt

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
        Date: 2021/01/28
        :return:None
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        db = gv.get_db()
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        f_prd_bgn_dt = self.f_prd_bgn_dt
        f_prd_end_dt = self.f_prd_end_dt

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
            # v_dict = {'tenant_id': tenant_id,
            #           'emp_id': emp_id,
            #           'emp_rcd': emp_rcd,
            #           'seq_num': seq_num,
            #           'shift_dt': shift_dt,
            #           'shift_id': shift_id,
            #           'active': active,
            #           'is_legal': '',
            #           'work_hours': 0,
            #           'days_convert': 0,
            #           'shift_type': '',
            #           'holiday_type': holiday_type,
            #           'wrk_pln_id': wrk_pln_id}
            # wk_cal = WorkCal(**v_dict)
            # self.wk_cal_list.append(wk_cal)

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
        Date: 2021/01/28
        :return:None
        """
        f_prd_bgn_dt = self.f_prd_bgn_dt
        f_prd_end_dt = self.f_prd_end_dt
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

    def process_seg(self):
        """
        Desc: 期间分段处理
        Author: David
        Date: 2021/01/28
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
