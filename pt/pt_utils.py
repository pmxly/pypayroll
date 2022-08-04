from ..dbengine import DataBaseAlchemy
from sqlalchemy.sql import text
from ..utils import get_current_date
from dateutil.relativedelta import relativedelta


max_seq_stmt = text("select max(HHR_SEQNUM) from hhr_pt_emp_leave_quota_t_b where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 ")

job_stmt = text("select a.hhr_empid from boogoo_corehr.hhr_org_per_jobdata a where a.tenant_id = :b1 and a.hhr_status = 'Y' and a.hhr_job_indicator = 'P' "
                "and a.hhr_efft_date = (select max(a1.hhr_efft_date) from boogoo_corehr.hhr_org_per_jobdata a1 "
                "where a1.tenant_id = a.tenant_id and a1.hhr_empid = a.hhr_empid and a1.hhr_emp_rcd = a.hhr_emp_rcd "
                "and a1.hhr_efft_date <= :b2) "
                "and a.hhr_efft_seq = (select max(a1.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata a1 where a1.tenant_id = a.tenant_id "
                "and a1.hhr_empid = a.hhr_empid and a1.hhr_emp_rcd = a.hhr_emp_rcd and a1.hhr_efft_date = a.hhr_efft_date) ")

ins_quota = text("insert into hhr_pt_emp_leave_quota_t_b(HHR_SYS_CODE, HHR_EMPID, HHR_PT_CODE, HHR_SEQNUM, HHR_PT_EMP_AMOUNT, "
                 "HHR_PT_EMP_UNIT, HHR_PT_EMP_CREATE_DATE, HHR_PT_EMP_INVALID_DATE, HHR_PT_EMP_RESORCE) "
                 "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9) ")


def get_invalid_date(benchmark_date, overdue_len, overdue_unit):
    invalid_date = None
    if overdue_unit == 'D':
        days_add = relativedelta(days=overdue_len)
        invalid_date = benchmark_date + days_add
    elif overdue_unit == 'W':
        weeks_add = relativedelta(weeks=overdue_len)
        invalid_date = benchmark_date + weeks_add
    elif overdue_unit == 'M':
        months_add = relativedelta(months=overdue_len)
        invalid_date = benchmark_date + months_add
    elif overdue_unit == 'Y':
        years_add = relativedelta(years=overdue_len)
        invalid_date = benchmark_date + years_add
    return invalid_date


def gen_leave_quota(conn, tenant_id, pt_code, pt_unit_type, quantity, is_overdue, overdue_len, overdue_unit, benchmark_date, emp_list):
    if len(emp_list) == 0:
        job_rs = conn.execute(job_stmt, b1=tenant_id, b2=get_current_date()).fetchall()
        for emp_row in job_rs:
            employee_id = emp_row['HHR_EMPID']
            emp_list.append(employee_id)
    for emp_id in emp_list:
        row = conn.execute(max_seq_stmt, b1=tenant_id, b2=emp_id).fetchone()
        if row[0] is None:
            new_seq_num = 1
        else:
            new_seq_num = row[0] + 1

        if is_overdue == 'N':
            invalid_date = None
        else:
            invalid_date = get_invalid_date(benchmark_date, overdue_len, overdue_unit)
        conn.execute(ins_quota, b1=tenant_id, b2=emp_id, b3=pt_code, b4=new_seq_num, b5=quantity,
                     b6=pt_unit_type, b7=get_current_date(), b8=invalid_date, b9='1')


def generate_emp_leave_quota(conn, tenant_id):
    """自动生成假期额度"""

    emp_list = []
    current_date = get_current_date()

    sel_stmt = text("select x.HHR_SYS_CODE, x.HHR_PT_CODE, x.HHR_PT_UNIT_TYPE, y.HHR_AMOUNT, z.HHR_BENCHMARK_DATE, "
                    "z.HHR_GENERATE_PERIOD, z.HHR_GENERATE_PERIOD_UNIT, z.HHR_IS_OVERDUE, z.HHR_OVERDUE_LENGTH, z.HHR_OVERDUE_UNIT "
                    "from HHR_PT_QUOTA_B x, HHR_PT_QUOTA_DTL_GEN_B y, HHR_PT_QUOTA_GENERATE_B z "
                    "where x.HHR_SYS_CODE = y.HHR_SYS_CODE and x.HHR_QUOTA_RULE_CODE = y.HHR_QUOTA_RULE_CODE "
                    "and x.HHR_STATUS = 'Y' and z.HHR_SYS_CODE = y.HHR_SYS_CODE and z.HHR_QUOTA_GENERATE_CODE = y.HHR_QUOTA_GENERATE_CODE "
                    "and z.HHR_STATUS = 'Y' and z.HHR_GENERATE_TYPE = 'FIX' and x.HHR_SYS_CODE = :b1 ")
    rs = conn.execute(sel_stmt, b1=tenant_id).fetchall()

    trans = conn.begin()

    for row in rs:
        pt_code = row['HHR_PT_CODE']
        pt_unit_type = row['HHR_PT_UNIT_TYPE']
        quantity = row['HHR_AMOUNT']
        benchmark_date = row['HHR_BENCHMARK_DATE']
        gen_period = row['HHR_GENERATE_PERIOD']
        gen_period_unit = row['HHR_GENERATE_PERIOD_UNIT']
        is_overdue = row['HHR_IS_OVERDUE']
        overdue_len = row['HHR_OVERDUE_LENGTH']
        if not overdue_len:
            overdue_len = 0
        else:
            overdue_len = int(overdue_len)
        overdue_unit = row['HHR_OVERDUE_UNIT']

        if not benchmark_date or not gen_period:
            continue
        if current_date < benchmark_date:
            continue

        gen_period = int(gen_period)
        generate_flag = False

        # 如果当前日期刚好为基准日期，则执行生成
        if benchmark_date == current_date:
            generate_flag = True

        # 按天
        if gen_period_unit == 'D':
            days_gap = current_date - benchmark_date
            mul = days_gap / gen_period
            if mul.seconds == 0:
                generate_flag = True
        # 按周
        elif gen_period_unit == 'W':
            days_gap = current_date - benchmark_date
            mul = days_gap / (gen_period * 7)
            if mul.seconds == 0:
                generate_flag = True
        # 按月
        elif gen_period_unit == 'M':
            current_month = current_date.month
            current_day = current_date.day
            bench_month = benchmark_date.month
            bench_day = benchmark_date.day

            if current_day == bench_day:
                month_gap = current_month - bench_month
                mod = month_gap % gen_period
                if mod == 0:
                    generate_flag = True
        # 按季度
        elif gen_period_unit == 'Q':
            current_month = current_date.month
            current_day = current_date.day
            bench_month = benchmark_date.month
            bench_day = benchmark_date.day

            if current_day == bench_day:
                month_gap = current_month - bench_month
                mod = month_gap % (gen_period * 3)
                if mod == 0:
                    generate_flag = True
        # 按年
        elif gen_period_unit == 'Y':
            current_year = current_date.year
            current_month = current_date.month
            current_day = current_date.day
            bench_year = benchmark_date.year
            bench_month = benchmark_date.month
            bench_day = benchmark_date.day
            if current_day == bench_day and current_month == bench_month:
                year_gap = current_year - bench_year
                mod = year_gap % gen_period
                if mod == 0:
                    generate_flag = True
        if generate_flag:
            gen_leave_quota(conn, tenant_id, pt_code, pt_unit_type, quantity, is_overdue, overdue_len, overdue_unit, benchmark_date, emp_list)

    trans.commit()
