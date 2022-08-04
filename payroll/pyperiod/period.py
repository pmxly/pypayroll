# coding:utf-8

from ..pysysutils import global_variables as gv
from sqlalchemy import select


class PeriodCalender:
    """
    期间日历
    """

    def __init__(self, tenant_id, period_id, period_year, period_num):
        # 租户ID
        self.tenant_id = ''
        # 期间ID
        self.period_id = ''
        # 年度
        self.year = 1900
        # 期间序号
        self.period_num = 0
        # 开始日期
        self.start_date = None
        # 结束日期
        self.end_date = None
        # 上次期间年度
        self.last_period_year = 1900
        # 上次期间序号
        self.last_period_num = 0

        self.tenant_id = tenant_id
        self.period_id = period_id
        self.year = period_year
        self.period_num = period_num
        db = gv.get_db()
        t = db.get_table('hhr_py_period_calendar_line', schema_name='boogoo_payroll')
        result = db.conn.execute(select(
            [t.c.hhr_period_year, t.c.hhr_prd_num, t.c.hhr_period_start_date, t.c.hhr_period_end_date,
             t.c.hhr_period_last_year, t.c.hhr_last_prd_num],
            (t.c.tenant_id == tenant_id) & (t.c.hhr_period_code == period_id) & (t.c.hhr_period_year == period_year) &
            (t.c.hhr_prd_num == period_num))).fetchone()
        if result is not None:
            self.year = result['hhr_period_year']
            self.period_num = result['hhr_prd_num']
            self.start_date = result['hhr_period_start_date']
            self.end_date = result['hhr_period_end_date']
            self.last_period_year = result['hhr_period_last_year']
            self.last_period_num = result['hhr_last_prd_num']
        else:
            pass


class AccumulateCalender:
    """
    期间累计日历
    """

    def __init__(self, tenant_id, period_id, period_year, period_num, acc_cal_type):
        # 租户ID
        self.tenant_id = ''
        # 期间ID
        self.period_id = ''
        # 年度
        self.year = 1900
        # 期间序号
        self.period_num = 0
        # 累计类型,M 月度,Q 季度,U 非限制,X 半年度,Y 年度
        self.acc_cal_type = ''
        # 累计年度
        self.ac_year = 1900
        # 累计编号
        self.ac_num = 0

        self.tenant_id = tenant_id
        self.period_id = period_id
        self.year = period_year
        self.period_num = period_num
        self.acc_cal_type = acc_cal_type
        db = gv.get_db()
        t = db.get_table('hhr_py_period_add_calendar_line', schema_name='boogoo_payroll')
        result = db.conn.execute(select(
            [t.c.hhr_period_add_year, t.c.hhr_period_add_number],
            (t.c.tenant_id == tenant_id) & (t.c.hhr_period_code == period_id) & (t.c.hhr_period_year == period_year) &
            (t.c.hhr_prd_num == period_num) & (t.c.hhr_period_add_type == acc_cal_type))).fetchone()
        if result is not None:
            self.ac_year = result['hhr_period_add_year']
            self.ac_num = result['hhr_period_add_number']
        else:
            pass


class Period:
    """
   期间头表
    """

    def __init__(self, tenant_id, period_id):
        # 租户ID
        self.tenant_id = ''
        # 期间ID
        self.period_id = ''
        """
        频率
        10:每月
        20:每半月
        30:每周
        40:每两周
        50:每四周
        60:每年
        70:每季度
        80:每半年
        """
        self.frequency = ''
        # 期间名称
        self.description = ''

        db = gv.get_db()
        t = db.get_table('hhr_py_period', schema_name='boogoo_payroll')
        result = db.conn.execute(select([t.c.hhr_period_name, t.c.hhr_period_unit],
                                        (t.c.tenant_id == tenant_id) & (t.c.hhr_period_code == period_id))).fetchone()
        if result is not None:
            self.tenant_id = tenant_id
            self.period_id = period_id
            self.frequency = result['hhr_period_unit']
            self.description = result['hhr_period_name']
