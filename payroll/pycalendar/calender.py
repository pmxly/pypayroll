# coding:utf-8
"""
日历
"""

from sqlalchemy.sql import text
from ..pypaygroup.paygroup import PayGroup
from ..pyperiod.period import *
from sqlalchemy import select


class Payee:
    """
    日历中受款人实体
    """

    __slots__ = [
        # 租户ID
        'tenant_id',
        # 日历ID，关键字
        'cal_id',
        # 员工ID，关键字
        'emp_id',
        # 薪资组
        'py_group_id',
        # 日历ID，关键字
        'emp_rcd',
        # 更正日历ID
        'update_cal_id',
        # 更正日历ID
        'update_cal_entity'
    ]

    def __init__(self, **kwargs):
        self.tenant_id = kwargs['tenant_id']
        self.cal_id = kwargs['cal_id']
        # self.py_group_id = kwargs['py_group_id']
        self.emp_id = kwargs['emp_id']
        self.emp_rcd = kwargs['emp_rcd']
        if 'update_cal_id' in kwargs:
            self.update_cal_id = kwargs['update_cal_id']
            self.update_cal_entity = Calendar(kwargs['tenant_id'], self.update_cal_id)


class Calendar:
    """
       日历
       create by wangling 2018/7/29
    """

    def __init__(self, tenant_id, cal_id):
        from ..pyruntype.run_type import RunType

        # 租户ID
        self.tenant_id = tenant_id
        # 日历ID，关键字
        self.cal_id = cal_id
        # 日历描述/名称
        self.description = ''
        # 薪资组ID
        self.py_group_id = ''
        # 薪资组实体
        self.py_group_entity = None
        # 运行类型ID
        self.run_type_id = ''
        # 运行类型实体
        self.run_type_entity = None
        # 期间ID
        self.period_id = ''
        # 期间日历年份
        self.period_year = 1900
        # 期间日历序号
        self.period_num = 0
        # 期间实体
        self.period_entity = None
        # 期间日历实体
        self.period_cal_entity = None
        # 支付日期
        self.pay_date = None
        # 计算类型，A-常规，B-单独，C-更正
        self.pay_cal_type = ''
        # 排除人员list,包含Payee对象
        self.except_pers_list_ = []
        # 手动添加人员list,包含Payee对象
        self.add_pers_list_ = []
        # 更正人员list,包含Payee对象
        self.update_pers_list_ = []
        # 实际计算人员list,包含Payee对象
        self.pers_list_ = []
        # 当前日历的最小追溯期间日历
        self.min_rto_prd_cal_ = None

        # 当前日历的最小追溯期间日历(新入职员工)
        self.min_rto_prd_cal_for_new_entry_ = None

        db = gv.get_db()
        t = db.get_table('hhr_py_calendar', schema_name='boogoo_payroll')
        result = db.conn.execute(select(
            [t.c.hhr_description, t.c.hhr_pygroup_id, t.c.hhr_runtype_id, t.c.hhr_period_code, t.c.hhr_pay_date,
             t.c.hhr_pycalc_type, t.c.hhr_period_year, t.c.hhr_prd_num]).where(
            t.c.tenant_id == tenant_id).where(t.c.hhr_py_cal_id == cal_id)).fetchone()
        if result is not None:
            self.description = result['hhr_description']
            self.py_group_id = result['hhr_pygroup_id']
            self.run_type_id = result['hhr_runtype_id']
            self.period_id = result['hhr_period_code']
            self.pay_date = result['hhr_pay_date']
            self.pay_cal_type = result['hhr_pycalc_type']
            self.period_year = result['hhr_period_year']
            self.period_num = result['hhr_prd_num']

        else:
            pass

        self.period_entity = Period(tenant_id, self.period_id)
        self.period_cal_entity = PeriodCalender(tenant_id, self.period_id, self.period_year, self.period_num)
        self.py_group_entity = PayGroup(tenant_id, self.py_group_id, self.period_cal_entity.end_date)
        self.run_type_entity = RunType(self.tenant_id, self.run_type_id)

    @property
    def except_pers_list(self):
        if len(self.except_pers_list_) == 0:
            self.init_except_add_list('EXPT')
        return self.except_pers_list_

    @property
    def add_pers_list(self):
        if len(self.add_pers_list_) == 0:
            self.init_except_add_list('ADD')
        return self.add_pers_list_

    @property
    def update_pers_list(self):
        if len(self.update_pers_list_) == 0:
            self.init_except_add_list('UPT')
        return self.update_pers_list_

    @property
    def pers_list(self):
        """
        根据标识受款人结果表初始化实际算薪人员list
        :return:
        """
        if len(self.pers_list_) == 0:
            pass
        return self.pers_list_

    @property
    def min_rto_prd_cal(self):
        """
        Desc: 获取当前日历的最小追溯期间日历
        Author: David
        Date: 2018/08/22
        :return:
        """
        if self.min_rto_prd_cal_ is None:
            self.init_min_rto_prd_cal()
        return self.min_rto_prd_cal_

    @property
    def min_rto_prd_cal_for_new_entry(self):
        if self.min_rto_prd_cal_for_new_entry_ is None:
            self.init_min_rto_prd_cal_for_new_entry()
        return self.min_rto_prd_cal_for_new_entry_

    def init_except_add_list(self, except_add_flag):
        """
        初始化人员列表
        :param except_add_flag:
        EXPT:初始化self.add_pers_list
        ADD:初始化self.except_pers_lists
        UPT:初始化self.update_pers_list
        :return:
        """
        tmp_list = []
        table_name = ''

        db = gv.get_db()
        if except_add_flag == 'EXPT':
            table_name = 'hhr_py_cal_emp_expt'
            tmp_list = self.except_pers_list_
        elif except_add_flag == 'ADD':
            table_name = 'hhr_py_cal_emp_add'
            tmp_list = self.add_pers_list_
        elif except_add_flag == 'UPT':
            table_name = 'hhr_py_cal_emp_upt'
            tmp_list = self.update_pers_list_

        if table_name is not None:
            t = db.get_table(table_name, schema_name='boogoo_payroll')
            # hhr_py_cal_emp_upt 的表结构和其他两张表不一样，需要单独写查询语句
            if except_add_flag == 'UPT':
                result = db.conn.execute(select(
                    [t.c.hhr_empid, t.c.hhr_emp_rcd, t.c.hhr_upt_calid])
                                         .where(t.c.tenant_id == self.tenant_id)
                                         .where(t.c.hhr_py_cal_id == self.cal_id)).fetchall()
            else:
                result = db.conn.execute(select(
                    [t.c.hhr_empid, t.c.hhr_emp_rcd])
                                         .where(t.c.tenant_id == self.tenant_id)
                                         .where(t.c.hhr_py_cal_id == self.cal_id)).fetchall()

            if result is not None:
                for result_line in result:
                    if table_name == 'hhr_py_cal_emp_upt':
                        tmp_list.append(
                            Payee(tenant_id=self.tenant_id, cal_id=self.cal_id, emp_id=result_line['hhr_empid'],
                                  emp_rcd=result_line['hhr_emp_rcd'], update_cal_id=result_line['hhr_upt_calid']))
                    else:
                        tmp_list.append(
                            Payee(tenant_id=self.tenant_id, cal_id=self.cal_id, emp_id=result_line['hhr_empid'],
                                  emp_rcd=result_line['hhr_emp_rcd']))

    def init_min_rto_prd_cal(self):
        """
        Desc: 初始化当前日历的最小追溯期间日历
        Author: David
        Date: 2018/08/22
        :return:
        """
        max_rto_num = self.py_group_entity.max_retro_prd_num
        if max_rto_num <= 0:
            self.min_rto_prd_cal_ = self.period_cal_entity
            return
        self.min_rto_prd_cal_ = self.get_min_rto_prd_cal_obj(max_rto_num)

    def init_min_rto_prd_cal_for_new_entry(self):
        max_rto_num = gv.get_var_value('VR_AUTOCAL_PERIOD')
        if max_rto_num <= 0:
            self.min_rto_prd_cal_for_new_entry_ = self.period_cal_entity
            return
        self.min_rto_prd_cal_for_new_entry_ = self.get_min_rto_prd_cal_obj(max_rto_num)

    def get_min_rto_prd_cal_obj(self, max_rto_num):
        if max_rto_num <= 0:
            self.min_rto_prd_cal_ = self.period_cal_entity
            return
        cnt = 0
        reach_cur = False
        db = gv.get_db()
        s = text("select hhr_period_year, hhr_prd_num from boogoo_payroll.hhr_py_period_calendar_line where tenant_id = :b1 "
                 "and hhr_period_code = :b2 and (hhr_period_year = :b3 or hhr_period_year = :b4) "
                 "order by hhr_period_year desc, hhr_prd_num desc ")
        result = db.conn.execute(s, b1=self.tenant_id, b2=self.period_id, b3=self.period_year,
                                 b4=self.period_year - 1).fetchall()
        year = None
        prd_num = None
        if result is not None:
            for row in result:
                year = row['hhr_period_year']
                prd_num = row['hhr_prd_num']
                if (not reach_cur) and (prd_num == self.period_num):
                    reach_cur = True
                    continue
                if reach_cur:
                    cnt += 1
                if cnt == max_rto_num:
                    break
        return PeriodCalender(self.tenant_id, self.period_id, year, prd_num)


class CalendarGroup:
    """
       日历组
       create by wangling 2018/7/29
    """

    def __init__(self, tenant_id, cal_group_id):
        self.tenant_id = tenant_id
        self.cal_group_id = cal_group_id
        self.calendar_dic_ = {}
        db = gv.get_db()
        t = db.get_table('hhr_py_cal_grp', schema_name='boogoo_payroll')
        result = db.conn.execute(select(
            [t.c.hhr_description, t.c.hhr_country, t.c.hhr_retro_flag]).where(t.c.tenant_id == tenant_id).
                                 where(t.c.hhr_pycalgrp_id == cal_group_id)).fetchone()
        if result is not None:
            self.description = result['hhr_description']
            self.country = result['hhr_country']
            self.retro = result['hhr_retro_flag']
        else:
            pass

    @property
    def calendar_dic(self):
        if len(self.calendar_dic_) == 0:
            db = gv.get_db()
            t = db.get_table('hhr_py_calgrp_cal', schema_name='boogoo_payroll')
            result = db.conn.execute(select(
                [t.c.hhr_py_cal_id]).where(t.c.tenant_id == self.tenant_id).
                                     where(t.c.hhr_pycalgrp_id == self.cal_group_id)).fetchall()

            if result is not None:
                for result_line in result:
                    self.calendar_dic_[result_line['hhr_py_cal_id']] = Calendar(self.tenant_id,
                                                                                result_line['hhr_py_cal_id'])

            return self.calendar_dic_
        else:
            return self.calendar_dic_


def get_cal_obj_in_dev(tenant_id, cal_id):
    """
    在已经实例化好的日历字段中获取日历对象，如果没有，新添加一个
    :param tenant_id:租户ID
    :param cal_id:日历ID
    :return:
    """
    calendar_dic = gv.get_run_var_value('CAL_OBJ_DIC')
    if cal_id in calendar_dic:
        return calendar_dic[cal_id]
    else:
        calendar_dic[cal_id] = Calendar(tenant_id, cal_id)
        return calendar_dic[cal_id]
