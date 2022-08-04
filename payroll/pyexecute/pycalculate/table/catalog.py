# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm
from sqlalchemy import text


class Catalog:
    """
    Desc: 薪资计算目录表 HHR_PY_CAL_CATALOG
    Author: David
    Date: 2018/08/23
    """

    __slots__ = ['db',
                 'run_parm',
                 'tenant_id',
                 'emp_id',
                 'emp_rcd',
                 'seq_num',
                 'f_calgrp_id',
                 'f_cal_id',
                 'f_country',
                 'f_pygrp_id',
                 'f_runtype_id',
                 'f_period_cd',
                 'f_period_year',
                 'f_prd_num',
                 'f_pay_date',
                 'f_cal_type',
                 'f_prd_bgn_dt',
                 'f_prd_end_dt',
                 'f_rt_cycle',
                 'cal_grp_id',
                 'cal_id',
                 'country',
                 'pygrp_id',
                 'run_type_id',
                 'period_cd',
                 'period_year',
                 'period_num',
                 'pay_date',
                 'cal_type',
                 'prd_bgn_dt',
                 'prd_end_dt',
                 'cycle',
                 'rec_stat',
                 'prev_seq',
                 'hist_seq',
                 'upd_seq',
                 'new_entry_flg',
                 ]

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.run_parm = gv.get_run_var_value('RUN_PARM')
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.new_entry_flg = 'N'

        action = kwargs.get('action', None)
        if action != 'U':
            # 历经期
            self.f_calgrp_id = kwargs.get('f_calgrp_id', '')
            self.f_cal_id = kwargs.get('f_cal_id', '')
            self.f_country = kwargs.get('f_country', '')
            self.f_pygrp_id = kwargs.get('f_pygrp_id', '')
            self.f_runtype_id = kwargs.get('f_runtype_id', '')
            self.f_period_cd = kwargs.get('f_period_cd', '')
            self.f_period_year = kwargs.get('f_period_year', 1900)
            self.f_prd_num = kwargs.get('f_prd_num', 0)
            self.f_pay_date = kwargs.get('f_pay_date', None)
            self.f_cal_type = kwargs.get('f_cal_type', '')
            self.f_prd_bgn_dt = kwargs.get('f_prd_bgn_dt', None)
            self.f_prd_end_dt = kwargs.get('f_prd_end_dt', None)
            self.f_rt_cycle = kwargs.get('f_rt_cycle', '')

            # 所在期
            self.cal_grp_id = kwargs.get('cal_grp_id', '')
            self.cal_id = kwargs.get('cal_id', '')
            self.country = kwargs.get('country', '')
            self.pygrp_id = kwargs.get('pygrp_id', '')
            self.run_type_id = kwargs.get('run_type_id', '')
            self.period_cd = kwargs.get('period_cd', '')
            self.period_year = kwargs.get('period_year', 1900)
            self.period_num = kwargs.get('period_num', 0)
            self.pay_date = kwargs.get('pay_date', None)
            self.cal_type = kwargs.get('cal_type', '')
            self.prd_bgn_dt = kwargs.get('prd_bgn_dt', None)
            self.prd_end_dt = kwargs.get('prd_end_dt', None)
            self.cycle = kwargs.get('cycle', '')

            self.rec_stat = kwargs.get('rec_stat', '')
            self.prev_seq = kwargs.get('prev_seq', 0)
            self.hist_seq = kwargs.get('hist_seq', 0)
            self.upd_seq = kwargs.get('upd_seq', None)

    def insert(self):
        cata = self.db.get_table('hhr_py_cal_catalog', schema_name='boogoo_payroll')
        ins = cata.insert()
        val = [
            {'tenant_id': self.tenant_id,
             'hhr_empid': self.emp_id,
             'hhr_emp_rcd': self.emp_rcd,
             'hhr_seq_num': self.seq_num,
             'hhr_f_calgrp_id': self.f_calgrp_id,
             'hhr_f_cal_id': self.f_cal_id,
             'hhr_f_country': self.f_country,
             'hhr_f_pygroup_id': self.f_pygrp_id,
             'hhr_f_runtype_id': self.f_runtype_id,
             'hhr_f_period_code': self.f_period_cd,
             'hhr_f_period_year': self.f_period_year,
             'hhr_f_prd_num': self.f_prd_num,
             'hhr_f_paydate': self.f_pay_date,
             'hhr_f_caltype': self.f_cal_type,
             'hhr_f_prd_bgn_dt': self.f_prd_bgn_dt,
             'hhr_f_prd_end_dt': self.f_prd_end_dt,
             'hhr_f_rt_cycle': self.f_rt_cycle,
             'hhr_pycalgrp_id': self.cal_grp_id,
             'hhr_py_cal_id': self.cal_id,
             'hhr_country': self.country,
             'hhr_pygroup_id': self.pygrp_id,
             'hhr_runtype_id': self.run_type_id,
             'hhr_period_code': self.period_cd,
             'hhr_period_year': self.period_year,
             'hhr_prd_num': self.period_num,
             'hhr_pay_date': self.pay_date,
             'hhr_pycalc_type': self.cal_type,
             'hhr_prd_bgn_dt': self.prd_bgn_dt,
             'hhr_prd_end_dt': self.prd_end_dt,
             'hhr_cycle': self.cycle,
             'hhr_py_rec_stat': self.rec_stat,
             'hhr_prev_seq': self.prev_seq,
             'hhr_hist_seq': self.hist_seq,
             'hhr_upd_seq': self.upd_seq,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': self.run_parm['operator_user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': self.run_parm['operator_user_id'],
             }]
        self.db.conn.execute(ins, val)

    def update(self, **values):
        cata = self.db.get_table('hhr_py_cal_catalog', schema_name='boogoo_payroll')
        values['hhr_modify_dttm'] = get_current_dttm()
        values['hhr_modify_user'] = self.run_parm['operator_user_id']
        u = cata.update(). \
            where(cata.c.tenant_id == self.tenant_id).\
            where(cata.c.hhr_empid == self.emp_id).\
            where(cata.c.hhr_emp_rcd == self.emp_rcd).\
            where(cata.c.hhr_seq_num == self.seq_num).\
            values(values)
        self.db.conn.execute(u)


def init_catalog(emp, cur_cal, cal_obj, do_retro_flag):
    """
    根据历经期和所在期日历对象生成catalog对象
    :param emp:员工参数对象hhr.payroll.pyexecute.parameterentity.EmpParameter
    :param cur_cal:历经期日历对象
    :param cal_obj:所在期日历对象
    :param do_retro_flag:追溯标志，Y：做过追溯, N:没处理过追溯
    :return:Catalog
    """
    db = gv.get_db()
    cal_grp_obj = gv.get_run_var_value('CAL_GRP_OBJ')
    calgrp_id = cal_grp_obj.cal_group_id
    tenant_id = emp.tenant_id
    emp_id = emp.emp_id
    emp_rcd = emp.emp_rcd

    sql = text("select a.hhr_seq_num,a.hhr_prev_seq,a.hhr_hist_seq,a.hhr_f_calgrp_id,a.hhr_f_cal_id from boogoo_payroll.hhr_py_cal_catalog a "
               "where a.tenant_id = :b1 and a.hhr_empid=:b2 and a.hhr_emp_rcd=:b3 order by hhr_seq_num desc")
    result = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd).fetchone()

    if result is None:
        seq_num = 0
        prev_seq = None
        his_seq = None
        f_calgrp_id = calgrp_id
    else:
        seq_num = result['hhr_seq_num']
        prev_seq = result['hhr_prev_seq']
        his_seq = result['hhr_hist_seq']
        # f_calgrp_id = result['HHR_F_CALGRP_ID']
        # if not f_calgrp_id:
        #     f_calgrp_id = calgrp_id
        f_calgrp_id = calgrp_id

    new_seq_num = seq_num + 1
    new_prev_seq = 0
    new_his_seq = None
    upd_seq = None
    if cal_obj.pay_cal_type == 'C':
        # 获取被更正日历当前期间的序号
        upd_seq_num = 0
        cata_sql = text("select a.hhr_seq_num from boogoo_payroll.hhr_py_cal_catalog a where a.tenant_id = :b1 and a.hhr_empid=:b2 "
                        "and a.hhr_emp_rcd=:b3 and a.hhr_f_cal_id = :b4 and a.hhr_py_cal_id = :b4 ")
        row = db.conn.execute(cata_sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=cur_cal.cal_id).fetchone()
        if row is not None:
            upd_seq_num = row['hhr_seq_num']

        # 生成新期间之前，需要将被更正日历当前期间的状态改为O（区别于P，都表示历史期间）
        upd_sql = text("update boogoo_payroll.hhr_py_cal_catalog set hhr_py_rec_stat = 'O' where tenant_id = :b1 and hhr_empid = :b2 "
                       "and hhr_emp_rcd = :b3 and hhr_f_cal_id = :b4 and hhr_py_cal_id = :b4 and hhr_py_rec_stat = 'A' ")
        db.conn.execute(upd_sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=cur_cal.cal_id)

        if do_retro_flag == 'Y':
            new_prev_seq = seq_num
            new_his_seq = upd_seq_num
        else:
            new_prev_seq = prev_seq
            new_his_seq = seq_num
        upd_seq = new_his_seq
        f_calgrp_id = cal_obj.cal_id
    elif cal_obj.pay_cal_type in ['A', 'B']:
        new_prev_seq = seq_num
        # new_his_seq = 0
        new_his_seq = None

    val_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': new_seq_num,
               'f_calgrp_id': f_calgrp_id,
               'f_cal_id': cal_obj.cal_id, 'f_country': cal_grp_obj.country, 'f_pygrp_id': cal_obj.py_group_id,
               'f_runtype_id': cal_obj.run_type_id,
               'f_period_cd': cal_obj.period_id, 'f_period_year': cal_obj.period_year,
               'f_prd_num': cal_obj.period_num, 'f_pay_date': cal_obj.pay_date,
               'f_cal_type': cal_obj.pay_cal_type, 'f_prd_bgn_dt': cal_obj.period_cal_entity.start_date,
               'f_prd_end_dt': cal_obj.period_cal_entity.end_date, 'f_rt_cycle': cal_obj.run_type_entity.cycle,
               'cal_grp_id': calgrp_id, 'cal_id': cal_obj.cal_id, 'country': cal_grp_obj.country,
               'pygrp_id': cal_obj.py_group_id, 'run_type_id': cal_obj.run_type_id,
               'period_cd': cal_obj.period_id, 'period_year': cal_obj.period_year, 'period_num': cal_obj.period_num,
               'pay_date': cal_obj.pay_date,
               'cal_type': cal_obj.pay_cal_type, 'prd_bgn_dt': cal_obj.period_cal_entity.start_date,
               'prd_end_dt': cal_obj.period_cal_entity.end_date,
               'cycle': cal_obj.run_type_entity.cycle, 'rec_stat': 'A',
               'prev_seq': new_prev_seq, 'hist_seq': new_his_seq, 'upd_seq': upd_seq}
    catalog = Catalog(**val_dic)
    catalog.insert()
    return catalog
