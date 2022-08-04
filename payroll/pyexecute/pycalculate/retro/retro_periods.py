# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from sqlalchemy.sql import text
from ..table.catalog import Catalog
from ....pysysutils.func_lib_01 import payee_pay_group_check
from ..calculate_by_emp import calc_without_catalog


class RetroPeriods:
    """
    Desc: 确定追溯期间
    Author: David
    Date: 2018/08/10
    """

    __slots__ = [
        'db',
        'cal_grp_obj',
        'retro_flag',
        'emp',
        'cal_obj',
        'retro_periods_',
        'max_seq'
    ]

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.cal_grp_obj = gv.get_run_var_value('CAL_GRP_OBJ')
        self.retro_flag = gv.get_run_var_value('RETRO_FLAG')
        self.emp = kwargs.get('emp', None)
        self.cal_obj = kwargs.get('cal_obj', None)
        self.retro_periods_ = []
        self.max_seq = kwargs.get('max_seq', None)

    @property
    def retro_periods(self):
        if len(self.retro_periods_) != 0:
            return self.retro_periods_
        if self.retro_flag != 'Y':
            return self.retro_periods_

        db = self.db
        tenant_id = self.emp.tenant_id
        emp_id = self.emp.emp_id
        emp_rcd = self.emp.emp_rcd
        cal_grp_id = self.cal_grp_obj.cal_group_id
        cal_id = self.cal_obj.cal_id
        country = self.cal_grp_obj.country

        min_rto_date = self.cal_obj.min_rto_prd_cal.start_date
        gv.set_run_var_value('RETRO_MIN', min_rto_date)
        if self.cal_obj.run_type_entity.cycle == 'C':  # 非周期不追溯

            # 根据人员编号、任职记录号获取“触发器状态”=U（未处理）或者“处理日历组”=当前日历组的记录
            # 将“人员追溯触发器”表中的记录标记为“P-已处理”或“C-取消”，“处理日历组”统一更新为当前日历组

            # “生效日期”小于最小追溯日期的记录“触发器状态”更新为C（取消）
            u = text("update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'C', hhr_pycalgrp_id = :b5 where (hhr_trgr_status = 'U' or hhr_pycalgrp_id = :b5) "
                     "and tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_trgr_effdt < :b4 and hhr_country = :b6 ")
            db.conn.execute(u, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=min_rto_date, b5=cal_grp_id, b6=country)

            # “生效日期”大于等于最小追溯日期的记录“触发器状态”更新为P（已处理）
            u = text("update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'P', hhr_pycalgrp_id = :b5 where (hhr_trgr_status = 'U' or hhr_pycalgrp_id = :b5) "
                     "and tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_trgr_effdt >= :b4 and hhr_country = :b6 ")
            db.conn.execute(u, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=min_rto_date, b5=cal_grp_id, b6=country)

            # 取“生效日期”大于等于最小追溯日期的记录中的最小生效日期，与最小追溯日期比较，取较大值作为实际追溯日期
            s = text("select min(hhr_trgr_effdt) from boogoo_payroll.hhr_py_rto_trgr where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
                     "and hhr_pycalgrp_id = :b4 and hhr_trgr_status = 'P' and hhr_trgr_effdt >= :b5 and hhr_country = :b6 ")
            row = db.conn.execute(s, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=cal_grp_id, b5=min_rto_date, b6=country).fetchone()

            actual_rto_date = None
            if row[0] is not None:
                temp_rto_date = row[0]
                actual_rto_date = temp_rto_date if temp_rto_date >= min_rto_date else min_rto_date
                gv.set_run_var_value('RETRO_ACT', actual_rto_date)

            # 追溯&常规/单独计算：根据实际追溯日期确定追溯的期间:

            # 获取所在期日历相关信息
            pygrp_id = self.cal_obj.py_group_id
            run_type_id = self.cal_obj.run_type_id
            period_cd = self.cal_obj.period_id
            period_year = self.cal_obj.period_year
            period_num = self.cal_obj.period_num
            pay_date = self.cal_obj.pay_date
            cal_type = self.cal_obj.pay_cal_type
            prd_bgn_dt = self.cal_obj.period_cal_entity.start_date
            prd_end_dt = self.cal_obj.period_cal_entity.end_date
            cycle = self.cal_obj.run_type_entity.cycle

            # # 确定已有薪资计算记录的最大序号，后续记录在此基础上依次+1
            # s = text("select max(HHR_SEQ_NUM) from HHR_PY_CAL_CATALOG where tenant_id = :b1 and HHR_EMPID = :b2 and HHR_EMP_RCD = :b3 ")
            # row = db.conn.execute(s, b1=tenant_id, b2=emp_id, b3=emp_rcd).fetchone()
            # if row[0] is not None:
            #     max_seq = row[0]

            max_seq = self.max_seq
            if max_seq is not None:

                # 需要追溯的期间是：记录状态=A（活动），且(历经期)结束日期>=实际追溯日期
                sql_catalog = "select hhr_py_cal_id, hhr_seq_num, hhr_f_calgrp_id, hhr_f_cal_id, hhr_f_country, hhr_f_pygroup_id, hhr_f_runtype_id, " \
                              "hhr_f_period_code, hhr_f_period_year, hhr_f_prd_num, hhr_f_paydate, hhr_f_caltype, hhr_f_prd_bgn_dt, " \
                              "hhr_f_prd_end_dt, hhr_f_rt_cycle, hhr_prev_seq, hhr_hist_seq from boogoo_payroll.hhr_py_cal_catalog where tenant_id = :b1 " \
                              "and hhr_empid = :b2 and hhr_emp_rcd = :b3 and ( (hhr_py_rec_stat = 'A' and hhr_f_prd_end_dt >= :b4) "

                # 追溯&更正计算：如果计算类型为更正C
                upd_cal_id = None
                if self.cal_obj.pay_cal_type == 'C':
                    # 更正&追溯时，如果薪资计算目录的历经期日历和更正日历相同（说明该期间为被更正日历当前的期间，非追溯），
                    sql_catalog = sql_catalog + ' or hhr_py_cal_id = :b5 ) and hhr_f_cal_id <> :b5 '
                    upd_cal_id = self.emp.upd_cal_id
                else:
                    sql_catalog = sql_catalog + ' ) '

                order_by = " order by hhr_seq_num "
                sql_catalog = sql_catalog + order_by

                rs = db.conn.execute(text(sql_catalog), b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=actual_rto_date, b5=upd_cal_id).fetchall()

                n = 0
                new_seq_num = 0
                for row in rs:
                    # 记录上一序号
                    temp_pre_new_seq = new_seq_num
                    n += 1
                    cur_cal_id = row['hhr_py_cal_id']
                    seq_num = row['hhr_seq_num']
                    new_seq_num = max_seq + n
                    f_calgrp_id = row['hhr_f_calgrp_id']
                    f_cal_id = row['hhr_f_cal_id']
                    f_country = row['hhr_f_country']
                    f_pygrp_id = row['hhr_f_pygroup_id']
                    f_runtype_id = row['hhr_f_runtype_id']
                    f_period_cd = row['hhr_f_period_code']
                    f_period_year = row['hhr_f_period_year']
                    f_prd_num = row['hhr_f_prd_num']
                    f_pay_date = row['hhr_f_paydate']
                    f_cal_type = row['hhr_f_caltype']
                    f_prd_bgn_dt = row['hhr_f_prd_bgn_dt']
                    f_prd_end_dt = row['hhr_f_prd_end_dt']
                    f_rt_cycle = row['hhr_f_rt_cycle']
                    prev_seq = row['hhr_prev_seq']
                    hist_seq = row['hhr_hist_seq']

                    # 被追溯期间的原记录，对应记录状态更新为P（历史）
                    # 如果为更正&追溯，被更正日历当前的期间也需要被更新为P
                    keys = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num, 'action': 'U'}
                    Catalog(**keys).update(hhr_py_rec_stat='P')

                    # 被追溯的第1个期间上一序号保持不变
                    if n == 1:
                        new_prev_seq = prev_seq
                    else:
                        new_prev_seq = temp_pre_new_seq
                    new_hist_seq = seq_num

                    # 更正&追溯时，如果历史序号有值&所在期日历为被更正的日历，需要将被追溯期间的最初的历史序号赋给新生成的期间
                    upd_seq = None
                    if self.cal_obj.pay_cal_type == 'C':
                        upd_seq = seq_num
                        if hist_seq and cur_cal_id == upd_cal_id:
                            new_hist_seq = hist_seq
                        else:
                            new_hist_seq = seq_num

                    row_dict = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': new_seq_num,
                                'f_calgrp_id': f_calgrp_id,
                                'f_cal_id': f_cal_id, 'f_country': f_country, 'f_pygrp_id': f_pygrp_id,
                                'f_runtype_id': f_runtype_id,
                                'f_period_cd': f_period_cd, 'f_period_year': f_period_year,
                                'f_prd_num': f_prd_num, 'f_pay_date': f_pay_date,
                                'f_cal_type': f_cal_type, 'f_prd_bgn_dt': f_prd_bgn_dt,
                                'f_prd_end_dt': f_prd_end_dt, 'f_rt_cycle': f_rt_cycle,
                                'cal_grp_id': cal_grp_id, 'cal_id': cal_id, 'country': country,
                                'pygrp_id': pygrp_id, 'run_type_id': run_type_id,
                                'period_cd': period_cd, 'period_year': period_year, 'period_num': period_num,
                                'pay_date': pay_date,
                                'cal_type': cal_type, 'prd_bgn_dt': prd_bgn_dt, 'prd_end_dt': prd_end_dt,
                                'cycle': cycle, 'rec_stat': 'A',
                                'prev_seq': new_prev_seq, 'hist_seq': new_hist_seq, 'upd_seq': upd_seq}
                    catalog = Catalog(**row_dict)
                    catalog.insert()
                    self.retro_periods_.append(catalog)
                return self.retro_periods_

            # 如果没有薪资计算记录
            else:
                # 针对宇通：如果新入职员工是是非正式工，不需要追溯
                from ....pysysutils.func_lib_01 import get_emp_type
                emp_type = get_emp_type(self.emp)
                if emp_type not in ['1', '2']:
                    return self.retro_periods_

                # 实际追溯日期 < 本次薪资计算的开始日期(所在期开始日期)
                if actual_rto_date is None:
                    return self.retro_periods_
                retro_periods = calc_without_catalog(db, actual_rto_date, self.emp, self.cal_grp_obj, self.cal_obj)
                if len(retro_periods):
                    self.retro_periods_ = retro_periods
                    return self.retro_periods_
                # if actual_rto_date < self.cal_obj.period_cal_entity.start_date:
                #     s = text("select HHR_PERIOD_YEAR, HHR_PRD_NUM, HHR_PERIOD_START_DATE, HHR_PERIOD_END_DATE "
                #              "from HHR_PERIOD_CALENDAR_LINE where tenant_id = :b1 "
                #              "and HHR_PERIOD_CODE = :b2 and HHR_PERIOD_END_DATE >= :b3 and HHR_PERIOD_END_DATE < :b4 order by HHR_PERIOD_END_DATE ")
                #     result = db.conn.execute(s, b1=tenant_id, b2=self.cal_obj.period_cal_entity.period_id,
                #                              b3=actual_rto_date, b4=self.cal_obj.period_cal_entity.end_date).fetchall()
                #     seq_num = 0
                #     if result is None:
                #         return self.retro_periods_
                #     for row in result:
                #         temp_pre_seq = seq_num
                #         seq_num += 1
                #         f_period_year = row['HHR_PERIOD_YEAR']
                #         f_prd_num = row['HHR_PRD_NUM']
                #         f_prd_bgn_dt = row['HHR_PERIOD_START_DATE']
                #         f_prd_end_dt = row['HHR_PERIOD_END_DATE']
                #
                #         # 若在本次薪资计算的开始日期前，薪资组不同，则不需要追溯
                #         if payee_pay_group_check(self.emp, self.cal_obj, bgn_dt=f_prd_bgn_dt, end_dt=f_prd_end_dt):
                #             if seq_num == 1:
                #                 prev_seq = None
                #             else:
                #                 prev_seq = temp_pre_seq
                #             row_dict = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                #                         'f_calgrp_id': '', 'f_cal_id': '', 'f_country': country, 'f_pygrp_id': pygrp_id,
                #                         'f_runtype_id': run_type_id,
                #                         'f_period_cd': period_cd, 'f_period_year': f_period_year,
                #                         'f_prd_num': f_prd_num, 'f_pay_date': pay_date,
                #                         'f_cal_type': cal_type, 'f_prd_bgn_dt': f_prd_bgn_dt,
                #                         'f_prd_end_dt': f_prd_end_dt, 'f_rt_cycle': cycle,
                #                         'cal_grp_id': cal_grp_id, 'cal_id': cal_id, 'country': country,
                #                         'pygrp_id': pygrp_id, 'run_type_id': run_type_id,
                #                         'period_cd': period_cd, 'period_year': period_year, 'period_num': period_num,
                #                         'pay_date': pay_date,
                #                         'cal_type': cal_type, 'prd_bgn_dt': prd_bgn_dt, 'prd_end_dt': prd_end_dt,
                #                         'cycle': cycle, 'rec_stat': 'A',
                #                         'prev_seq': prev_seq, 'hist_seq': None}
                #             catalog = Catalog(**row_dict)
                #             catalog.insert()
                #             self.retro_periods_.append(catalog)
                #     return self.retro_periods_
        return self.retro_periods_
