# coding:utf-8


from ...pysysutils import global_variables as gv
from sqlalchemy import select
from sqlalchemy.sql import text
from ...pycalendar.calender import Payee
from ...pysysutils.func_lib_01 import ins_py_calc_stat, ins_py_calc_msg, payee_pay_group_check, del_py_calc_rslt, del_py_calc_log, update_cal_grp_run, del_payee_calc_stat
from ...pysysutils.func_lib_01 import get_main_cost_day


class IdentifyEmployees:
    """
    Desc: 标记受款人
    Author: David
    Date: 2018/08/10
    """

    def __init__(self, run_param):
        self.db = gv.get_db()
        self.logger = gv.get_run_var_value('LOGGER')
        self.cal_grp_obj = gv.get_run_var_value('CAL_GRP_OBJ')
        self.calendar_dic = self.cal_grp_obj.calendar_dic
        self.run_param = run_param
        self.locked_payees = []

    def identify_employees(self):

        # 1.数据一致性校验
        suc = self.consistency_check()

        # 2.更新/删除原操作记录和结果
        if suc:
            try:
                # t1 = self.db.conn.begin() 2021-03-11
                self.clr_payee_calc_stat()
                # t1.commit() 2021-03-11

                # t2 = self.db.conn.begin() 2021-03-11
                self.clr_pycalc_rslt()
                # t2.commit() 2021-03-11

                # t3 = self.db.conn.begin() 2021-03-11
                self.clr_pycalc_log()
                # t3.commit() 2021-03-11

                # t4 = self.db.conn.begin() 2021-03-11
                self.clr_identified_payees()
                # t4.commit() 2021-03-11

                # t5 = self.db.conn.begin() 2021-03-11
                # 3.开始标记人员
                self.begin_identify()
                self.upd_action_rec()
                # t5.commit() 2021-03-11
            except Exception:
                raise
        else:
            raise Exception

    def upd_action_rec(self):
        """
        Desc: 更新日历组中操作的时间记录
        Author: David
        Date: 2018/08/10
        """

        run_param = self.run_param
        db = self.db
        tenant_id = run_param['tenant_id']
        cal_grp_id = run_param['cal_grp_id']
        user_id = run_param['operator_user_id']
        update_cal_grp_run(db, tenant_id, cal_grp_id, user_id, 'PY_IDEN')

    def clr_payee_calc_stat(self):
        run_param = self.run_param
        db = self.db
        tenant_id = run_param['tenant_id']
        cal_grp_id = run_param['cal_grp_id']
        user_id = run_param['operator_user_id']
        del_payee_calc_stat(db, tenant_id, cal_grp_id, user_id, 'ALL')

    def clr_pycalc_rslt(self):
        """
        Desc: 清除已保存的薪资计算结果
        Author: David
        Date: 2018/08/10
        """

        run_param = self.run_param
        db = self.db
        tenant_id = run_param['tenant_id']
        cal_grp_id = run_param['cal_grp_id']
        user_id = run_param['operator_user_id']
        del_py_calc_rslt(db, tenant_id, cal_grp_id, user_id)

    def clr_pycalc_log(self):
        """
        Desc: 清除已保存的过程日志
        Author: David
        Date: 2018/08/10
        """

        run_param = self.run_param
        db = self.db
        tenant_id = run_param['tenant_id']
        cal_grp_id = run_param['cal_grp_id']
        del_py_calc_log(db, tenant_id, cal_grp_id)

    def clr_identified_payees(self):
        """
        Desc: 清除已标记的人员
        Author: David
        Date: 2018/08/10
        """

        run_param = self.run_param
        db = self.db

        tenant_id = run_param['tenant_id']
        cal_grp_id = run_param['cal_grp_id']

        stat = db.get_table('hhr_py_payee_calc_stat', schema_name='boogoo_payroll')  # 薪资标记&计算结果表
        msg = db.get_table('hhr_py_payee_calc_msg', schema_name='boogoo_payroll')    # 薪资标记&计算消息表

        d1 = stat.delete().where(stat.c.tenant_id == tenant_id).where(stat.c.hhr_pycalgrp_id == cal_grp_id)
        d2 = msg.delete().where(msg.c.tenant_id == tenant_id).where(msg.c.hhr_pycalgrp_id == cal_grp_id)
        db.conn.execute(d1)
        db.conn.execute(d2)

    def consistency_check(self):
        """
        Desc: 数据一致性校验
        Author: David
        Date: 2018/08/10
        """

        if self.cal_grp_obj is not None:
            error = False

            country = self.cal_grp_obj.country

            cal_lst = []
            pay_grp_lst = []
            run_type_lst = []
            for cal in self.calendar_dic.values():
                cal_lst.append(cal)
                pay_grp_lst.append(cal.py_group_entity)
                run_type_lst.append(cal.run_type_entity)

            # 1.日历组的国家/地区、日历的薪资组的国家/地区、日历的运行类型的国家/地区必须一致
            for pg in pay_grp_lst:
                if pg.country != country:
                    error = True
                    self.logger.message('日历组' + self.cal_grp_obj.cal_group_id + '的国家/地区与薪资组' + pg.pay_group_id + '的国家/地区不一致')
                    break
            if not error:
                for rt in run_type_lst:
                    if rt.country != country:
                        error = True
                        self.logger.message('日历组' + self.cal_grp_obj.cal_group_id + '的国家/地区与运行类型' + rt.process_type_id + '的国家/地区不一致')
                        break

            # 2.日历的期间编码、日历的薪资组的期间编码必须一致
            for cal in cal_lst:
                if cal.period_id != cal.py_group_entity.period_cd:
                    error = True
                    self.logger.message('日历' + cal.cal_id + '的期间编码与薪资组' + cal.py_group_entity.pay_group_id + '的期间编码不一致')
                    break

            # 3.日历组下所有日历的期间年度、期间序号必须一致
            # 4.日历组下所有日历的计算类型必须一致
            # 5.日历组下所有日历的运行类型的性质必须一致
            # 6.当日历组勾选了追溯时，校验日历的运行类型的性质，必须为“C-周期”。为“O-非周期”时，日历组不能勾选追溯
            if len(cal_lst) > 1:
                year = cal_lst[0].period_year
                seq = cal_lst[0].period_num
                cal_type = cal_lst[0].pay_cal_type
                cycle = cal_lst[0].run_type_entity.cycle
                for cal in cal_lst:
                    if year != cal.period_year:
                        error = True
                        self.logger.message('日历' + cal_lst[0].cal_id + '的期间年度' + str(year) + '与日历' + cal.cal_id + '的期间年度' + str(cal.period_year) + '不一致')
                        break
                    if seq != cal.period_num:
                        error = True
                        self.logger.message('日历' + cal_lst[0].cal_id + '的期间序号' + str(seq) + '与日历' + cal.cal_id + '的期间序号' + str(cal.period_num) + '不一致')
                        break
                    if cal_type != cal.pay_cal_type:
                        error = True
                        self.logger.message('日历' + cal_lst[0].cal_id + '的计算类型' + str(cal_type) + '与日历' + cal.cal_id + '的计算类型' + str(cal.pay_cal_type) + '不一致')
                        break
                    if cycle != cal.run_type_entity.cycle:
                        error = True
                        self.logger.message('日历' + cal_lst[0].cal_id + '的运行类型的性质' + str(cycle) + '与日历' + cal.cal_id + '的运行类型的性质' + str(cal.run_type_entity.cycle) + '不一致')
                        break
                    if self.cal_grp_obj.retro == 'Y':
                        if cal.run_type_entity.cycle != 'C':
                            error = True
                            self.logger.message('日历组' + self.cal_grp_obj.cal_group_id + '启用了追溯，但是日历' + cal.cal_id + '的运行类型的性质并不是C-周期')
                            break
            if error:
                return False
            else:
                return True
        else:
            return False

    def begin_identify(self):
        """
        Desc: 开始标记受款人
        Author: David
        Date: 2018/08/14
        """

        db = self.db
        run_param = self.run_param

        tenant_id = run_param['tenant_id']

        main_cost_day = get_main_cost_day(tenant_id)

        # 一次性获取所有被其他日历锁定的人员任职记录
        stat = self.db.get_table('hhr_py_payee_calc_stat', schema_name='boogoo_payroll')
        s = select([stat.c.hhr_empid, stat.c.hhr_emp_rcd, stat.c.hhr_py_cal_id], (stat.c.tenant_id == tenant_id) & (stat.c.hhr_lock_flg == '1'))
        rs = self.db.conn.execute(s).fetchall()
        for row in rs:
            emp_id = row['hhr_empid']
            emp_rcd = row['hhr_emp_rcd']
            lck_cal_id = row['hhr_py_cal_id']
            self.locked_payees.append((emp_id, emp_rcd, lck_cal_id))

        # 处理日历组的每个日历 (实际上日历组和日历一一对应)
        for cal in self.calendar_dic.values():
            prd_bgn_dt = cal.period_cal_entity.start_date
            prd_end_dt = cal.period_cal_entity.end_date
            py_group_id = cal.py_group_id
            cal_type = cal.pay_cal_type

            cal.main_cost_day = main_cost_day

            # 针对每个日历，一次性获取所有后续期间已存在计算结果的人员任职记录
            further_payees = []
            cata = self.db.get_table('hhr_py_cal_catalog', schema_name='boogoo_payroll')
            s = select([cata.c.hhr_empid, cata.c.hhr_emp_rcd],
                       (cata.c.tenant_id == tenant_id) & (cata.c.hhr_f_prd_end_dt > prd_end_dt)).distinct()
            rs = self.db.conn.execute(s).fetchall()
            for row in rs:
                emp_id = row['hhr_empid']
                emp_rcd = row['hhr_emp_rcd']
                further_payees.append((emp_id, emp_rcd))

            # 针对每个日历，一次性获取当前期间已存在周期计算结果的人员
            cycle_payees = []
            if (cal.run_type_entity.cycle == 'C') and (cal_type == 'A' or cal_type == 'B'):
                s = text("select distinct hhr_empid, hhr_emp_rcd from boogoo_payroll.hhr_py_cal_catalog "
                         "where tenant_id = :b1 and hhr_f_rt_cycle = 'C' "
                         "and (hhr_f_prd_bgn_dt <= :b2 and hhr_f_prd_end_dt >= :b3) ")
                rs = self.db.conn.execute(s, b1=tenant_id, b2=prd_end_dt, b3=prd_bgn_dt).fetchall()
                for row in rs:
                    emp_id = row['hhr_empid']
                    emp_rcd = row['hhr_emp_rcd']
                    cycle_payees.append((emp_id, emp_rcd))

            if cal_type == 'A':    # 常规
                # 增补人员
                add_list = cal.add_pers_list
                # 排除人员
                except_list = cal.except_pers_list
                except_t_list = []
                for p in except_list:
                    except_t_list.append((p.emp_id, p.emp_rcd))

                # 指定薪资组在薪资计算的期间内在职的人员（至少1天在职）
                # 根据薪资组、期间结束日期从基础薪酬表中获取员工任职（在期间结束日期属于此薪资组）
                stmt1 = text("select a.hhr_empid, a.hhr_emp_rcd from boogoo_payroll.hhr_py_assign_pg a where a.tenant_id = :b1 "
                             "and :b2 between a.hhr_efft_date and a.hhr_efft_end_date "
                             # "and a.hhr_efft_date = (select max(a1.hhr_efft_date) from boogoo_payroll.hhr_py_assign_pg a1 where a1.tenant_id = a.tenant_id "
                             # "and a1.hhr_empid = a.hhr_empid and a1.hhr_emp_rcd = a.hhr_emp_rcd and a1.hhr_efft_date <= :b2) "
                             "and a.hhr_efft_seq = (select max(a2.hhr_efft_seq) from boogoo_payroll.hhr_py_assign_pg a2 where a2.tenant_id = a.tenant_id "
                             "and a2.hhr_empid = a.hhr_empid and a2.hhr_emp_rcd = a.hhr_emp_rcd and a2.hhr_efft_date = a.hhr_efft_date) "
                             "and a.hhr_pygroup_id = :b3 ")

                stmt2 = text("select A.hhr_empid from boogoo_corehr.hhr_org_per_jobdata A where A.tenant_id = :b1 and A.hhr_efft_date > :b2 "
                             "and A.hhr_efft_date <= :b3 and A.hhr_status = 'Y' and A.hhr_empid = :b4 and A.hhr_emp_rcd = :b5 "
                             "union "
                             "select a.hhr_empid from boogoo_corehr.hhr_org_per_jobdata a where a.tenant_id = :b1 and a.hhr_status = 'Y' "
                             "and :b2 between a.hhr_efft_date and a.hhr_efft_end_date "
                             # "and a.hhr_efft_date = (select max(a1.hhr_efft_date) from boogoo_corehr.hhr_org_per_jobdata a1 "
                             # "where a1.tenant_id = a.tenant_id and a1.hhr_empid=a.hhr_empid and a1.hhr_emp_rcd = a.hhr_emp_rcd "
                             # "and a1.hhr_efft_date <= :b2) "
                             "and a.hhr_efft_seq = (select max(a1.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata a1 where a1.tenant_id = a.tenant_id "
                             "and a1.hhr_empid=a.hhr_empid and a1.hhr_emp_rcd = a.hhr_emp_rcd and a1.hhr_efft_date = a.hhr_efft_date) "
                             "and a.hhr_empid = :b4 and a.hhr_emp_rcd = :b5 ")

                rs1 = db.conn.execute(stmt1, b1=tenant_id, b2=prd_end_dt, b3=py_group_id).fetchall()
                for row1 in rs1:
                    emp_id = row1['hhr_empid']
                    emp_rcd = row1['hhr_emp_rcd']
                    if (emp_id, emp_rcd) in except_t_list:
                        continue
                    r = db.conn.execute(stmt2, b1=tenant_id, b2=prd_bgn_dt, b3=prd_end_dt, b4=emp_id, b5=emp_rcd).fetchone()
                    if r is not None:
                        payee = Payee(tenant_id=tenant_id, cal_id=cal.cal_id, emp_id=emp_id, emp_rcd=emp_rcd)
                        self.payee_check(cal=cal, payee=payee, fur_payees=further_payees, cycle_payees=cycle_payees)

                for p in add_list:
                    self.payee_check(cal=cal, payee=p, fur_payees=further_payees, cycle_payees=cycle_payees)

            elif cal_type == 'B':  # 单独
                solo_list = cal.add_pers_list
                for p in solo_list:
                    self.payee_check(cal=cal, payee=p, fur_payees=further_payees, cycle_payees=cycle_payees)

            elif cal_type == 'C':  # 更正
                upd_list = cal.update_pers_list
                for p in upd_list:
                    self.payee_check(cal=cal, payee=p, fur_payees=further_payees, cycle_payees=cycle_payees)

    def payee_check(self, **kwargs):
        """
        Desc: 校验受款人并标记
        Author: David
        Date: 2018/08/14
        """

        db = self.db
        cal = kwargs['cal']
        cal_type = cal.pay_cal_type
        cal_grp = self.cal_grp_obj

        payee = kwargs['payee']
        fur_payees = kwargs['fur_payees']
        cycle_payees = kwargs['cycle_payees']

        tenant_id = payee.tenant_id
        cal_grp_id = cal_grp.cal_group_id
        cal_id = cal.cal_id
        user_id = self.run_param['operator_user_id']

        emp_id = payee.emp_id
        emp_rcd = payee.emp_rcd

        # 1.校验人员是否已被其他日历锁定，若是则此人员标记失败
        error = False
        for lck_emp in self.locked_payees:
            if (emp_id, emp_rcd) == lck_emp[0:2]:
                # msg_class: A-标记，B-计算
                # msg_type: S-成功，E-失败，W-警告，I-通知
                lock_cal_id = lck_emp[2]
                msg_txt = '该员工已被日历' + lock_cal_id + '锁定'
                ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                                emp_rcd=emp_rcd, msg_class='A', msg_type='E', msg_txt=msg_txt, user_id=user_id)
                error = True

        # 2.校验人员后续期间是否已存在计算结果，若已存在则此人员标记失败（存在已有薪资计算结果的结束日期>当前结束日期，标记失败）
        if (emp_id, emp_rcd) in fur_payees:
            msg_txt = '该员工后续期间已存在计算结果'
            ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                            emp_rcd=emp_rcd, msg_class='A', msg_type='E', msg_txt=msg_txt, user_id=user_id)
            error = True

        # 3.当运行类型的性质为“C-周期”时，对常规计算或单独计算的人员，校验人员当前期间是否已存在历经期运行类型为周期的计算结果，
        #   若已存在则此人员标记失败（存在已有周期薪资计算结果的期间与当前期间交叉，标记失败）
        if (emp_id, emp_rcd) in cycle_payees:
            msg_txt = '该员工已存在周期的计算结果'
            ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                            emp_rcd=emp_rcd, msg_class='A', msg_type='E', msg_txt=msg_txt, user_id=user_id)
            error = True

        # 4.对常规计算的增补人员，校验期间结束日期的薪资组是否一致，若不一致则此人员标记失败
        # 5.对单独计算的人员，校验期间结束日期的薪资组是否一致，若不一致则此人员标记失败
        # 6.对更正计算的人员，校验期间结束日期的薪资组是否一致，若不一致则此人员标记失败
        if not payee_pay_group_check(payee, cal):
            msg_txt = '员工期间结束日期内的薪资组与日历的薪资组不一致'
            ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                            emp_rcd=emp_rcd, msg_class='A', msg_type='E', msg_txt=msg_txt, user_id=user_id)
            error = True

        # 7.对更正计算的人员，校验更正日历的期间编码、年度、序号、运行类型是否相同，并且是此人员序号最大的所在期日历，若不一致则此人员标记失败
        u_cal_id = None
        if cal_type == 'C':
            u_cal_id = payee.update_cal_id
            u_prd_id = payee.update_cal_entity.period_id
            u_prd_year = payee.update_cal_entity.period_year
            u_prd_num = payee.update_cal_entity.period_num
            u_run_type = payee.update_cal_entity.run_type_id

            cal_prd_id = cal.period_id
            cal_prd_year = cal.period_year
            cal_prd_num = cal.period_num
            cal_run_type = cal.run_type_id

            if u_prd_id != cal_prd_id or u_prd_year != cal_prd_year or u_prd_num != cal_prd_num or u_run_type != cal_run_type:
                msg_txt = '更正日历' + u_cal_id + '的期间编码、年度、序号、运行类型与运行日历' + cal_id + '不一致'
                ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                                emp_rcd=emp_rcd, msg_class='A', msg_type='E', msg_txt=msg_txt, user_id=user_id)
                error = True
            else:
                # 获取人员序号最大的所在期日历
                sql = text("select hhr_py_cal_id from boogoo_payroll.hhr_py_cal_catalog a where a.tenant_id = :b1 and a.hhr_empid = :b2 "
                           "and hhr_emp_rcd = :b3 order by a.hhr_seq_num desc")
                r = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd).fetchone()
                if r is not None:
                    prev_cal_id = r['hhr_py_cal_id']
                    if prev_cal_id != u_cal_id:
                        error = True
        if not error:
            """
            Desc: 按原逻辑应该标记成功的员工，再多加一重校验：
                  校验“核算控制”信息中，此员工的“工资锁定”字段是否=Y，
                  =Y则：hhr_py_ind_stat置为W，hhr_lock_flg置为0，同时记录原因“工资锁定”。
                  后续薪资计算则类似标记失败，不计算。
            Author: David
            Date: 2020/12/18
            """

            ind_stat = 'S'
            lock_flag = '1'
            t = text("select hhr_acc_sal_lock from boogoo_payroll.hhr_py_acc_cntrl where tenant_id = :b1 and hhr_empid = :b2")
            r = db.conn.execute(t, b1=tenant_id, b2=emp_id).fetchone()
            if r is not None:
                acc_sal_lock = r['hhr_acc_sal_lock']
                if acc_sal_lock == 'Y':
                    ind_stat = 'W'
                    lock_flag = '0'
                    msg_txt = '工资锁定'
                    ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                                    emp_rcd=emp_rcd, msg_class='A', msg_type='E', msg_txt=msg_txt, user_id=user_id)
                else:
                    self.locked_payees.append((emp_id, emp_rcd, cal_id))

            # lock: 1-锁定 | ind_stat: S-标记成功，W-标记警告
            ins_py_calc_stat(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                             emp_rcd=emp_rcd, upd_cal_id=u_cal_id, lock=lock_flag, ind_stat=ind_stat, calc_stat='', user_id=user_id, cal=cal)
        else:
            # lock: 0-未锁定 | ind_stat: E-标记失败
            ins_py_calc_stat(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_id, emp_id=emp_id,
                             emp_rcd=emp_rcd, upd_cal_id=u_cal_id, lock='0', ind_stat='E', calc_stat='', user_id=user_id, cal=cal)
