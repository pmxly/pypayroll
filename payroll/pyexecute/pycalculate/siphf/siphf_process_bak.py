# coding:utf-8

from ....pysysutils import global_variables as gv
from sqlalchemy import text
from ....pysysutils.func_lib_02 import round_rule
from ....pysysutils.func_lib_02 import set_pin_seg_amt
from ....pysysutils.func_lib_02 import ex_currency
from ....pysysutils.func_lib_02 import get_area_sal_lvl
from ....pysysutils.py_calc_log import add_fc_log_item


class SiPhfProcess:
    """
    员工社保公积金处理
    created by David 2018/10/11
    """

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.fc_obj = kwargs.get('fc_obj', None)
        self.catalog = kwargs.get('catalog', None)
        self.pg_currency = gv.get_var_value('VR_PG_CURRENCY')
        # 员工缴纳信息字典
        self.contrib_data_dic = {}  # {('PENSION','SH','ALL'):{'PENSION'', 'eff_dt': '2018-10-11', 'account_id': '1234567', ...}, ...}
        # 缴纳规则字典，key为元组(保险类型, 缴纳地, 缴纳账户编码)
        self.rule_dic = {}
        self.insur_pins_amt = {}

    def get_emp_contrib_data(self):
        """获取员工缴纳信息"""

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        f_end_date = catalog.f_prd_end_dt

        # 根据人员编码、历经期结束日期从员工保险缴纳信息表中获取账户状态打开的记录
        sql = text("select a.hhr_insur_type_code,a.hhr_efft_date,a.hhr_area_code,a.hhr_contrib_acc_code, "
                   "a.hhr_insur_rule_code,a.hhr_ee_base_amt,a.hhr_ee_tot_amt,a.hhr_ee_ovrd_flg,a.hhr_ee_take_method, "
                   "a.hhr_com_base_amt,a.hhr_com_tot_amt,a.hhr_com_ovrd_flg,a.hhr_com_take_method, a.hhr_status "
                   "from boogoo_payroll.hhr_py_emp_insurance a where a.tenant_id = :b1 and a.hhr_empid = :b2 "
                   "and a.hhr_efft_date <= :b3 order by a.hhr_insur_type_code, a.hhr_efft_date desc ")
        rs = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=f_end_date).fetchall()
        temp_lst = []
        for row in rs:
            insur_type = row['hhr_insur_type_code']

            if insur_type in temp_lst:
                continue
            temp_lst.append(insur_type)

            hhr_status = row['hhr_status']
            if hhr_status != 'Y':
                continue

            eff_date = row['hhr_efft_date']
            area_id = row['hhr_area_code']
            contrib_acc_cd = row['hhr_contrib_acc_code']
            insur_rule_id = row['hhr_insur_rule_code']
            ee_base_amt = row['hhr_ee_base_amt']
            ee_tot_amt = row['hhr_ee_tot_amt']
            ee_ovd_flg = row['hhr_ee_ovrd_flg']
            ee_take_mth = row['hhr_ee_take_method']
            com_base_amt = row['hhr_com_base_amt']
            com_tot_amt = row['hhr_com_tot_amt']
            com_ovd_flg = row['hhr_com_ovrd_flg']
            com_take_mth = row['hhr_com_take_method']
            # self.contrib_data_dic[(insur_type, area_id, contrib_acc_cd, insur_rule_id)] = {
            self.contrib_data_dic[(insur_type, area_id, insur_rule_id)] = {
                'insur_type': insur_type, 'area_id': area_id, 'eff_date': eff_date, 'contrib_acc_cd': contrib_acc_cd,
                'ee_base_amt': ee_base_amt,
                'ee_tot_amt': ee_tot_amt, 'ee_ovd_flg': ee_ovd_flg, 'ee_take_mth': ee_take_mth,
                'com_base_amt': com_base_amt, 'com_tot_amt': com_tot_amt, 'com_ovd_flg': com_ovd_flg,
                'com_take_mth': com_take_mth}

    def get_contrib_rule(self):
        """
        获取缴纳规则
       """

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        f_end_date = catalog.f_prd_end_dt

        # 根据历经期结束日期从险种规则表中获取有效状态的记录
        contrib_rule_dic = gv.get_run_var_value('CONTRIB_RULE_DIC')
        if contrib_rule_dic is None:
            contrib_rule_dic = dict()

        if f_end_date in contrib_rule_dic:
            self.rule_dic = contrib_rule_dic[f_end_date]
            return self.rule_dic
        else:
            temp_dic = {}
            sql = text("select a.hhr_insur_type_code,a.hhr_area_code,a.hhr_contrib_acc_code,a.hhr_insur_rule_code, "
                       "a.hhr_efft_date,a.hhr_contrib_freq,a.hhr_contrib_month,a.hhr_switch_month,a.hhr_currency, "
                       "a.hhr_ee_contrib_method,a.hhr_ee_cal_method_code,a.hhr_ee_base_fixed_amt,a.hhr_ee_base_contrib_rate, "
                       "a.hhr_ee_low_lmt_amt,a.hhr_ee_high_lmt_amt,a.hhr_ee_tot_fixed_amt,a.hhr_ee_add_amt, "
                       "a.hhr_ee_base_round_rule,a.hhr_ee_tot_round_rule, "
                       "a.hhr_com_contrib_method,a.hhr_com_cal_method_code,a.hhr_com_base_fixed_amt,a.hhr_com_base_contrib_rate, "
                       "a.hhr_com_low_lmt_amt,a.hhr_com_high_lmt_amt,a.hhr_com_tot_fixed_amt,a.hhr_com_add_amt, "
                       "a.hhr_com_base_round_rule,a.hhr_com_tot_round_rule, a.hhr_status "
                       "from boogoo_payroll.hhr_py_insurance_rule a where a.tenant_id = :b1 and a.hhr_efft_date <= :b2 "
                       "order by a.hhr_insur_type_code,a.hhr_area_code,a.hhr_contrib_acc_code,a.hhr_efft_date desc ")
            temp_lst = []
            rs = db.conn.execute(sql, b1=tenant_id, b2=f_end_date).fetchall()
            for row in rs:
                insur_type = row['hhr_insur_type_code']
                area_id = row['hhr_area_code']
                contrib_acc_cd = row['hhr_contrib_acc_code']
                insur_rule_id = row['hhr_insur_rule_code']

                if (insur_type, area_id, contrib_acc_cd, insur_rule_id) in temp_lst:
                    continue
                temp_lst.append((insur_type, area_id, contrib_acc_cd, insur_rule_id))

                hhr_status = row['hhr_status']
                if hhr_status != 'Y':
                    continue

                eff_date = row['hhr_efft_date']
                contrib_freq = row['hhr_contrib_freq']
                contrib_mon = row['hhr_contrib_month']
                switch_mon = row['hhr_switch_month']
                currency = row['hhr_currency']

                ee_contrib_mth = row['hhr_ee_contrib_method']
                ee_calc_mth = row['hhr_ee_cal_method_code']
                ee_base_fixed_amt = row['hhr_ee_base_fixed_amt']
                ee_base_cntrib_rt = row['hhr_ee_base_contrib_rate']
                ee_low_lmt_amt = row['hhr_ee_low_lmt_amt']
                ee_high_lmt_amt = row['hhr_ee_high_lmt_amt']
                ee_tot_fixed_amt = row['hhr_ee_tot_fixed_amt']
                ee_add_amt = row['hhr_ee_add_amt']
                ee_base_round_rule = row['hhr_ee_base_round_rule']
                ee_tot_round_rule = row['hhr_ee_tot_round_rule']

                com_contrib_mth = row['hhr_com_contrib_method']
                com_calc_mth = row['hhr_com_cal_method_code']
                com_base_fixed_amt = row['hhr_com_base_fixed_amt']
                com_base_cntrib_rt = row['hhr_com_base_contrib_rate']
                com_low_lmt_amt = row['hhr_com_low_lmt_amt']
                com_high_lmt_amt = row['hhr_com_high_lmt_amt']
                com_tot_fixed_amt = row['hhr_com_tot_fixed_amt']
                com_add_amt = row['hhr_com_add_amt']
                com_base_round_rule = row['hhr_com_base_round_rule']
                com_tot_round_rule = row['hhr_com_tot_round_rule']

                if self.pg_currency != currency:
                    ee_base_fixed_amt = ex_currency(currency, self.pg_currency, ee_base_fixed_amt)
                    ee_low_lmt_amt = ex_currency(currency, self.pg_currency, ee_low_lmt_amt)
                    ee_high_lmt_amt = ex_currency(currency, self.pg_currency, ee_high_lmt_amt)
                    ee_tot_fixed_amt = ex_currency(currency, self.pg_currency, ee_tot_fixed_amt)
                    ee_add_amt = ex_currency(currency, self.pg_currency, ee_add_amt)

                    com_base_fixed_amt = ex_currency(currency, self.pg_currency, com_base_fixed_amt)
                    com_low_lmt_amt = ex_currency(currency, self.pg_currency, com_low_lmt_amt)
                    com_high_lmt_amt = ex_currency(currency, self.pg_currency, com_high_lmt_amt)
                    com_tot_fixed_amt = ex_currency(currency, self.pg_currency, com_tot_fixed_amt)
                    com_add_amt = ex_currency(currency, self.pg_currency, com_add_amt)

                # temp_dic[(insur_type, area_id, contrib_acc_cd, insur_rule_id)] = {
                temp_dic[(insur_type, area_id, insur_rule_id)] = {
                    'eff_date': eff_date, 'contrib_freq': contrib_freq, 'contrib_mon': contrib_mon,
                    'switch_mon': switch_mon, 'currency': currency, 'pg_currency': self.pg_currency,
                    'ee_contrib_mth': ee_contrib_mth,
                    'ee_calc_mth': ee_calc_mth,
                    'ee_base_fixed_amt': ee_base_fixed_amt, 'ee_base_cntrib_rt': ee_base_cntrib_rt,
                    'ee_low_lmt_amt': ee_low_lmt_amt,
                    'ee_high_lmt_amt': ee_high_lmt_amt, 'ee_tot_fixed_amt': ee_tot_fixed_amt, 'ee_add_amt': ee_add_amt,
                    'ee_base_round_rule': ee_base_round_rule, 'ee_tot_round_rule': ee_tot_round_rule,
                    'com_contrib_mth': com_contrib_mth,
                    'com_calc_mth': com_calc_mth, 'com_base_fixed_amt': com_base_fixed_amt,
                    'com_base_cntrib_rt': com_base_cntrib_rt,
                    'com_low_lmt_amt': com_low_lmt_amt, 'com_high_lmt_amt': com_high_lmt_amt,
                    'com_tot_fixed_amt': com_tot_fixed_amt,
                    'com_add_amt': com_add_amt, 'com_base_round_rule': com_base_round_rule,
                    'com_tot_round_rule': com_tot_round_rule
                }
            contrib_rule_dic[f_end_date] = temp_dic
            gv.set_run_var_value('CONTRIB_RULE_DIC', contrib_rule_dic)
            self.rule_dic = contrib_rule_dic[f_end_date]

    def get_insurance_pins(self):
        """
        获取保险类型对应的薪资项目
        :return: 保险类型对应的薪资项目字典，Key为保险类型
        """

        insurance_pins_dic = gv.get_run_var_value('INSURANCE_PINS_DIC')
        if len(insurance_pins_dic) > 0:
            return insurance_pins_dic
        else:
            db = self.db
            catalog = self.catalog
            tenant_id = catalog.tenant_id
            insurance_pins_dic = dict()
            sql = text("select a.hhr_insur_type_code,a.hhr_payable_ee,a.hhr_payable_com,a.hhr_paid_ee,"
                       "a.hhr_paid_com from boogoo_payroll.hhr_py_insurance_type a where a.tenant_id = :b1")
            rs = db.conn.execute(sql, b1=tenant_id).fetchall()
            for row in rs:
                insur_type = row['hhr_insur_type_code']
                ee_payable = row['hhr_payable_ee']
                com_payable = row['hhr_payable_com']
                ee_paid = row['hhr_paid_ee']
                com_paid = row['hhr_paid_com']
                insurance_pins_dic[insur_type] = {'ee_payable': ee_payable, 'com_payable': com_payable,
                                                  'ee_paid': ee_paid, 'com_paid': com_paid}
            gv.set_run_var_value('INSURANCE_PINS_DIC', insurance_pins_dic)
            return insurance_pins_dic

    def check_insur_type(self, insur_type, insurance_pins_dic):
        """
        判断是否处理某个保险类型
        :param insur_type: 保险类型
        :param insurance_pins_dic 保险类型对应薪资项字典
        :return: True/False
        """

        # 获取系统薪资项目和适用范围薪资项目
        pins_dic = gv.get_pin_dic()

        # 1.针对员工缴纳信息中每个保险类型，判断对应的薪资项目：4个薪资项目均不属于系统薪资项目或适用范围薪资项目，此保险类型不处理；
        #   只要有1个薪资项目属于系统薪资项目或适用范围薪资项目，则继续处理
        fail_cnt = 0
        insur_pin_dic = insurance_pins_dic[insur_type]
        for pin_cd in insur_pin_dic.values():
            if pin_cd not in pins_dic:
                fail_cnt += 1
        if fail_cnt == len(insur_pin_dic):
            return False

        # 2.判断薪资项目表中是否已存在对应的薪资项目，不存在时继续处理；
        #   存在且处理列为空，继续处理；只要有1个存在且处理列为C或U，停止处理。
        for pin_cd in insur_pin_dic.values():
            if pin_cd in pins_dic:
                pin_obj = pins_dic[pin_cd]
                if pin_obj.prcs_flag == 'C' or pin_obj.prcs_flag == 'U' or pin_obj.prcs_flag == 'S':
                    return False
        return True

    def get_avg_mon_sal(self, area_sal_data_dic, switch_mon, area, f_period_year, f_period_no):
        """
        获取上一年度月平均工资
        :param area_sal_data_dic: 缴纳地工资水平字典，key为元组(地区, 年度, 生效日期)
        :param switch_mon: 切换月份
        :param area: 缴纳地
        :param f_period_year: 历经期期间年度
        :param f_period_no: 历经期期间序号
        :return avg_mon_sal: 上一年度月平均工资
        """

        eff_dt_lst = []
        avg_mon_sal = 0
        for tuple_key, value_dic in area_sal_data_dic.items():
            if tuple_key[0] == area and tuple_key[1] == f_period_year:
                eff_dt_lst.append(tuple_key[2])
        if len(eff_dt_lst) > 0:
            eff_dt = None
            # 如果(历经期)期间序号 < 切换月，取生效日期最小的记录
            if f_period_no < switch_mon:
                eff_dt = eff_dt_lst[0]
            # 如果(历经期)期间序号 >= 切换月，取生效日期最大的记录
            elif f_period_no >= switch_mon:
                eff_dt = eff_dt_lst[len(eff_dt_lst) - 1]
            area_sal_val_dic = area_sal_data_dic[(area, f_period_year, eff_dt)]
            avg_mon_sal = area_sal_val_dic['avg_mon_sal']
        return avg_mon_sal

    def get_base_amt(self, rule_val_dic, contrib_val_dic, area_sal_data_dic, base_calc_mth, f_period_year, f_period_no,
                     _type):
        """
        计算基数值
        :param rule_val_dic: 某个保险类型对应的缴纳规则字典
        :param contrib_val_dic: 员工某个保险类型的缴纳数据字典
        :param area_sal_data_dic: 缴纳地工资水平字典，key为元组(地区, 年度, 生效日期)
        :param base_calc_mth: 基数计算方法:
                              01-上一年度个人月平均工资（暂不实现此功能）
                              02-本市上一年度月平均工资
                              03-60%本市上一年度月平均工资
                              04-固定金额
                              99-用户输入
        :param f_period_year: 历经期期间年度
        :param f_period_no: 历经期期间序号
        :param _type: 个人'EE'/公司'ER'
        :return: 基数值
        """

        currency = rule_val_dic['currency']
        base_amt = 0
        # 切换月份
        switch_mon = rule_val_dic['switch_mon']
        area = contrib_val_dic['area_id']

        # 根据缴纳地取上一年度月平均工资 or 根据缴纳地取上一年度月平均工资，再乘以0.6
        if base_calc_mth == '02' or base_calc_mth == '03':
            avg_mon_sal = self.get_avg_mon_sal(area_sal_data_dic, switch_mon, area, f_period_year, f_period_no)
            if base_calc_mth == '02':
                base_amt = avg_mon_sal
            elif base_calc_mth == '03':
                base_amt = avg_mon_sal * 0.6

        # 取缴纳规则中的“基数固定金额”
        elif base_calc_mth == '04':
            if _type == 'EE':
                base_amt = rule_val_dic['ee_base_fixed_amt']
            elif _type == 'ER':
                base_amt = rule_val_dic['com_base_fixed_amt']
        # 取员工缴纳信息中的“缴纳基数”
        elif base_calc_mth == '99':
            if _type == 'EE':
                base_amt = contrib_val_dic['ee_base_amt']
            elif _type == 'ER':
                base_amt = contrib_val_dic['com_base_amt']
            # 员工缴纳信息中的金额需要按照险种规则中的货币进行转换
            if self.pg_currency != currency:
                base_amt = ex_currency(currency, self.pg_currency, base_amt)

        return base_amt

    def calc_final_amt(self, rule_val_dic, contrib_val_dic, area_sal_data_dic, f_period_year, f_period_no, _type):
        """
        计算最终金额
        :param rule_val_dic: 某个保险类型对应的缴纳规则字典
        :param contrib_val_dic: 员工某个保险类型的缴纳数据字典
        :param area_sal_data_dic: 缴纳地工资水平字典
        :param f_period_year: 历经期期间年度
        :param f_period_no: 历经期期间序号
        :param _type 个人'EE'/公司'ER'
        :return: None
        """

        pg_currency = gv.get_var_value('VR_PG_CURRENCY')

        insur_type = contrib_val_dic['insur_type']
        final_amt = 0
        contrib_mth = base_calc_mth = ovd_flg = None
        tot_fixed_amt = add_amt = contrib_tot_amt = base_cntrib_rt = 0
        tot_round_rule = None
        currency = rule_val_dic['currency']
        if _type == 'EE':
            # 个人缴纳方式(A-基数*比例，B-固定金额)
            contrib_mth = rule_val_dic['ee_contrib_mth']
            # 个人缴纳固定金额+附加金额
            tot_fixed_amt = rule_val_dic['ee_tot_fixed_amt']
            add_amt = rule_val_dic['ee_add_amt']
            ovd_flg = contrib_val_dic['ee_ovd_flg']
            contrib_tot_amt = contrib_val_dic['ee_tot_amt']
            base_cntrib_rt = rule_val_dic['ee_base_cntrib_rt']
            # 基数计算方法
            base_calc_mth = rule_val_dic['ee_calc_mth']
            # 最终金额取整规则
            tot_round_rule = rule_val_dic['ee_tot_round_rule']
        elif _type == 'ER':
            # 公司缴纳方式(A-基数*比例，B-固定金额)
            contrib_mth = rule_val_dic['com_contrib_mth']
            # 公司缴纳固定金额+附加金额
            tot_fixed_amt = rule_val_dic['com_tot_fixed_amt']
            add_amt = rule_val_dic['com_add_amt']
            ovd_flg = contrib_val_dic['com_ovd_flg']
            contrib_tot_amt = contrib_val_dic['com_tot_amt']
            base_cntrib_rt = rule_val_dic['com_base_cntrib_rt']
            base_calc_mth = rule_val_dic['com_calc_mth']
            # 最终金额取整规则
            tot_round_rule = rule_val_dic['com_tot_round_rule']

        # 当缴纳方式为固定金额时，根据缴纳规则中的：缴纳固定金额+附加金额，得到最终金额。
        # 若员工缴纳信息中“覆盖”列=Y，则最终金额直接取员工缴纳信息中的“总额”。
        if contrib_mth == 'B':
            if ovd_flg == 'Y':
                final_amt = contrib_tot_amt
                if pg_currency != currency:
                    final_amt = ex_currency(currency, pg_currency, final_amt)
            else:
                final_amt = tot_fixed_amt + add_amt

        # 当缴纳方式为基数*比例时，最终金额=基数*基数缴纳比例+附加金额
        if contrib_mth == 'A':
            base_amt = self.get_base_amt(rule_val_dic, contrib_val_dic, area_sal_data_dic,
                                         base_calc_mth, f_period_year, f_period_no, _type)

            low_lmt_amt = high_lmt_amt = 0
            base_round_rule = None
            if _type == 'EE':
                # 基数下限
                low_lmt_amt = rule_val_dic['ee_low_lmt_amt']
                # 基数上限
                high_lmt_amt = rule_val_dic['ee_high_lmt_amt']
                # 基数取整规则
                base_round_rule = rule_val_dic['ee_base_round_rule']
            elif _type == 'ER':
                # 基数下限
                low_lmt_amt = rule_val_dic['com_low_lmt_amt']
                # 基数上限
                high_lmt_amt = rule_val_dic['com_high_lmt_amt']
                # 基数取整规则
                base_round_rule = rule_val_dic['com_base_round_rule']

            # 取到的基数小于下限，使用下限（未维护下限代表无下限，无需比较）；
            if low_lmt_amt:
                if base_amt < low_lmt_amt:
                    base_amt = low_lmt_amt
            # 取到的基数大于上限，使用上限（未维护上限代表无上限，无需比较）
            if high_lmt_amt:
                if base_amt > high_lmt_amt:
                    base_amt = high_lmt_amt

            base_amt = round_rule(base_round_rule, base_amt)

            # 记录实际基数与实际比例，用于后续计算公积金超额计税金额
            if _type == 'EE':
                self.insur_pins_amt[insur_type]['ee_base_amt'] = base_amt
                self.insur_pins_amt[insur_type]['ee_base_cntrib_rt'] = base_cntrib_rt
            elif _type == 'ER':
                self.insur_pins_amt[insur_type]['com_base_amt'] = base_amt
                self.insur_pins_amt[insur_type]['com_base_cntrib_rt'] = base_cntrib_rt

            final_amt = base_amt * base_cntrib_rt / 100 + add_amt
            final_amt = round_rule(tot_round_rule, final_amt)

        return final_amt

    def get_phf_tax(self):
        """
        获取公积金超额纳税规则
        :return: 公积金超额纳税规则字典，key为元组(地区, 年度, 生效日期)
        """

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        f_end_date = catalog.f_prd_end_dt

        # 根据历经期结束日期从公积金超额纳税规则表中获取有效状态的记录
        phf_tax_dic = gv.get_run_var_value('PHF_TAX_DIC')
        if phf_tax_dic is None:
            phf_tax_dic = dict()

        if f_end_date in phf_tax_dic:
            return phf_tax_dic[f_end_date]
        else:
            temp_dic = {}
            sql = text(
                "select a.hhr_insur_type_code,a.hhr_area_code,a.hhr_tax_method,a.hhr_currency,a.hhr_switch_month, "
                "a.hhr_ee_high_lmt_meth,a.hhr_ee_multiplier,a.hhr_ee_high_lmt_rate,a.hhr_ee_exmpt_round_rl, "
                "a.hhr_com_high_lmt_meth,a.hhr_com_multiplier,a.hhr_com_high_lmt_rate,a.hhr_com_exmpt_round_rl,a.hhr_ee_fixed_amt, "
                "a.hhr_com_fixed_amt, a.hhr_status from boogoo_payroll.hhr_py_phf_tax_cfg a where a.tenant_id = :b1 and a.hhr_efft_date <=:b2 "
                "order by a.hhr_insur_type_code,a.hhr_area_code,a.hhr_efft_date desc ")
            temp_lst = []
            rs = db.conn.execute(sql, b1=tenant_id, b2=f_end_date).fetchall()
            for row in rs:
                insur_type = row['hhr_insur_type_code']
                area_id = row['hhr_area_code']

                if (insur_type, area_id) not in temp_lst:
                    temp_lst.append((insur_type, area_id))
                else:
                    continue

                hhr_status = row['hhr_status']
                if hhr_status != 'Y':
                    continue

                tax_method = row['hhr_tax_method']
                currency = row['hhr_currency']
                switch_mon = row['hhr_switch_month']

                ee_fixed_amt = row['hhr_ee_fixed_amt']
                ee_high_lmt_mth = row['hhr_ee_high_lmt_meth']
                ee_multi = row['hhr_ee_multiplier']
                ee_high_lmt_rt = row['hhr_ee_high_lmt_rate']
                ee_exmpt_round_rl = row['hhr_ee_exmpt_round_rl']

                com_fixed_amt = row['hhr_com_fixed_amt']
                com_high_lmt_mth = row['hhr_com_high_lmt_meth']
                com_multi = row['hhr_com_multiplier']
                com_high_lmt_rt = row['hhr_com_high_lmt_rate']
                com_exmpt_round_rl = row['hhr_com_exmpt_round_rl']

                if self.pg_currency != currency:
                    ee_fixed_amt = ex_currency(currency, self.pg_currency, ee_fixed_amt)
                    com_fixed_amt = ex_currency(currency, self.pg_currency, com_fixed_amt)

                temp_dic[(insur_type, area_id)] = {'tax_method': tax_method, 'currency': currency,
                                                   'pg_currency': self.pg_currency, 'switch_mon': switch_mon,
                                                   'ee_fixed_amt': ee_fixed_amt, 'ee_high_lmt_mth': ee_high_lmt_mth,
                                                   'ee_multi': ee_multi, 'ee_high_lmt_rt': ee_high_lmt_rt,
                                                   'ee_exmpt_round_rl': ee_exmpt_round_rl,
                                                   'com_fixed_amt': com_fixed_amt, 'com_high_lmt_mth': com_high_lmt_mth,
                                                   'com_multi': com_multi, 'com_high_lmt_rt': com_high_lmt_rt,
                                                   'com_exmpt_round_rl': com_exmpt_round_rl}
            phf_tax_dic[f_end_date] = temp_dic
            gv.set_run_var_value('PHF_TAX_DIC', phf_tax_dic)
            return phf_tax_dic[f_end_date]

    def get_tax_free_amt(self, area, area_sal_data_dic, v_dic, _type):
        """
        获取免税金额
        :param area: 缴纳地
        :param area_sal_data_dic: 缴纳地工资水平字典，key为元组(地区, 年度, 生效日期)
        :param v_dic: 公积金超额纳税规则数据字典
        :param _type: 个人'EE'/公司'ER'
        :return: 免税金额
        """

        # 获取基数上限值
        switch_mon = v_dic['switch_mon']
        f_period_year = gv.get_var_value('VR_F_PERIOD_YEAR')
        f_period_no = gv.get_var_value('VR_F_PERIOD_NO')
        base_ceil_amt = self.get_avg_mon_sal(area_sal_data_dic, switch_mon, area, f_period_year, f_period_no)

        multi = high_lmt_rt = 0
        exmpt_round_rl = None
        if _type == 'EE':
            # 倍数
            multi = v_dic['ee_multi']
            if multi is None or multi == 0:
                multi = 1
            # 比例上限
            high_lmt_rt = v_dic['ee_high_lmt_rt']
            # 免税额取整规则
            exmpt_round_rl = v_dic['ee_exmpt_round_rl']
        elif _type == 'ER':
            multi = v_dic['com_multi']
            if multi is None or multi == 0:
                multi = 1
            high_lmt_rt = v_dic['com_high_lmt_rt']
            exmpt_round_rl = v_dic['com_exmpt_round_rl']

        # 免税金额 = 基数上限*倍数*比例上限
        tax_free_amt = base_ceil_amt * multi * high_lmt_rt / 100
        # tax_free_amt = round_rule(exmpt_round_rl, tax_free_amt)
        return tax_free_amt

    def calc_phf_tax(self):
        """
        计算公积金超额计税金额
        :return:
        """

        area = gv.get_var_value('VR_TAXAREA')

        phf_tax_dic = self.get_phf_tax()
        if phf_tax_dic is None:
            return
        pg_currency = self.pg_currency
        for tuple_key, contrib_val_dic in self.contrib_data_dic.items():
            insur_type = tuple_key[0]
            pins_amt_dic = self.insur_pins_amt[insur_type]
            # if len(pins_amt_dic) == 0:
            #     continue
            if (insur_type, area) in phf_tax_dic:
                v_dic = phf_tax_dic[(insur_type, area)]
                # 纳税方式(A-超“基数上限*比例上限”，B-基数或比例任一超上限，C-超固定金额)
                tax_method = v_dic['tax_method']

                ee_high_lmt_mth = v_dic['ee_high_lmt_mth']
                com_high_lmt_mth = v_dic['com_high_lmt_mth']

                ee_payable_amt = pins_amt_dic.get('ee_payable_amt', 0)
                com_payable_amt = pins_amt_dic.get('com_payable_amt', 0)

                ee_exmpt_round_rl = v_dic['ee_exmpt_round_rl']
                com_exmpt_round_rl = v_dic['com_exmpt_round_rl']

                # 超“基数上限*比例上限”
                if tax_method == 'A':
                    area_sal_data_dic = get_area_sal_lvl(currency=pg_currency)

                    # 当员工基数上限=A（社平工资）时，基数上限取纳税地社平工资
                    if ee_high_lmt_mth == 'A':
                        ee_tax_free_amt = self.get_tax_free_amt(area, area_sal_data_dic, v_dic, 'EE')

                        # 比较应缴金额与免税金额，超过免税金额的部分计入税基
                        #   个人部分计入：WT_TAXBASE_PHF_EE
                        ee_payable_amt = pins_amt_dic.get('ee_payable_amt', 0)
                        ee_gap = ee_payable_amt - ee_tax_free_amt
                        if ee_gap > 0:
                            ee_gap = round_rule(ee_exmpt_round_rl, ee_gap)
                            set_pin_seg_amt('WT_TAXBASE_PHF_EE', ee_gap, currency=pg_currency,
                                            round_rule_id=ee_exmpt_round_rl)

                        #  个人免税金额：WT_TAXFREE_PHF_EE
                        ee_tax_free_amt = round_rule(ee_exmpt_round_rl, ee_tax_free_amt)
                        set_pin_seg_amt('WT_TAXFREE_PHF_EE', ee_tax_free_amt, currency=pg_currency,
                                        round_rule_id=ee_exmpt_round_rl)

                    # 当公司基数上限=A（社平工资）时，基数上限取纳税地社平工资
                    if com_high_lmt_mth == 'A':
                        com_tax_free_amt = self.get_tax_free_amt(area, area_sal_data_dic, v_dic, 'ER')

                        # 比较应缴金额与免税金额，超过免税金额的部分计入税基
                        #   公司部分计入：WT_TAXBASE_PHF_ER
                        com_payable_amt = pins_amt_dic.get('com_payable_amt', 0)
                        com_gap = com_payable_amt - com_tax_free_amt
                        if com_gap > 0:
                            com_gap = round_rule(com_exmpt_round_rl, com_gap)
                            set_pin_seg_amt('WT_TAXBASE_PHF_ER', com_gap, currency=pg_currency,
                                            round_rule_id=com_exmpt_round_rl)

                        #  公司免税金额：WT_TAXFREE_PHF_ER
                        com_tax_free_amt = round_rule(com_exmpt_round_rl, com_tax_free_amt)
                        set_pin_seg_amt('WT_TAXFREE_PHF_ER', com_tax_free_amt, currency=pg_currency,
                                        round_rule_id=com_exmpt_round_rl)

                # 基数或比例任一超上限
                elif tax_method == 'B':
                    area_sal_data_dic = get_area_sal_lvl(currency=pg_currency)
                    switch_mon = v_dic['switch_mon']
                    f_period_year = gv.get_var_value('VR_F_PERIOD_YEAR')
                    f_period_no = gv.get_var_value('VR_F_PERIOD_NO')
                    base_ceil_amt = self.get_avg_mon_sal(area_sal_data_dic, switch_mon, area, f_period_year,
                                                         f_period_no)
                    # 当员工基数上限=A（社平工资）时，取纳税地社平工资
                    if ee_high_lmt_mth == 'A':
                        # 倍数
                        ee_multi = v_dic['ee_multi']
                        if ee_multi is None or ee_multi == 0:
                            ee_multi = 1
                        # 实际基数
                        ee_base_amt = pins_amt_dic.get('ee_base_amt', 0)
                        # 实际比例
                        ee_base_cntrib_rt = pins_amt_dic.get('ee_base_cntrib_rt', 0)
                        # 比例上限
                        ee_high_lmt_rt = v_dic['ee_high_lmt_rt']
                        # 免税额取整规则
                        ee_exmpt_round_rl = v_dic['ee_exmpt_round_rl']

                        # 超额基数(比较实际基数与基数上限*倍数，超过部分为超额基数)
                        gap_base_amt = ee_base_amt - base_ceil_amt * ee_multi
                        if gap_base_amt <= 0:
                            gap_base_amt = 0
                        # gap_base_amt = round_rule(ee_exmpt_round_rl, gap_base_amt)
                        # 超额比例(比例实际比例与比例上限，超过部分为超额比例)
                        gap_rate = ee_base_cntrib_rt - ee_high_lmt_rt
                        if gap_rate <= 0:
                            gap_rate = 0

                        # 税基WT_TAXBASE_PHF_EE = 超额基数*实际比例+实际基数*超额比例，按取整规则进行处理
                        tax_base_amt = gap_base_amt * ee_base_cntrib_rt / 100 + ee_base_amt * gap_rate / 100
                        if tax_base_amt > 0:
                            tax_base_amt = round_rule(ee_exmpt_round_rl, tax_base_amt)
                            set_pin_seg_amt('WT_TAXBASE_PHF_EE', tax_base_amt, currency=pg_currency,
                                            round_rule_id=ee_exmpt_round_rl)
                        # 个人免税金额WT_TAXFREE_PHF_EE=应缴金额-纳税金额
                        tax_free = ee_payable_amt - tax_base_amt
                        if tax_free > 0:
                            tax_free = round_rule(ee_exmpt_round_rl, tax_free)
                            set_pin_seg_amt('WT_TAXFREE_PHF_EE', tax_free, currency=pg_currency,
                                            round_rule_id=ee_exmpt_round_rl)

                    # 当公司基数上限=A（社平工资）时，基数上限取纳税地社平工资
                    if com_high_lmt_mth == 'A':
                        com_multi = v_dic['com_multi']
                        if com_multi is None or com_multi == 0:
                            com_multi = 1
                        com_base_amt = pins_amt_dic.get('com_base_amt', 0)
                        com_base_cntrib_rt = pins_amt_dic.get('com_base_cntrib_rt', 0)
                        com_high_lmt_rt = v_dic['com_high_lmt_rt']
                        com_exmpt_round_rl = v_dic['com_exmpt_round_rl']

                        # 超额基数(比较实际基数与基数上限*倍数，超过部分为超额基数)
                        gap_base_amt = com_base_amt - base_ceil_amt * com_multi
                        if gap_base_amt <= 0:
                            gap_base_amt = 0
                        # gap_base_amt = round_rule(com_exmpt_round_rl, gap_base_amt)
                        # 超额比例(比例实际比例与比例上限，超过部分为超额比例)
                        gap_rate = com_base_cntrib_rt - com_high_lmt_rt
                        if gap_rate <= 0:
                            gap_rate = 0

                        # 税基WT_TAXBASE_PHF_ER = 超额基数*实际比例+实际基数*超额比例，按取整规则进行处理
                        tax_base_amt = gap_base_amt * com_base_cntrib_rt / 100 + com_base_amt * gap_rate / 100
                        if tax_base_amt > 0:
                            tax_base_amt = round_rule(com_exmpt_round_rl, tax_base_amt)
                            set_pin_seg_amt('WT_TAXBASE_PHF_ER', tax_base_amt, currency=pg_currency,
                                            round_rule_id=com_exmpt_round_rl)
                        # 公司免税金额WT_TAXFREE_PHF_ER=应缴金额-纳税金额
                        tax_free = com_payable_amt - tax_base_amt
                        if tax_free > 0:
                            tax_free = round_rule(com_exmpt_round_rl, tax_free)
                            set_pin_seg_amt('WT_TAXFREE_PHF_ER', tax_free, currency=pg_currency,
                                            round_rule_id=com_exmpt_round_rl)
                # 超固定金额
                elif tax_method == 'C':
                    # 固定金额
                    ee_fixed_amt = v_dic['ee_fixed_amt']
                    com_fixed_amt = v_dic['com_fixed_amt']
                    # 税基 = 应缴金额 - 固定金额
                    ee_tax_base = ee_payable_amt - ee_fixed_amt
                    if ee_tax_base > 0:
                        set_pin_seg_amt('WT_TAXBASE_PHF_EE', ee_tax_base, currency=pg_currency)
                    com_tax_base = com_payable_amt - com_fixed_amt
                    if com_tax_base > 0:
                        set_pin_seg_amt('WT_TAXBASE_PHF_ER', com_tax_base, currency=pg_currency)
                    # 免税金额工资项（即维护的固定金额）
                    set_pin_seg_amt('WT_TAXFREE_PHF_EE', ee_fixed_amt, currency=pg_currency)
                    set_pin_seg_amt('WT_TAXFREE_PHF_ER', com_fixed_amt, currency=pg_currency)

    def calc_phf_tax_for_yt(self):
        """
        计算公积金超额计税金额(yt)
        :return:
        """

        area = gv.get_var_value('VR_TAXAREA')

        phf_tax_dic = self.get_phf_tax()

        if phf_tax_dic is None:
            return
        pg_currency = self.pg_currency
        insur_type = 'PHF'
        pins_amt_dic = {}
        if (insur_type, area) in phf_tax_dic:
            v_dic = phf_tax_dic[(insur_type, area)]
            # 纳税方式(A-超“基数上限*比例上限”，B-基数或比例任一超上限，C-超固定金额)
            tax_method = v_dic['tax_method']

            ee_payable_amt = pins_amt_dic.get('ee_payable_amt', 0)
            com_payable_amt = pins_amt_dic.get('com_payable_amt', 0)

            # 超固定金额
            if tax_method == 'C':
                # 固定金额
                ee_fixed_amt = v_dic['ee_fixed_amt']
                com_fixed_amt = v_dic['com_fixed_amt']
                # 税基 = 应缴金额 - 固定金额
                ee_tax_base = ee_payable_amt - ee_fixed_amt
                if ee_tax_base > 0:
                    set_pin_seg_amt('WT_TAXBASE_PHF_EE', ee_tax_base, currency=pg_currency)
                com_tax_base = com_payable_amt - com_fixed_amt
                if com_tax_base > 0:
                    set_pin_seg_amt('WT_TAXBASE_PHF_ER', com_tax_base, currency=pg_currency)
                # 免税金额工资项（即维护的固定金额）
                set_pin_seg_amt('WT_TAXFREE_PHF_EE', ee_fixed_amt, currency=pg_currency)
                set_pin_seg_amt('WT_TAXFREE_PHF_ER', com_fixed_amt, currency=pg_currency)

    def process_si_phf(self):
        """处理员工社保/公积金薪资项"""

        self.get_contrib_rule()
        self.get_emp_contrib_data()
        rule_dic = self.rule_dic
        insurance_pins_dic = self.get_insurance_pins()
        pg_currency = self.pg_currency
        area_sal_data_dic = get_area_sal_lvl(currency=pg_currency)
        f_period_year = gv.get_var_value('VR_F_PERIOD_YEAR')
        f_period_no = gv.get_var_value('VR_F_PERIOD_NO')

        vr_si_phf = gv.get_var_value('VR_SIPHFID')

        for tuple_key, contrib_val_dic in self.contrib_data_dic.items():
            insur_type = tuple_key[0]
            self.insur_pins_amt[insur_type] = {}

            if vr_si_phf == 'Y':
                if self.check_insur_type(insur_type, insurance_pins_dic):
                    # 3.按规则计算：
                    # 根据员工缴纳信息中的保险类型、缴纳地、缴纳账户编码、规则获取缴纳规则的对应记录
                    rule_val_dic = rule_dic[tuple_key]

                    # 最终金额取整规则
                    ee_tot_round_rule = rule_val_dic['ee_tot_round_rule']
                    com_tot_round_rule = rule_val_dic['com_tot_round_rule']

                    # 缴纳月份
                    contrib_mon = rule_val_dic['contrib_mon']
                    # -缴纳频率(A-每年，I-每月)
                    contrib_freq = rule_val_dic['contrib_freq']
                    # --当缴纳频率为每年时，判断历经期是否为缴纳月份，是继续计算，否则停止处理
                    # --当缴纳频率为每月时，直接计算
                    if contrib_freq == 'A':
                        if f_period_no != contrib_mon:
                            continue
                    elif contrib_freq == 'I':
                        pass
                    else:
                        continue

                    final_amt_ee = self.calc_final_amt(rule_val_dic, contrib_val_dic, area_sal_data_dic, f_period_year,
                                                       f_period_no, 'EE')
                    final_amt_com = self.calc_final_amt(rule_val_dic, contrib_val_dic, area_sal_data_dic, f_period_year,
                                                        f_period_no, 'ER')

                    # 承担方式
                    ee_take_mth = contrib_val_dic['ee_take_mth']
                    com_take_mth = contrib_val_dic['com_take_mth']

                    insur_pin_dic = insurance_pins_dic[insur_type]

                    """注意：社保金额处理只使用险种规则配置上的取整规则，不使用薪资项目上的取整规则"""

                    # 社保公积金薪资项按理不分段
                    set_pin_seg_amt(insur_pin_dic['ee_payable'], final_amt_ee, currency=pg_currency,
                                    round_rule_id=ee_tot_round_rule)
                    set_pin_seg_amt(insur_pin_dic['com_payable'], final_amt_com, currency=pg_currency,
                                    round_rule_id=com_tot_round_rule)

                    ee_paid_amt = 0
                    com_paid_amt = 0
                    # 当个人部分的承担方式为个人承担时，个人应缴归入个人实缴
                    if ee_take_mth == 'A':
                        ee_paid_amt = final_amt_ee

                    # 当个人部分的承担方式为公司承担时，个人应缴归入公司实缴
                    if ee_take_mth == 'B':
                        com_paid_amt = final_amt_ee

                    # 当公司部分的承担方式为个人承担时，公司应缴归入个人实缴
                    if com_take_mth == 'A':
                        ee_paid_amt += final_amt_com

                    # 当公司部分的承担方式为公司承担时，公司应缴归入公司实缴
                    if com_take_mth == 'B':
                        com_paid_amt += final_amt_com

                    set_pin_seg_amt(insur_pin_dic['ee_paid'], ee_paid_amt, currency=pg_currency,
                                    round_rule_id=ee_tot_round_rule)
                    set_pin_seg_amt(insur_pin_dic['com_paid'], com_paid_amt, currency=pg_currency,
                                    round_rule_id=com_tot_round_rule)

                    self.insur_pins_amt[insur_type]['ee_payable_amt'] = final_amt_ee
                    self.insur_pins_amt[insur_type]['com_payable_amt'] = final_amt_com

                    add_fc_log_item(self.fc_obj, 'WT', insur_pin_dic['ee_payable'])
                    add_fc_log_item(self.fc_obj, 'WT', insur_pin_dic['com_payable'])
                    add_fc_log_item(self.fc_obj, 'WT', insur_pin_dic['ee_paid'])
                    add_fc_log_item(self.fc_obj, 'WT', insur_pin_dic['com_paid'])

        # self.calc_phf_tax()
        self.calc_phf_tax_for_yt()
