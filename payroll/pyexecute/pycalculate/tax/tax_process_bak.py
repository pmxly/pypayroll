# coding:utf-8

from ....pysysutils import global_variables as gv
from sqlalchemy import text
from ....pysysutils.func_lib_02 import set_pin_seg_amt
from ....pysysutils.func_lib_02 import get_acc_year_seq, get_prd_year_seq, get_prd_date_lst
from ....pyfunctions.FC_BR import PyFunction as Br
from ....pysysutils.func_lib_02 import get_max_avg_anl_sal
from ....pysysutils.py_calc_log import add_fc_log_item
from decimal import *


class TaxProcess:
    """
    员工个税处理
    created by David 2018/10/18
    """

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.fc_obj = kwargs.get('fc_obj', None)
        self.catalog = kwargs.get('catalog', None)
        self.pg_currency = gv.get_var_value('VR_PG_CURRENCY')
        self.period_status = gv.get_var_value('VR_PERIOD_STATUS')
        self.pins_obj_dic = gv.get_pin_dic()
        self.tax_pin_dic = {}
        self.tax_wc_dic = {}

        # 根据纳税地取总免税金额=平均年度工资*3(记为q)
        pg_currency = gv.get_var_value('VR_PG_CURRENCY')
        area = gv.get_var_value('VR_TAXAREA')
        f_period_year = gv.get_var_value('VR_F_PERIOD_YEAR')

        avg_anl_sal = get_max_avg_anl_sal(area, f_period_year, pg_currency)
        self.tot_tax_free_q = avg_anl_sal * 3

    def init_tax_pin_dic(self):
        """
        初始化个税薪资项目字典
        :return:
        """

        self.tax_pin_dic = {}
        tax_pin_dic = self.tax_pin_dic

        # 计税金额(公司反算)
        tax_pin_dic['WT_TAXBASE01_ER_B'] = 0  # 工资计税(公司反算)
        tax_pin_dic['WT_TAXBASE02_ER_B'] = 0  # 年终奖计税(公司反算)
        tax_pin_dic['WT_TAXBASE03_ER_B'] = 0  # 离职补偿金计税(公司反算)
        tax_pin_dic['WT_TAXBASE04_ER_B'] = 0  # 其他所得计税(公司反算)

        # 个税
        tax_pin_dic['WT_TAX01_EE'] = 0  # 工资税
        tax_pin_dic['WT_TAX01_ER'] = 0  # 工资税(公司)
        tax_pin_dic['WT_TAX02_EE'] = 0  # 年终奖税
        tax_pin_dic['WT_TAX02_ER'] = 0  # 年终奖税(公司)
        tax_pin_dic['WT_TAX03_EE'] = 0  # 离职补偿金税
        tax_pin_dic['WT_TAX03_ER'] = 0  # 离职补偿金税(公司)
        tax_pin_dic['WT_TAX04_EE'] = 0  # 其他所得税
        tax_pin_dic['WT_TAX04_ER'] = 0  # 其他所得税(公司)

        tax_pin_dic['WT_TAX05_EE'] = 0  # 劳务报酬税
        tax_pin_dic['WT_TAX05_ER'] = 0  # 劳务报酬税(公司)

        tax_pin_dic['WT_TAX_EE'] = 0  # 个税(个人)
        tax_pin_dic['WT_TAX_ER'] = 0  # 个税(公司)
        tax_pin_dic['WT_TAXBASE_ER_B'] = 0  # 计税金额（公司反算）
        tax_pin_dic['WT_TAX_EE_T'] = 0  # 应纳税额(个人)
        tax_pin_dic['WT_TAX_ER_T'] = 0  # 应纳税额(公司)
        tax_pin_dic['WT_TAXBASE_ACT_EE_T'] = 0  # 应纳税所得额(个人)
        tax_pin_dic['WT_TAXBASE_ACT_ER_T'] = 0  # 应纳税所得额(公司)

        # 应税金额
        tax_pin_dic['WT_TAXBASE01_ACT_EE'] = 0  # 工资应税金额(个人)
        tax_pin_dic['WT_TAXBASE01_ACT_ER'] = 0  # 工资应税金额(公司)
        tax_pin_dic['WT_TAXBASE01_ACT_ER_B'] = 0  # 工资应税金额(公司反算)
        tax_pin_dic['WT_TAXBASE02_ACT_EE'] = 0  # 年终奖应税金额(个人)
        tax_pin_dic['WT_TAXBASE02_ACT_ER'] = 0  # 年终奖应税金额(公司)
        tax_pin_dic['WT_TAXBASE02_ACT_ER_B'] = 0  # 年终奖应税金额(公司反算)
        tax_pin_dic['WT_TAXBASE03_ACT_EE'] = 0  # 离职补偿金应税金额(个人)
        tax_pin_dic['WT_TAXBASE03_ACT_ER'] = 0  # 离职补偿金应税金额(公司)
        tax_pin_dic['WT_TAXBASE03_ACT_ER_B'] = 0  # 离职补偿金应税金额(公司反算)

        # 免税金额
        tax_pin_dic['WT_TAXFREE01_EE'] = 0  # 工资免税金额(个人)
        tax_pin_dic['WT_TAXFREE01_ER'] = 0  # 工资免税金额(公司)
        tax_pin_dic['WT_TAXFREE02_EE'] = 0  # 年终奖免税金额(个人)
        tax_pin_dic['WT_TAXFREE02_ER'] = 0  # 年终奖免税金额(公司)
        tax_pin_dic['WT_TAXFREE03_EE'] = 0  # 离职补偿金免税金额(个人)
        tax_pin_dic['WT_TAXFREE03_ER'] = 0  # 离职补偿金免税金额(公司)

        add_fc_log_item(self.fc_obj, 'WT', '', tax_pin_dic)

    def init_tax_wc_dic(self):
        """
        初始化个税薪资项目累计字典
        :return:
        """

        self.tax_wc_dic = {}
        tax_wc_dic = self.tax_wc_dic

        # 计税金额累计
        tax_wc_dic['WC_TAXBASE01_EE_M'] = 0  # 工资计税(个人)月度累计
        tax_wc_dic['WC_TAXBASE01_ER_M'] = 0  # 工资计税(公司)月度累计
        tax_wc_dic['WC_TAXBASE01_ER_B_M'] = 0  # 工资计税(公司反算)月度累计
        tax_wc_dic['WC_TAXBASE02_EE_M'] = 0  # 年终奖计税(个人)月度累计
        tax_wc_dic['WC_TAXBASE02_ER_M'] = 0  # 年终奖计税(公司)月度累计
        tax_wc_dic['WC_TAXBASE02_ER_B_M'] = 0  # 年终奖计税(公司反算)月度累计
        tax_wc_dic['WC_TAXBASE03_EE_M'] = 0  # 离职补偿金计税(个人)月度累计
        tax_wc_dic['WC_TAXBASE03_ER_M'] = 0  # 离职补偿金计税(公司)月度累计
        tax_wc_dic['WC_TAXBASE03_ER_B_M'] = 0  # 离职补偿金计税(公司反算)月度累计
        tax_wc_dic['WC_TAXBASE04_EE_M'] = 0  # 其他所得计税(个人)月度累计
        tax_wc_dic['WC_TAXBASE04_ER_M'] = 0  # 其他所得计税(公司)月度累计
        tax_wc_dic['WC_TAXBASE04_ER_B_M'] = 0  # 其他所得计税(公司反算)月度累计

        tax_wc_dic['WC_TAXBASE05_EE_M'] = 0  # 劳务报酬计税(个人)月度累计
        tax_wc_dic['WC_TAXBASE05_ER_M'] = 0  # 劳务报酬计税(公司)月度累计
        tax_wc_dic['WC_TAXBASE05_ER_B_M'] = 0  # 劳务报酬计税(公司反算)月度累计

        # 已缴纳的个税累计
        tax_wc_dic['WC_TAX01_EE_M'] = 0  # 工资税月度累计
        tax_wc_dic['WC_TAX01_ER_M'] = 0  # 工资税(公司)月度累计
        tax_wc_dic['WC_TAX02_EE_M'] = 0  # 年终奖税月度累计
        tax_wc_dic['WC_TAX02_ER_M'] = 0  # 年终奖税(公司)月度累计
        tax_wc_dic['WC_TAX03_EE_M'] = 0  # 离职补偿金税月度累计
        tax_wc_dic['WC_TAX03_ER_M'] = 0  # 离职补偿金税(公司)月度累计
        tax_wc_dic['WC_TAX04_EE_M'] = 0  # 其他所得税月度累计
        tax_wc_dic['WC_TAX04_ER_M'] = 0  # 其他所得税(公司)月度累计

        tax_wc_dic['WC_TAX05_EE_M'] = 0  # 劳务报酬税月度累计
        tax_wc_dic['WC_TAX05_ER_M'] = 0  # 劳务报酬税(公司)月度累计

        # 应税金额
        tax_wc_dic['WC_TAXBASE01_ACT_EE_M'] = 0  # 工资应税金额(个人)月度累计
        tax_wc_dic['WC_TAXBASE01_ACT_ER_M'] = 0  # 工资应税金额(公司)月度累计
        tax_wc_dic['WC_TAXBASE01_ACT_ER_B_M'] = 0  # 工资应税金额(公司反算)月度累计
        tax_wc_dic['WC_TAXBASE02_ACT_EE_M'] = 0  # 年终奖应税金额(个人)月度累计
        tax_wc_dic['WC_TAXBASE02_ACT_ER_M'] = 0  # 年终奖应税金额(公司)月度累计
        tax_wc_dic['WC_TAXBASE02_ACT_ER_B_M'] = 0  # 年终奖应税金额(公司反算)月度累计
        tax_wc_dic['WC_TAXBASE03_ACT_EE_M'] = 0  # 离职补偿金应税金额(个人)月度累计
        tax_wc_dic['WC_TAXBASE03_ACT_ER_M'] = 0  # 离职补偿金应税金额(公司)月度累计
        tax_wc_dic['WC_TAXBASE03_ACT_ER_B_M'] = 0  # 离职补偿金应税金额(公司反算)月度累计

        tax_wc_dic['WC_TAXBASE05_ACT_EE_M'] = 0  # 劳务报酬应税金额(个人)月度累计
        tax_wc_dic['WC_TAXBASE05_ACT_ER_M'] = 0  # 劳务报酬应税金额(公司)月度累计
        tax_wc_dic['WC_TAXBASE05_ACT_ER_B_M'] = 0  # 劳务报酬应税金额(公司反算)月度累计

        # 免税金额
        tax_wc_dic['WC_TAXFREE01_EE_M'] = 0  # 工资免税金额(个人)月度累计
        tax_wc_dic['WC_TAXFREE01_ER_M'] = 0  # 工资免税金额(公司)月度累计
        tax_wc_dic['WC_TAXFREE02_EE_M'] = 0  # 年终奖免税金额(个人)月度累计
        tax_wc_dic['WC_TAXFREE02_ER_M'] = 0  # 年终奖免税金额(公司)月度累计
        tax_wc_dic['WC_TAXFREE03_EE_M'] = 0  # 离职补偿金免税金额(个人)月度累计
        tax_wc_dic['WC_TAXFREE03_ER_M'] = 0  # 离职补偿金免税金额(公司)月度累计

        tax_wc_dic['WC_TAXFREE05_EE_M'] = 0  # 劳务报酬免税金额(个人)月度累计
        tax_wc_dic['WC_TAXFREE05_ER_M'] = 0  # 劳务报酬免税金额(公司)月度累计

        # 个税月度累计
        tax_wc_dic['WC_TAX_EE_M'] = 0
        # 个税(公司)月度累计
        tax_wc_dic['WC_TAX_ER_M'] = 0

        add_fc_log_item(self.fc_obj, 'WC', '', tax_wc_dic)

    def copy_from_hist_tax(self, hist_seq):
        """
        获取历史序号对应期间的个税金额
        (默认个税薪资项不分段，分段号为999)
        :return: 个税薪资项历史金额字典 (Key为个税薪资项目编码，Value为其对应的金额)
        """

        db = self.db
        catalog = self.catalog
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = hist_seq

        sql = text("select hhr_pin_cd, hhr_amt, hhr_currency, hhr_prcs_flag, hhr_std_amt, hhr_retro_amt, hhr_quantity, "
                   "hhr_quanty_unit, hhr_ratio, hhr_init_amt, hhr_init_cur, hhr_seg_rule_cd, hhr_round_rule, hhr_formula_id, hhr_accm_type "
                   "from boogoo_payroll.hhr_py_rslt_pin where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
                   "and hhr_seq_num = :b4 and hhr_pin_cd like 'WT_TAX%' and hhr_segment_num = 999 ")
        rs = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num).fetchall()
        for row in rs:
            pin_cd = row['hhr_pin_cd']
            if pin_cd not in self.tax_pin_dic:
                continue
            pin_amt = row['hhr_amt']
            currency = row['hhr_currency']
            prc_flag = row['hhr_prcs_flag']
            std_amt = row['hhr_std_amt']
            retro_amt = row['hhr_retro_amt']
            quantity = row['hhr_quantity']
            quantity_unit = row['hhr_quanty_unit']
            ratio = row['hhr_ratio']
            init_amt = row['hhr_init_amt']
            init_currency = row['hhr_init_cur']
            seg_rule_id = row['hhr_seg_rule_cd']
            round_rule_id = row['hhr_round_rule']
            formula_id = row['hhr_formula_id']
            acc_type = row['hhr_accm_type']

            self.tax_pin_dic[pin_cd] = pin_amt

            pin_obj = self.pins_obj_dic[pin_cd]
            pin_obj.segment['*'].amt_ = pin_amt
            pin_obj.segment['*'].currency = currency
            pin_obj.segment['*'].prcs_flag = prc_flag
            pin_obj.segment['*'].std_amt = std_amt
            pin_obj.segment['*'].retro_amt = retro_amt
            pin_obj.segment['*'].quantity_ = quantity
            pin_obj.segment['*'].quantity_unit = quantity_unit
            pin_obj.segment['*'].ratio = ratio
            pin_obj.segment['*'].init_amt = init_amt
            pin_obj.segment['*'].init_currency = init_currency
            pin_obj.segment['*'].seg_rule_id = seg_rule_id
            pin_obj.segment['*'].round_rule_id = round_rule_id
            pin_obj.segment['*'].formula_id = formula_id
            pin_obj.segment['*'].acc_type = acc_type

    def set_wt_tax_amt(self):
        """获取本月已累计的计税金额、已缴纳的个税、应税金额、免税金额"""

        db = self.db
        catalog = self.catalog
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        # 从累计日历表中取累计类型为M的累计年度、累计序号；
        acc_type = 'M'
        acc_year_seq = get_acc_year_seq(acc_type)
        # 从上一序号对应期间的薪资项目累计表中取值，累计类型、累计年度、累计序号与上述一致的薪资项目累计
        prev_seq = self.catalog.prev_seq
        add_year = acc_year_seq[0]
        add_num = acc_year_seq[1]

        sql = text(
            "select hhr_acc_cd, hhr_amt from boogoo_payroll.hhr_py_rslt_accm where tenant_id = :b1 and hhr_empid = :b2 "
            "and hhr_emp_rcd = :b3 and hhr_seq_num = :b4 and hhr_acc_cd like 'WC_TAX%' and hhr_accm_type = :b5 "
            "and hhr_period_add_year = :b6 and hhr_period_add_number = :b7 ")
        rs = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=prev_seq, b5=acc_type, b6=add_year,
                             b7=add_num).fetchall()
        for row in rs:
            acc_cd = row['hhr_acc_cd']
            if acc_cd not in self.tax_wc_dic:
                continue
            amt = row['hhr_amt']
            self.tax_wc_dic[acc_cd] = amt

    def get_tax_year_amt(self):
        """获取本年（税务年度）已累计的计税金额、个税"""

        db = self.db
        catalog = self.catalog
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        # (历经期)期间编码
        vr_f_period = gv.get_var_value('VR_F_PERIOD')
        # 税务期间对齐
        vr_taxperiod_adjust = gv.get_var_value('VR_TAXPERIOD_ADJUST')

        vr_tax_year = None
        vr_tax_no = None
        """若税务期间对齐='1'（与工资期相同），则直接根据历经期期间编码、年度、序号对应的累计类型为Y的
        累计年度、累计序号作为税务年度（VR_TAX_YEAR）、税务年度序号（VR_TAX_NO）"""
        if vr_taxperiod_adjust == '1':
            acc_year_seq = get_acc_year_seq('Y')
            vr_tax_year = acc_year_seq[0]
            vr_tax_no = acc_year_seq[1]
            gv.set_var_value('VR_TAX_YEAR', vr_tax_year)
            gv.set_var_value('VR_TAX_NO', vr_tax_no)
            # 历经期期间编码、年度、序号对应的累计类型为M的累计序号作为税务月份（VR_TAX_MONTH）
            acc_year_seq = get_acc_year_seq('M')
            vr_tax_month = acc_year_seq[1]
            gv.set_var_value('VR_TAX_MONTH', vr_tax_month)

        """若税务期间对齐='2'（下一个月），则先找到历经期期间编码、年度、序号对应的累计类型为M的
        累计年度(year1)、累计序号(no1)。再找到一个月度累计为下一个月的期间"""
        if vr_taxperiod_adjust == '2':
            acc_year_seq = get_acc_year_seq('M')
            year1 = acc_year_seq[0]
            no1 = acc_year_seq[1]
            if no1 == 12:
                year2 = year1 + 1
                no2 = 1
            else:
                year2 = year1
                no2 = no1 + 1

            # 找一个期间编码为历经期期间编码、累计类型为M、累计年度为year2、累计序号为no2的记录
            period_year, period_num = get_prd_year_seq('M', vr_f_period, year2, no2)[0]

            # 根据找到的期间年度、期间序号、累计类型Y获取累计年度、累计序号作为税务年度（VR_TAX_YEAR）、税务年度序号（VR_TAX_NO）
            vr_tax_year, vr_tax_no = get_acc_year_seq('Y', period_cd=vr_f_period, period_year=period_year,
                                                      period_num=period_num)
            vr_tax_month = no2
            gv.set_var_value('VR_TAX_YEAR', vr_tax_year)
            gv.set_var_value('VR_TAX_NO', vr_tax_no)
            gv.set_var_value('VR_TAX_MONTH', vr_tax_month)

        """判断变量VR_TAX_PRE的值：值为1则不获取前期累计的值 Modified by David on 20190529"""
        vr_tax_pre = gv.get_var_value('VR_TAX_PRE')
        if vr_tax_pre != 1:
            # 从薪资目录的上一序号对应期间的薪资项目累计表中取值，累计类型、累计年度、累计序号分别为T、VR_TAX_YEAR、VR_TAX_NO
            if vr_tax_year and vr_tax_no:
                prev_seq = self.catalog.prev_seq
                acc_type = 'T'
                add_year = vr_tax_year
                add_num = vr_tax_no

                sql = text(
                    "Select hhr_acc_cd, hhr_amt from boogoo_payroll.hhr_py_rslt_accm where tenant_id = :b1 and hhr_empid = :b2 "
                    "and hhr_emp_rcd = :b3  and hhr_seq_num = :b4 and hhr_acc_cd in ('WC_TAXBASE_T', 'WC_TAX_T') "
                    "and hhr_accm_type = :b5 and hhr_period_add_year = :b6 and hhr_period_add_number = :b7 ")
                rs = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=prev_seq, b5=acc_type, b6=add_year,
                                     b7=add_num).fetchall()
                for row in rs:
                    acc_cd = row['hhr_acc_cd']
                    amt = row['hhr_amt']
                    if acc_cd == 'WC_TAXBASE_T':
                        set_pin_seg_amt('WT_TAXBASE_PRE_T', amt, currency=self.pg_currency)
                    elif acc_cd == 'WC_TAX_T':
                        set_pin_seg_amt('WT_TAX_PRE_T', amt, currency=self.pg_currency)

    def get_mon_cum(self):
        """获取累计月数"""

        # 减除费用起算日期
        vr_tax_entrydate = gv.get_var_value('VR_TAX_ENTRYDATE')
        # 减除费用终止日期
        vr_tax_leavedate = gv.get_var_value('VR_TAX_LEAVEDATE')
        # (历经期)期间编码
        vr_f_period = gv.get_var_value('VR_F_PERIOD')
        # 税务年度
        vr_tax_year = gv.get_var_value('VR_TAX_YEAR')
        # 税务月份
        vr_tax_month = gv.get_var_value('VR_TAX_MONTH')
        # 税务期间对齐
        vr_taxperiod_adjust = gv.get_var_value('VR_TAXPERIOD_ADJUST')

        """先根据历经期期间编码、税务年度确定此税务年度的所有期间"""
        prd_date_lst = []
        # 若税务期间对齐='1'（与工资期相同）：则取累计类型M,累计年度、累计序号在（税务年度/1 ~ 税务年度/税务月份）之间的期间。
        if vr_taxperiod_adjust == '1':
            acc_year = vr_tax_year
            for acc_no in range(1, int(vr_tax_month) + 1):
                prd_yr_seq_lst = get_prd_year_seq('M', vr_f_period, acc_year, acc_no)
                for prd_yr_seq in prd_yr_seq_lst:
                    period_year = prd_yr_seq[0]
                    period_num = prd_yr_seq[1]
                    cal_date_lst = get_prd_date_lst(vr_f_period, period_year, period_num)
                    cal_date_lst.append(acc_no)
                    prd_date_lst.append(cal_date_lst)
        # 若税务期间对齐=2（下一个月）：则取累计类型M,累计年度、累计序号在（上一税务年度/12 ~ 税务年度/税务月份-1）之间的期间。
        elif vr_taxperiod_adjust == '2':
            acc_year = vr_tax_year - 1
            acc_no = 12
            prd_yr_seq_lst = get_prd_year_seq('M', vr_f_period, acc_year, acc_no)
            for prd_yr_seq in prd_yr_seq_lst:
                period_year = prd_yr_seq[0]
                period_num = prd_yr_seq[1]
                cal_date_lst = get_prd_date_lst(vr_f_period, period_year, period_num)
                cal_date_lst.append(acc_no)
                prd_date_lst.append(cal_date_lst)

            for acc_no in range(1, int(vr_tax_month)):
                acc_year = vr_tax_year
                prd_yr_seq_lst = get_prd_year_seq('M', vr_f_period, acc_year, acc_no)
                for prd_yr_seq in prd_yr_seq_lst:
                    period_year = prd_yr_seq[0]
                    period_num = prd_yr_seq[1]
                    cal_date_lst = get_prd_date_lst(vr_f_period, period_year, period_num)
                    cal_date_lst.append(acc_no)
                    prd_date_lst.append(cal_date_lst)

        """若减除费用起算日期在这些期间中，则为年度新入职员工。所在期间的累计类型M的累计序号记为no1"""
        """若减除费用终止日期在这些期间中，则为年度离职员工。所在期间的累计类型M的累计序号记为no2"""
        new_entry_flag = False
        ter_emp_flag = False
        no1 = None
        no2 = None
        for date_lst in prd_date_lst:
            start_dt = date_lst[0]
            end_dt = date_lst[1]

            if vr_tax_entrydate is None:
                new_entry_flag = False
            else:
                if start_dt <= vr_tax_entrydate <= end_dt:
                    acc_no = date_lst[4]
                    no1 = acc_no
                    new_entry_flag = True

            if vr_tax_leavedate is None:
                ter_emp_flag = False
            else:
                if start_dt <= vr_tax_leavedate <= end_dt:
                    acc_no = date_lst[4]
                    no2 = acc_no
                    ter_emp_flag = True

        """若税务期间对齐=2（下一个月），更新no1。为12时更新为1，其他更新为no1+1"""
        if new_entry_flag:
            if vr_taxperiod_adjust == '2':
                if no1 == 12:
                    no1 = 1
                else:
                    no1 = no1 + 1
        # 非年度新入职员工，no1=1
        else:
            no1 = 1

        """若税务期间对齐=2（下一个月），更新no2。为12时更新为1，其他更新为no2+1"""
        if ter_emp_flag:
            if vr_taxperiod_adjust == '2':
                if no2 == 12:
                    no2 = 1
                else:
                    no2 = no2 + 1
        # 非年度离职员工，no2=税务月份
        else:
            no2 = vr_tax_month

        # 累计月数 = no2 - no1 + 1
        vr_month_cum = no2 - no1 + 1
        gv.set_var_value('VR_MONTH_CUM', vr_month_cum)

    def get_mon_tax_free_amt(self):
        """获取月度免税金额"""

        # 免税额度存储在分级中，分级编码：BR_TAXFREE
        # 根据员工税类型(VR_TAXTYPE,VR_TAXTYPE在公式FM_TAXTYPE中赋值)从分级中获取免税金额(VR_TAXFREE)
        Br().func_exec('BR_TAXFREE')
        vr_taxfree = gv.get_var_value('VR_TAXFREE')

        # 修改：变量VR_TAXFREE_FLAG的值为Y时，WT_TAXFREE_T累计减除费用 = A * 12
        #      值不为Y时，仍为旧规则：WT_TAXFREE_T累计减除费用 = A * 累计月数（VR_MONTH_CUM）
        vr_taxfree_flag = gv.get_var_value('VR_TAXFREE_FLAG')
        if vr_taxfree_flag == 'Y':
            wt_taxfree_t = vr_taxfree * 12
        else:
            # 累计减除费用 = 月度免税金额 * 累计月数（VR_MONTH_CUM）
            wt_taxfree_t = vr_taxfree * gv.get_var_value('VR_MONTH_CUM')
        set_pin_seg_amt('WT_TAXFREE_T', wt_taxfree_t, currency=self.pg_currency)
        return vr_taxfree

    def get_tax_rate_deduct(self, tax_base, bracket_cd, **kwargs):
        """
        获取税率和速算扣除数
        :param tax_base: 应税金额 VR_TAXBASE_EE/VR_TAXBASE_ER
        :param bracket_cd: 分级编码
        :return: (税率VR_TAXRATE_EE/VR_TAXRATE_ER,速算扣除数VR_TAXDEDUC_EE/VR_TAXDEDUC_ER)
        """

        catalog = self.catalog
        tax_rate = tax_deduct = 0

        tax_type = kwargs.get('tax_type', None)
        # 如果当前是计算公司年终奖税，则需要将税率分级表(分级编码为BR_TAX_EE)根据应税金额降序排列
        if tax_type == '02_ER':
            db = self.db
            tenant_id = catalog.tenant_id
            f_prd_end_dt = catalog.f_prd_end_dt

            data_dic = gv.get_run_var_value('BR_TAX_EE_DESC_DIC')
            if data_dic is None:
                data_dic = {}
            if f_prd_end_dt not in data_dic:
                data_dic[f_prd_end_dt] = []
                sql = text('select hhr_efft_date, hhr_data_key1_dec, hhr_data_val1_dec, hhr_data_val2_dec from '
                           'boogoo_payroll.hhr_py_bracket_data where tenant_id = :b1 and hhr_bracket_cd = :b2 and hhr_efft_date <= :b3 '
                           'order by hhr_efft_date desc, hhr_data_key1_dec desc')
                temp_lst = []
                rs = db.conn.execute(sql, b1=tenant_id, b2=bracket_cd, b3=f_prd_end_dt).fetchall()
                for row in rs:
                    eff_dt = row['hhr_efft_date']
                    if len(temp_lst) == 0:
                        temp_lst.append(eff_dt)
                    if eff_dt not in temp_lst:
                        break
                    else:
                        tax_base_ee = row['hhr_data_key1_dec']
                        tax_rate_ee = row['hhr_data_val1_dec']
                        tax_deduct_ee = row['hhr_data_val2_dec']
                        data_dic[f_prd_end_dt].append((tax_base_ee, tax_rate_ee, tax_deduct_ee))
                gv.set_run_var_value('BR_TAX_EE_DESC_DIC', data_dic)

            # 循环处理税率分级表每一条记录(记为cur_line)，并同时读取下一条记录(记为next_line)
            data_lst = data_dic[f_prd_end_dt]
            data_len = len(data_lst)
            n = 0
            for cur_line in data_lst:
                n += 1
                # 当为最后一条记录时，金额(记为amt_k)= cur_line应税金额
                if n == data_len:
                    amt_k = cur_line[0]
                else:
                    # 当非最后一条记录时，金额(记为amt_k)= cur_line应税金额 * 12 * (1 - next_line税率/100)+ next_line速算扣除数
                    cur_line_base = cur_line[0]
                    next_line = data_lst[n]
                    next_line_rate = next_line[1]
                    next_line_deduct = next_line[2]
                    amt_k = cur_line_base * 12 * (1 - next_line_rate / 100) + next_line_deduct

                # 当总应税金额>=amt_k时，终止循环，得到cur_line的税率、速算扣除数
                if tax_base >= amt_k:
                    tax_rate = cur_line[1]
                    tax_deduct = cur_line[2]
                    break
            return tax_rate, tax_deduct

        if bracket_cd == 'BR_TAX_EE':
            # 个人税率存储在分级中，分级编码：BR_TAX_EE
            # 根据应税金额(VR_TAXBASE_EE)获取税率(VR_TAXRATE_EE)和速算扣除数(VR_TAXDEDUC_EE)
            gv.get_var_obj('VR_TAXBASE_EE').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_TAXRATE_EE')
            tax_deduct = gv.get_var_value('VR_TAXDEDUC_EE')

        elif bracket_cd == 'BR_TAX_EE_Y':
            gv.get_var_obj('VR_TAXBASE_EE').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_TAXRATE_EE')
            tax_deduct = gv.get_var_value('VR_TAXDEDUC_EE')

        elif bracket_cd == 'BR_TAX_ER':
            gv.get_var_obj('VR_TAXBASE_ER').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_TAXRATE_ER')
            tax_deduct = gv.get_var_value('VR_TAXDEDUC_ER')

        elif bracket_cd == 'BR_TAX_ER_Y':
            gv.get_var_obj('VR_TAXBASE_ER').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_TAXRATE_ER')
            tax_deduct = gv.get_var_value('VR_TAXDEDUC_ER')

        elif bracket_cd == 'BR_LABOUR_01':
            gv.get_var_obj('VR_AMT').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_RATE_DEDUCT')
            tax_deduct = gv.get_var_value('VR_AMT_DEDUCT')

        elif bracket_cd == 'BR_LABOUR_02':
            gv.get_var_obj('VR_AMT').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_RATE_DEDUCT')
            tax_deduct = gv.get_var_value('VR_AMT_DEDUCT')

        elif bracket_cd == 'BR_LABOUR_03':
            gv.get_var_obj('VR_AMT').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_RATE_DEDUCT')
            tax_deduct = gv.get_var_value('VR_AMT_DEDUCT')

        elif bracket_cd == 'BR_LABOURTAX_EE':
            gv.get_var_obj('VR_TAXBASE_EE').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_TAXRATE_EE')
            tax_deduct = gv.get_var_value('VR_TAXDEDUC_EE')

        elif bracket_cd == 'BR_LABOURTAX_ER':
            gv.get_var_obj('VR_TAXBASE_ER').value = tax_base
            Br().func_exec(bracket_cd)
            tax_rate = gv.get_var_value('VR_TAXRATE_ER')
            tax_deduct = gv.get_var_value('VR_TAXDEDUC_ER')

        if tax_rate is None:
            tax_rate = 0
        if tax_deduct is None:
            tax_deduct = 0
        return tax_rate, tax_deduct

    def calc_tax01_ee(self):
        """计算工资应税金额(个人)、工资免税金额(个人)、工资税"""

        # 税类型（VR_TAXTYPE）为2（非居民）时，才处理
        vr_taxtype = gv.get_var_value('VR_TAXTYPE')
        if vr_taxtype != '2':
            return

        # <<计算工资应税金额(个人)、工资免税金额(个人)>>
        # 本次计算的工资计税(个人)(记为c)
        tax01_c = gv.get_pin('WT_TAXBASE01_EE').segment['*'].amt

        if tax01_c <= 0:
            return

        # 本月已累计的计税金额(2项总和记为b)
        tax01_b = self.tax_wc_dic['WC_TAXBASE01_EE_M'] + self.tax_wc_dic['WC_TAXBASE01_ER_B_M']

        # 当b>=a时，工资免税金额(个人) =0，工资应税金额(个人) =c
        if tax01_b >= self.mon_tax_free_a:
            wt_taxfree01_ee = 0
            wt_taxbase01_act_ee = tax01_c
        else:
            # 当b<a时：
            #    当c<=(a-b)时，工资免税金额(个人) =c，工资应税金额(个人)=0
            if tax01_c <= self.mon_tax_free_a - tax01_b:
                wt_taxfree01_ee = tax01_c
                wt_taxbase01_act_ee = 0
            else:
                # 当c>(a-b)时，工资免税金额(个人) =a-b，工资应税金额(个人)=c-(a-b)
                wt_taxfree01_ee = self.mon_tax_free_a - tax01_b
                wt_taxbase01_act_ee = tax01_c - (self.mon_tax_free_a - tax01_b)

        set_pin_seg_amt('WT_TAXFREE01_EE', wt_taxfree01_ee, currency=self.pg_currency)
        set_pin_seg_amt('WT_TAXBASE01_ACT_EE', wt_taxbase01_act_ee, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE01_EE'] = wt_taxfree01_ee
        self.tax_pin_dic['WT_TAXBASE01_ACT_EE'] = wt_taxbase01_act_ee

        # <<计算工资税>>

        # 本月已累计的应税金额：工资应税金额(个人)月度累计 + 工资应税金额(公司反算)月度累计
        tax01_d1 = self.tax_wc_dic['WC_TAXBASE01_ACT_EE_M'] + self.tax_wc_dic['WC_TAXBASE01_ACT_ER_B_M']
        # 本次计算的应税金额：工资应税金额(个人)
        tax01_d2 = wt_taxbase01_act_ee

        # 以上述3项之和(记为d)找到对应的税率和速算扣除数, 总工资税 = d * 税率/100 - 速算扣除数
        tax01_d = tax01_d1 + tax01_d2
        tax_rate, tax_deduct = self.get_tax_rate_deduct(tax01_d, 'BR_TAX_EE')
        # 将税率和速算扣除数分别赋值变量VR_TAXRATE01_EE、VR_TAXDEDUC01_EE
        gv.get_var_obj('VR_TAXRATE01_EE').value = tax_rate
        gv.get_var_obj('VR_TAXDEDUC01_EE').value = tax_deduct

        tot_tax01 = tax01_d * tax_rate / 100 - tax_deduct
        # 本次计算的工资税为总工资税减去已扣除的工资税(个人/公司)：工资税 = 总工资税 - 工资税月度累计 - 工资税(公司)月度累计
        wt_tax01_ee = tot_tax01 - self.tax_wc_dic['WC_TAX01_EE_M'] - self.tax_wc_dic['WC_TAX01_ER_M']
        if wt_tax01_ee < 0:
            wt_tax01_ee = 0
        set_pin_seg_amt('WT_TAX01_EE', wt_tax01_ee, currency=self.pg_currency, ratio=tax_rate)
        self.tax_pin_dic['WT_TAX01_EE'] = wt_tax01_ee

    def calc_year_tax01_ee(self):
        """按年度计算个税（个人）"""

        # 税类型（VR_TAXTYPE）为1（居民）时，才处理
        vr_taxtype = gv.get_var_value('VR_TAXTYPE')
        if vr_taxtype != '1':
            return

        # 前期已累计的计税金额（记为t1）
        t1 = gv.get_pin('WT_TAXBASE_PRE_T').segment['*'].amt
        # 前期已累计的扣缴个税（记为t5）
        t5 = gv.get_pin('WT_TAX_PRE_T').segment['*'].amt
        # 本次计算的计税金额（个人）（记为t2）
        t2 = gv.get_pin('WT_TAXBASE_EE').segment['*'].amt
        # 本次计算的减除费用（记为t3）、专项附加扣除（记为t4）
        t3 = gv.get_pin('WT_TAXFREE_T').segment['*'].amt
        t4 = gv.get_pin_acc('WC_SPADEDUCT_CUM').segment['*'].amt

        if t2 > 0:
            # 应纳税所得额(个人)（WT_TAXBASE_ACT_EE_T）= t1 + t2 - t3 - t4
            wt_taxbase_act_ee_t = t1 + t2 - t3 - t4
            if wt_taxbase_act_ee_t < 0:
                wt_taxbase_act_ee_t = 0
            set_pin_seg_amt('WT_TAXBASE_ACT_EE_T', wt_taxbase_act_ee_t, currency=self.pg_currency)
            self.tax_pin_dic['WT_TAXBASE_ACT_EE_T'] = wt_taxbase_act_ee_t
            # 以应纳税所得额(个人)( >0时 )找到对应的税率和速算扣除数（分级BR_TAX_EE_Y）
            tax_rate, tax_deduct = self.get_tax_rate_deduct(wt_taxbase_act_ee_t, 'BR_TAX_EE_Y')
            # 应纳税额(个人)（WT_TAX_EE_T） = 应纳税所得额(个人) * 税率/100 - 速算扣除数
            wt_tax_ee_t = wt_taxbase_act_ee_t * tax_rate / 100 - tax_deduct
            set_pin_seg_amt('WT_TAX_EE_T', wt_tax_ee_t, currency=self.pg_currency)
            self.tax_pin_dic['WT_TAX_EE_T'] = wt_tax_ee_t
            # 个税（个人）（WT_TAX_EE） = 应纳税额(个人) – t5, 若<0,则重置为0
            wt_tax_ee = wt_tax_ee_t - t5
            if wt_tax_ee < 0:
                wt_tax_ee = 0
            set_pin_seg_amt('WT_TAX_EE', wt_tax_ee, currency=self.pg_currency, ratio=tax_rate)
            self.tax_pin_dic['WT_TAX_EE'] = wt_tax_ee

            gv.set_var_value('VR_TAXRATE_EE_T', tax_rate)
            gv.set_var_value('VR_TAXDEDUC_EE_T', tax_deduct)

    def calc_tax01_com(self):
        """计算工资税(公司)、工资计税(公司反算)、工资免税金额(公司)、工资应税金额(公司)、工资应税金额(公司反算)"""

        # 税类型（VR_TAXTYPE）为2（非居民）时，才处理
        vr_taxtype = gv.get_var_value('VR_TAXTYPE')
        if vr_taxtype != '2':
            return

        # 总税后金额 = 工资计税(个人)月度累计 + 工资计税(公司)月度累计 + 工资计税(个人) + 工资计税(公司) -工资税月度累计 - 工资税
        wt_taxbase01_er = gv.get_pin('WT_TAXBASE01_ER').segment['*'].amt
        if wt_taxbase01_er <= 0:
            return
        wc_taxbase01_ee_m = self.tax_wc_dic['WC_TAXBASE01_EE_M']
        wc_taxbase01_er_m = self.tax_wc_dic['WC_TAXBASE01_ER_M']
        wt_taxbase01_ee = gv.get_pin('WT_TAXBASE01_EE').segment['*'].amt

        wc_tax01_ee_m = self.tax_wc_dic['WC_TAX01_EE_M']
        wt_tax01_ee = self.tax_pin_dic['WT_TAX01_EE']
        tot_after_tax_amt = wc_taxbase01_ee_m + wc_taxbase01_er_m + wt_taxbase01_ee + wt_taxbase01_er - wc_tax01_ee_m - wt_tax01_ee

        # 根据[总税后金额]反算[总税前金额]、[总工资免税金额]

        # 本月已累计的年终奖免税金额(2项总和记为e)：
        #  WC_TAXFREE02_EE_M	年终奖免税金额(个人)月度累计
        #  WC_TAXFREE02_ER_M	年终奖免税金额(公司)月度累计
        # tax02_e = self.tax_wc_dic['WC_TAXFREE02_EE_M'] + self.tax_wc_dic['WC_TAXFREE02_ER_M']
        #
        # # 计算总工资免税金额(记为f)： 当e>=a,f=0; 当e<a,f=a-e
        # if tax02_e >= self.mon_tax_free_a:
        #     tot_tax01_free_f = 0
        # else:
        #     tot_tax01_free_f = self.mon_tax_free_a - tax02_e

        # 计算总工资免税金额(记为f)：f=a
        tot_tax01_free_f = self.mon_tax_free_a

        # 当总税后金额<=f，总税前金额 = 总税后金额，更新f(总工资免税金额)=总税后金额
        tax_rate = 0
        if tot_after_tax_amt <= tot_tax01_free_f:
            tot_pre_tax_amt = tot_after_tax_amt
            tot_tax01_free_f = tot_after_tax_amt
        else:
            # 总应税金额 = 总税后金额 – f
            tot_payable_tax_amt = tot_after_tax_amt - tot_tax01_free_f
            # 根据总应税金额找到对应的税率和速算扣除数(根据公司税率分级)
            tax_rate, tax_deduct = self.get_tax_rate_deduct(tot_payable_tax_amt, 'BR_TAX_ER')
            gv.get_var_obj('VR_TAXRATE01_ER').value = tax_rate
            gv.get_var_obj('VR_TAXDEDUC01_ER').value = tax_deduct

            # 总税前金额 = ( 总应税金额 – 速算扣除数 ) / ( 1 – 税率/100 ) + f
            tot_pre_tax_amt = (tot_payable_tax_amt - tax_deduct) / (1 - tax_rate / 100) + tot_tax01_free_f

        # 工资税(公司)= 总税前金额 - 工资计税(个人)月度累计 - 工资计税(公司)月度累计 - 工资计税(个人) - 工资计税(公司) - 工资税(公司)月度累计
        wt_tax01_er = tot_pre_tax_amt - wc_taxbase01_ee_m - wc_taxbase01_er_m - wt_taxbase01_ee - wt_taxbase01_er - \
                      self.tax_wc_dic['WC_TAX01_ER_M']
        if wt_tax01_er < 0:
            wt_tax01_er = 0
        if tax_rate != 0:
            set_pin_seg_amt('WT_TAX01_ER', wt_tax01_er, currency=self.pg_currency, ratio=tax_rate)
        else:
            set_pin_seg_amt('WT_TAX01_ER', wt_tax01_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAX01_ER'] = wt_tax01_er

        # 工资计税(公司反算)= 工资计税(公司) + 工资税(公司)
        wt_taxbase01_er_b = wt_taxbase01_er + wt_tax01_er
        set_pin_seg_amt('WT_TAXBASE01_ER_B', wt_taxbase01_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE01_ER_B'] = wt_taxbase01_er_b

        # 工资免税金额(公司) = 总工资免税金额 – ( 工资免税金额(个人)月度累计 + 工资免税金额(公司)月度累计 + 工资免税金额(个人) )
        wt_taxfree01_er = tot_tax01_free_f - (
                    self.tax_wc_dic['WC_TAXFREE01_EE_M'] + self.tax_wc_dic['WC_TAXFREE01_ER_M'] + self.tax_pin_dic[
                'WT_TAXFREE01_EE'])
        set_pin_seg_amt('WT_TAXFREE01_ER', wt_taxfree01_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE01_ER'] = wt_taxfree01_er

        # 工资应税金额(公司)  = 工资计税(公司) - 工资免税金额(公司)
        wt_taxbase01_act_er = wt_taxbase01_er - wt_taxfree01_er
        set_pin_seg_amt('WT_TAXBASE01_ACT_ER', wt_taxbase01_act_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE01_ACT_ER'] = wt_taxbase01_act_er

        # 工资应税金额(公司反算) = 工资计税(公司反算) - 工资免税金额(公司)
        wt_taxbase01_act_er_b = wt_taxbase01_er_b - wt_taxfree01_er
        set_pin_seg_amt('WT_TAXBASE01_ACT_ER_B', wt_taxbase01_act_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE01_ACT_ER_B'] = wt_taxbase01_act_er_b

    def calc_year_tax01_com(self):
        """按年度计算个税（公司）"""

        # 税类型（VR_TAXTYPE）为1（居民）时，才处理
        vr_taxtype = gv.get_var_value('VR_TAXTYPE')
        if vr_taxtype != '1':
            return

        # 前期已累计的计税金额（记为t1）
        t1 = gv.get_pin('WT_TAXBASE_PRE_T').segment['*'].amt
        # 前期已累计的扣缴个税（记为t5）
        t5 = gv.get_pin('WT_TAX_PRE_T').segment['*'].amt
        # 本次计算的计税金额（个人）（记为t2）
        t2 = gv.get_pin('WT_TAXBASE_EE').segment['*'].amt
        # 本次计算的减除费用（记为t3）、专项附加扣除（记为t4）
        t3 = gv.get_pin('WT_TAXFREE_T').segment['*'].amt
        t4 = gv.get_pin_acc('WC_SPADEDUCT_CUM').segment['*'].amt
        # 本次计算的计税金额（公司）（记为t6）
        t6 = gv.get_pin('WT_TAXBASE_ER').segment['*'].amt
        # 本次计算的个税（个人）（记为t7）
        t7 = gv.get_pin('WT_TAX_EE').segment['*'].amt

        if t6 > 0:
            # 总税后金额 = t1 + t2 + t6 - t5 - t7
            tot_af_tax_amt = t1 + t2 + t6 - t5 - t7

            """根据总税后金额反算总税前金额"""

            tax_rate = 0
            # 总工资免税金额（记为t8） = t3 + t4
            t8 = t3 + t4
            # 当总税后金额<= t8，总税前金额 = 总税后金额
            if tot_af_tax_amt <= t8:
                tot_pre_tax_amt = tot_af_tax_amt
            else:
                # 总应税金额 = 总税后金额 – t8
                tot_act_tax_amt = tot_af_tax_amt - t8
                # 根据总应税金额找到对应的税率和速算扣除数（根据公司税率分级BR_TAX_ER_Y）
                tax_rate, tax_deduct = self.get_tax_rate_deduct(tot_act_tax_amt, 'BR_TAX_ER_Y')
                # 应纳税所得额(公司)（WT_TAXBASE_ACT_ER_T）=  ( 总应税金额 – 速算扣除数 ) / ( 1 – 税率/100 )
                wt_taxbase_act_er_t = (tot_act_tax_amt - tax_deduct) / (1 - tax_rate / 100)
                set_pin_seg_amt('WT_TAXBASE_ACT_ER_T', wt_taxbase_act_er_t, currency=self.pg_currency)
                self.tax_pin_dic['WT_TAXBASE_ACT_ER_T'] = wt_taxbase_act_er_t
                # 总税前金额 =  应纳税所得额(公司) + t8
                tot_pre_tax_amt = wt_taxbase_act_er_t + t8
                # 应纳税额(公司)（WT_TAX_ER_T） = 总税前金额 – 总税后金额
                wt_tax_er_t = tot_pre_tax_amt - tot_af_tax_amt
                set_pin_seg_amt('WT_TAX_ER_T', wt_tax_er_t, currency=self.pg_currency)
                self.tax_pin_dic['WT_TAX_ER_T'] = wt_tax_er_t

                gv.set_var_value('VR_TAXRATE_ER_T', tax_rate)
                gv.set_var_value('VR_TAXDEDUC_ER_T', tax_deduct)

            # 个税（公司）（WT_TAX_ER） = 总税前金额 - t1 - t2 – t6
            wt_tax_er = tot_pre_tax_amt - t1 - t2 - t6
            if wt_tax_er < 0:
                wt_tax_er = 0
            set_pin_seg_amt('WT_TAX_ER', wt_tax_er, currency=self.pg_currency, ratio=tax_rate)
            self.tax_pin_dic['WT_TAX_ER'] = wt_tax_er
        else:
            wt_tax_er = 0

        # 计税金额（公司反算）（WT_TAXBASE_ER_B） = 计税金额(公司) + 个税（公司）
        wt_taxbase_er_b = t6 + wt_tax_er
        set_pin_seg_amt('WT_TAXBASE_ER_B', wt_taxbase_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE_ER_B'] = wt_taxbase_er_b

    def calc_tax02_ee(self):
        """计算年终奖应税金额(个人)、年终奖免税金额(个人)、年终奖税"""

        # 本次计算的年终奖计税(个人)(记为h)
        tax02_h = gv.get_pin('WT_TAXBASE02_EE').segment['*'].amt
        if tax02_h <= 0:
            return
        # # 工资计税(个人)月度累计
        # wc_taxbase01_ee_m = self.tax_wc_dic['WC_TAXBASE01_EE_M']
        # # 工资计税(公司反算)月度累计
        # wc_taxbase01_er_b_m = self.tax_wc_dic['WC_TAXBASE01_ER_B_M']
        # # 年终奖计税(个人)月度累计
        # wc_taxbase02_ee_m = self.tax_wc_dic['WC_TAXBASE02_EE_M']
        # # 年终奖计税(公司反算)月度累计
        # wc_taxbase02_er_b_m = self.tax_wc_dic['WC_TAXBASE02_ER_B_M']
        # # 工资计税(个人)
        # wt_taxbase01_ee = gv.get_pin('WT_TAXBASE01_EE').segment['*'].amt
        # # 工资计税(公司反算)
        # wt_taxbase01_er_b = self.tax_pin_dic['WT_TAXBASE01_ER_B']

        """ 
        2019个税调整：年终奖不再考虑免税额
        年终奖免税金额(个人)=0，年终奖应税金额(个人)=h 
        """
        # 上述6项之和(记为g)
        # tax02_g = wc_taxbase01_ee_m + wc_taxbase01_er_b_m + wc_taxbase02_ee_m + wc_taxbase02_er_b_m + wt_taxbase01_ee + wt_taxbase01_er_b

        # 当g>=a时，年终奖免税金额(个人)=0，年终奖应税金额(个人)=h
        # if tax02_g >= self.mon_tax_free_a:
        #     set_pin_seg_amt('WT_TAXFREE02_EE', 0)
        #     self.tax_pin_dic['WT_TAXFREE02_EE'] = 0
        #     set_pin_seg_amt('WT_TAXBASE02_ACT_EE', tax02_h)
        #     self.tax_pin_dic['WT_TAXBASE02_ACT_EE'] = tax02_h
        # else:
        #     # 当h<=(a-g)时，年终奖免税金额(个人)=h，年终奖应税金额(个人)=0
        #     if tax02_h <= (self.mon_tax_free_a - tax02_g):
        #         set_pin_seg_amt('WT_TAXFREE02_EE', tax02_h)
        #         self.tax_pin_dic['WT_TAXFREE02_EE'] = tax02_h
        #         set_pin_seg_amt('WT_TAXBASE02_ACT_EE', 0)
        #         self.tax_pin_dic['WT_TAXBASE02_ACT_EE'] = 0
        #
        #     # 当h>(a-g)时，年终奖免税金额(个人)=a-g，年终奖应税金额(个人)=h-(a-g)
        #     wt_taxfree02_ee = self.mon_tax_free_a - tax02_g
        #     wt_taxbase02_act_ee = tax02_h - wt_taxfree02_ee
        #     if wt_taxbase02_act_ee > 0:
        #         set_pin_seg_amt('WT_TAXFREE02_EE', wt_taxfree02_ee)
        #         self.tax_pin_dic['WT_TAXFREE02_EE'] = wt_taxfree02_ee
        #         set_pin_seg_amt('WT_TAXBASE02_ACT_EE', wt_taxbase02_act_ee)
        #         self.tax_pin_dic['WT_TAXBASE02_ACT_EE'] = wt_taxbase02_act_ee
        set_pin_seg_amt('WT_TAXFREE02_EE', 0, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE02_EE'] = 0
        set_pin_seg_amt('WT_TAXBASE02_ACT_EE', tax02_h, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE02_ACT_EE'] = tax02_h

        # 年终奖应税金额(个人)月度累计
        wc_taxbase02_act_ee_m = self.tax_wc_dic['WC_TAXBASE02_ACT_EE_M']
        # 年终奖应税金额(公司反算)月度累计
        wc_taxbase02_act_er_b_m = self.tax_wc_dic['WC_TAXBASE02_ACT_ER_B_M']
        # 年终奖应税金额(个人)
        wt_taxbase02_act_ee = self.tax_pin_dic['WT_TAXBASE02_ACT_EE']
        # 以上述3项之和(记为I)
        tax02_i = wc_taxbase02_act_ee_m + wc_taxbase02_act_er_b_m + wt_taxbase02_act_ee

        if tax02_i:
            tax_rate, tax_deduct = self.get_tax_rate_deduct(tax02_i / 12, 'BR_TAX_EE')
        else:
            tax_rate, tax_deduct = 0, 0
        gv.get_var_obj('VR_TAXRATE02_EE').value = tax_rate
        gv.get_var_obj('VR_TAXDEDUC02_EE').value = tax_deduct

        # 总年终奖税 = i * 税率/100 - 速算扣除数
        tot_tax02_amt = tax02_i * tax_rate / 100 - tax_deduct
        # 年终奖税 = 总年终奖税 – 年终奖税月度累计 – 年终奖税(公司)月度累计
        wt_tax02_ee = tot_tax02_amt - self.tax_wc_dic['WC_TAX02_EE_M'] - self.tax_wc_dic['WC_TAX02_ER_M']
        set_pin_seg_amt('WT_TAX02_EE', wt_tax02_ee, currency=self.pg_currency, ratio=tax_rate)
        self.tax_pin_dic['WT_TAX02_EE'] = Decimal(str(wt_tax02_ee)).quantize(Decimal('0.0000'))

    def calc_tax02_com(self):
        """计算年终奖税(公司)、年终奖计税(公司反算)、年终奖免税金额(公司)、年终奖应税金额(公司)、年终奖应税金额(公司反算)"""

        # 年终奖计税(公司)
        wt_taxbase02_er = gv.get_pin('WT_TAXBASE02_ER').segment['*'].amt
        if wt_taxbase02_er <= 0:
            return
        # 年终奖计税(个人)月度累计
        wc_taxbase02_ee_m = self.tax_wc_dic['WC_TAXBASE02_EE_M']
        # 年终奖计税(公司)月度累计
        wc_taxbase02_er_m = self.tax_wc_dic['WC_TAXBASE02_ER_M']
        # 年终奖计税(个人)
        wt_taxbase02_ee = gv.get_pin('WT_TAXBASE02_EE').segment['*'].amt

        # 年终奖税月度累计
        wc_tax02_ee_m = self.tax_wc_dic['WC_TAX02_EE_M']
        # 年终奖税
        wt_tax02_ee = self.tax_pin_dic['WT_TAX02_EE']
        # 总税后金额 = 年终奖计税(个人)月度累计 + 年终奖计税(公司)月度累计 + 年终奖计税(个人) + 年终奖计税(公司) - 年终奖税月度累计 - 年终奖税
        tot_after_tax_amt = wc_taxbase02_ee_m + wc_taxbase02_er_m + wt_taxbase02_ee + wt_taxbase02_er - wc_tax02_ee_m - wt_tax02_ee

        # 根据[总税后金额]反算[总税前金额]、[总年终奖免税金额]

        # 工资免税金额(个人)月度累计
        # wc_taxfree01_ee_m = self.tax_wc_dic['WC_TAXFREE01_EE_M']
        # # 工资免税金额(公司)月度累计
        # wc_taxfree01_er_m = self.tax_wc_dic['WC_TAXFREE01_ER_M']
        # # 工资免税金额(个人)
        # wt_taxfree01_ee = self.tax_pin_dic['WT_TAXFREE01_EE']
        # # 工资免税金额(公司)
        # wt_taxfree01_er = self.tax_pin_dic['WT_TAXFREE01_ER']

        # 上述4项之和记为j
        # tax02_j = wc_taxfree01_ee_m + wc_taxfree01_er_m + wt_taxfree01_ee + wt_taxfree01_er

        """ 
        2019个税调整：年终奖不再考虑免税额 
        """
        # 计算总年终奖免税金额(记为k)
        # 当j>=a,k=0; 当j<a,k=a-j
        # if tax02_j >= self.mon_tax_free_a:
        #     tot_tax02_free_k = 0
        # else:
        #     tot_tax02_free_k = self.mon_tax_free_a - tax02_j
        tot_tax02_free_k = 0

        # 当总税后金额<=k，总税前金额 = 总税后金额，更新k(总年终奖免税金额)=总税后金额
        tax_rate = 0
        if tot_after_tax_amt <= tot_tax02_free_k:
            tot_pre_tax_amt = tot_after_tax_amt
            tot_tax02_free_k = tot_after_tax_amt
        else:
            # 总应税金额 = 总税后金额 – k
            tot_payable_tax_amt = tot_after_tax_amt - tot_tax02_free_k
            # 根据总应税金额获取税率、速算扣除数(非分级的取数方式)：
            tax_rate, tax_deduct = self.get_tax_rate_deduct(tot_payable_tax_amt, 'BR_TAX_EE', tax_type='02_ER')
            gv.get_var_obj('VR_TAXRATE02_ER').value = tax_rate
            gv.get_var_obj('VR_TAXDEDUC02_ER').value = tax_deduct

            # 总税前金额 = ( 总应税金额 – 速算扣除数 ) / ( 1 – 税率/100 ) + k
            tot_pre_tax_amt = (tot_payable_tax_amt - tax_deduct) / (1 - tax_rate / 100) + tot_tax02_free_k

        # 年终奖税(公司)= 总税前金额 - 年终奖计税(个人)月度累计 - 年终奖计税(公司)月度累计 - 年终奖计税(个人) - 年终奖计税(公司) - 年终奖税(公司)月度累计
        wt_tax02_er = tot_pre_tax_amt - wc_taxbase02_ee_m - wc_taxbase02_er_m - wt_taxbase02_ee - wt_taxbase02_er - \
                      self.tax_wc_dic['WC_TAX02_ER_M']
        if tax_rate != 0:
            set_pin_seg_amt('WT_TAX02_ER', wt_tax02_er, currency=self.pg_currency, ratio=tax_rate)
        else:
            set_pin_seg_amt('WT_TAX02_ER', wt_tax02_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAX02_ER'] = wt_tax02_er

        # 年终奖计税(公司反算)= 年终奖计税(公司) + 年终奖税(公司)
        wt_taxbase02_er_b = wt_taxbase02_er + wt_tax02_er
        set_pin_seg_amt('WT_TAXBASE02_ER_B', wt_taxbase02_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE02_ER_B'] = wt_taxbase02_er_b

        # 年终奖免税金额(公司) = 总年终奖免税金额 – (年终奖免税金额(个人)月度累计 + 年终奖免税金额(公司)月度累计 + 年终奖免税金额(个人))
        wt_taxfree02_er = tot_tax02_free_k - (
                    self.tax_wc_dic['WC_TAXFREE02_EE_M'] + self.tax_wc_dic['WC_TAXFREE02_ER_M'] + self.tax_pin_dic[
                'WT_TAXFREE02_EE'])
        set_pin_seg_amt('WT_TAXFREE02_ER', wt_taxfree02_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE02_ER'] = wt_taxfree02_er

        # 年终奖应税金额(公司) = 年终奖计税(公司) - 年终奖免税金额(公司)
        wt_taxbase02_act_er = wt_taxbase02_er - wt_taxfree02_er
        set_pin_seg_amt('WT_TAXBASE02_ACT_ER', wt_taxbase02_act_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE02_ACT_ER'] = wt_taxbase02_act_er

        # 年终奖应税金额(公司反算) = 年终奖计税(公司反算) - 年终奖免税金额(公司)
        wt_taxbase02_act_er_b = wt_taxbase02_er_b - wt_taxfree02_er
        set_pin_seg_amt('WT_TAXBASE02_ACT_ER_B', wt_taxbase02_act_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE02_ACT_ER_B'] = wt_taxbase02_act_er_b

    def calc_tax03_ee(self):
        """计算离职补偿金应税金额(个人)、离职补偿金免税金额(个人)、离职补偿金税"""

        # 离职补偿金计税(个人)
        wt_taxbase03_ee_p = gv.get_pin('WT_TAXBASE03_EE').segment['*'].amt
        if wt_taxbase03_ee_p <= 0:
            return
        # 离职补偿金计税(个人)月度累计
        wc_taxbase03_ee_m1 = self.tax_wc_dic['WC_TAXBASE03_EE_M']
        # 离职补偿金计税(公司反算)月度累计
        wc_taxbase03_er_b_m1 = self.tax_wc_dic['WC_TAXBASE03_ER_B_M']
        # 上述两项之和记为m
        tax03_m = wc_taxbase03_ee_m1 + wc_taxbase03_er_b_m1

        # 当m>=q时，离职补偿金免税金额(个人)=0，离职补偿金应税金额(个人)=p
        if tax03_m >= self.tot_tax_free_q:
            wt_taxfree03_ee = 0
            wt_taxbase03_act_ee = wt_taxbase03_ee_p
        else:
            # 当p<=(q-m)时，离职补偿金免税金额(个人)=p，离职补偿金应税金额(个人)=0
            q_m = self.tot_tax_free_q - tax03_m
            if wt_taxbase03_ee_p <= q_m:
                wt_taxfree03_ee = wt_taxbase03_ee_p
                wt_taxbase03_act_ee = 0
            else:
                # 当p>(q-m)时，离职补偿金免税金额(个人)=q-m，离职补偿金应税金额(个人)=p-(q-m)
                wt_taxfree03_ee = q_m
                wt_taxbase03_act_ee = wt_taxbase03_ee_p - q_m
        set_pin_seg_amt('WT_TAXFREE03_EE', wt_taxfree03_ee, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE03_EE'] = wt_taxfree03_ee
        set_pin_seg_amt('WT_TAXBASE03_ACT_EE', wt_taxbase03_act_ee, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE03_ACT_EE'] = wt_taxbase03_act_ee

        # 离职补偿金应税金额(个人)月度累计
        wc_taxbase03_act_ee_m = self.tax_wc_dic['WC_TAXBASE03_ACT_EE_M']
        # 离职补偿金应税金额(公司反算)月度累计
        wc_taxbase03_act_er_b_m = self.tax_wc_dic['WC_TAXBASE03_ACT_ER_B_M']

        """
        2019个税调整：离职补偿直接使用年税率，不再考虑工龄
        以上述3项之和（记为T）找到对应的税率和速算扣除数（新年度税率分级：BR_TAX_EE_Y）：
        总离职补偿金税 =  T * 税率/100 - 速算扣除数
        """
        amt_t = (wc_taxbase03_act_ee_m + wc_taxbase03_act_er_b_m + wt_taxbase03_act_ee)
        tax_rate, tax_deduct = self.get_tax_rate_deduct(amt_t, 'BR_TAX_EE_Y')
        gv.get_var_obj('VR_TAXRATE03_EE').value = tax_rate
        gv.get_var_obj('VR_TAXDEDUC03_EE').value = tax_deduct
        tot_tax03_amt = amt_t * tax_rate / 100 - tax_deduct

        # 司龄>12时年限取12，司龄<1时年限取1，否则年限取司龄
        # com_age = gv.get_var_value('VR_COMPYEAR')
        #
        # if com_age > 12:
        #     age = 12
        # elif com_age < 1:
        #     age = 1
        # else:
        #     age = com_age
        # # 月平均值(记为r)
        # mon_avg_r = (wc_taxbase03_act_ee_m + wc_taxbase03_act_er_b_m + wt_taxbase03_act_ee) / age
        # tax_rate = 0
        # # 当r<=a时，总离职补偿金税=0
        # if mon_avg_r <= self.mon_tax_free_a:
        #     tot_tax03_amt = 0
        # else:
        #     # 当r>a时，以(r-a)找到对应的税率和速算扣除数：总离职补偿金税 = ( (r-a) * 税率/100 - 速算扣除数 ) * 年限
        #     r_a = mon_avg_r - self.mon_tax_free_a
        #     tax_rate, tax_deduct = self.get_tax_rate_deduct(r_a, 'BR_TAX_EE')
        #     gv.get_var_obj('VR_TAXRATE03_EE').value = tax_rate
        #     gv.get_var_obj('VR_TAXDEDUC03_EE').value = tax_deduct
        #
        #     tot_tax03_amt = (r_a * tax_rate / 100 - tax_deduct) * age

        # 离职补偿金税 = 总离职补偿金税 - 离职补偿金税月度累计 - 离职补偿金税(公司)月度累计
        wt_tax03_ee = tot_tax03_amt - self.tax_wc_dic['WC_TAX03_EE_M'] - self.tax_wc_dic['WC_TAX03_ER_M']

        if tax_rate != 0:
            set_pin_seg_amt('WT_TAX03_EE', wt_tax03_ee, currency=self.pg_currency, ratio=tax_rate)
        else:
            set_pin_seg_amt('WT_TAX03_EE', wt_tax03_ee, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAX03_EE'] = wt_tax03_ee

    def calc_tax03_com(self):
        """计算离职补偿金税(公司)、离职补偿金计税(公司反算)、离职补偿金免税金额(公司)、离职补偿金应税金额(公司)、离职补偿金应税金额(公司反算)"""

        # 离职补偿金计税(公司)
        wt_taxbase03_er = gv.get_pin('WT_TAXBASE03_ER').segment['*'].amt
        if wt_taxbase03_er <= 0:
            return
        # 离职补偿金计税(个人)月度累计
        wc_taxbase03_ee_m = self.tax_wc_dic['WC_TAXBASE03_EE_M']
        # 离职补偿金计税(公司)月度累计
        wc_taxbase03_er_m = self.tax_wc_dic['WC_TAXBASE03_ER_M']
        # 离职补偿金计税(个人)
        wt_taxbase03_ee_p = gv.get_pin('WT_TAXBASE03_EE').segment['*'].amt

        # 离职补偿金税月度累计
        wc_tax03_ee_m = self.tax_wc_dic['WC_TAX03_EE_M']
        # 离职补偿金税(公司)月度累计
        wc_tax03_er_m = self.tax_wc_dic['WC_TAX03_ER_M']

        # 总税后金额 = 离职补偿金计税(个人)月度累计 + 离职补偿金计税(公司)月度累计 + 离职补偿金计税(个人) + 离职补偿金计税(公司) - 离职补偿金税月度累计 - 离职补偿金税
        tot_after_tax_amt = wc_taxbase03_ee_m + wc_taxbase03_er_m + wt_taxbase03_ee_p + wt_taxbase03_er - wc_tax03_ee_m - \
                            self.tax_pin_dic['WT_TAX03_EE']

        tax_rate = 0
        # 根据总税后金额反算总税前金额、总离职补偿金免税金额：
        # 当总税后金额<=q，总税前金额 = 总税后金额，总离职补偿金免税金额=总税后金额

        if tot_after_tax_amt <= self.tot_tax_free_q:
            tot_pre_tax_amt = tot_after_tax_amt
            tot_tax03_free_amt = tot_after_tax_amt
        else:
            # 当总税后金额>q, 总离职补偿金免税金额=q，总应税金额 = 总税后金额 – q
            tot_tax03_free_amt = self.tot_tax_free_q
            tot_payable_tax_amt = tot_after_tax_amt - self.tot_tax_free_q

            """
            2019个税调整：离职补偿直接使用年税率，不再考虑工龄
            根据总应税金额找到对应的税率和速算扣除数（新年度税率分级：BR_TAX_ER_Y）
            总税前金额 = ( 总应税金额 – 速算扣除数 ) / ( 1 – 税率/100 ) + Q
            """
            tax_rate, tax_deduct = self.get_tax_rate_deduct(tot_payable_tax_amt, 'BR_TAX_ER_Y')
            gv.get_var_obj('VR_TAXRATE03_ER').value = tax_rate
            gv.get_var_obj('VR_TAXDEDUC03_ER').value = tax_deduct
            tot_pre_tax_amt = (tot_payable_tax_amt - tax_deduct) / (1 - tax_rate / 100) + self.tot_tax_free_q

            # 司龄>12时年限取12，司龄<1时年限取1，否则年限取司龄
            # com_age = gv.get_var_value('VR_COMPYEAR')
            # if com_age > 12:
            #     age = 12
            # elif com_age < 1:
            #     age = 1
            # else:
            #     age = com_age
            #
            # # 总应税金额/年限=月平均值(记为s)
            # mon_avg_s = tot_payable_tax_amt / age
            #
            # # 当s<=a时，总税前金额 = 总税后金额
            # if mon_avg_s <= self.mon_tax_free_a:
            #     tot_pre_tax_amt = tot_after_tax_amt
            # else:
            #     # 当s>a时，月应税金额 = s – a
            #     mon_payable_amt = mon_avg_s - self.mon_tax_free_a
            #     # 根据月应税金额找到对应的税率和速算扣除数（根据公司税率分级）
            #     tax_rate, tax_deduct = self.get_tax_rate_deduct(mon_payable_amt, 'BR_TAX_ER')
            #     gv.get_var_obj('VR_TAXRATE03_ER').value = tax_rate
            #     gv.get_var_obj('VR_TAXDEDUC03_ER').value = tax_deduct
            #
            #     # 月税前金额 =(月应税金额 – 速算扣除数) / ( 1 – 税率/100 ) + a
            #     mon_pre_tax_amt = (mon_payable_amt - tax_deduct) / (1 - tax_rate/100) + self.mon_tax_free_a
            #     # 总税前金额 = 月税前金额 * 年限 + q
            #     tot_pre_tax_amt = mon_pre_tax_amt * age + self.tot_tax_free_q

        # 离职补偿金税(公司)= 总税前金额 - 离职补偿金计税(个人)月度累计 - 离职补偿金计税(公司)月度累计 - 离职补偿金计税(个人) - 离职补偿金计税(公司) - 离职补偿金税(公司)月度累计
        wt_tax03_er = tot_pre_tax_amt - wc_taxbase03_ee_m - wc_taxbase03_er_m - wt_taxbase03_ee_p - wt_taxbase03_er - wc_tax03_er_m

        if tax_rate != 0:
            set_pin_seg_amt('WT_TAX03_ER', wt_tax03_er, currency=self.pg_currency, ratio=tax_rate)
        else:
            set_pin_seg_amt('WT_TAX03_ER', wt_tax03_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAX03_ER'] = wt_tax03_er

        # 离职补偿金计税(公司反算) = 离职补偿金计税(公司) + 离职补偿金税(公司)
        wt_taxbase03_er_b = wt_taxbase03_er + wt_tax03_er

        set_pin_seg_amt('WT_TAXBASE03_ER_B', wt_taxbase03_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE03_ER_B'] = wt_taxbase03_er_b

        # 离职补偿金免税金额(公司) = 总离职补偿金免税金额 – (离职补偿金免税金额(个人)月度累计 + 离职补偿金免税金额(公司)月度累计 + 离职补偿金免税金额(个人))
        wt_taxfree03_er = tot_tax03_free_amt - (
                    self.tax_wc_dic['WC_TAXFREE03_EE_M'] + self.tax_wc_dic['WC_TAXFREE03_ER_M'] + self.tax_pin_dic[
                'WT_TAXFREE03_EE'])
        set_pin_seg_amt('WT_TAXFREE03_ER', wt_taxfree03_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE03_ER'] = wt_taxfree03_er

        # 离职补偿金应税金额(公司) = 离职补偿金计税(公司) - 离职补偿金免税金额(公司)
        wt_taxbase03_act_er = wt_taxbase03_er - wt_taxfree03_er
        set_pin_seg_amt('WT_TAXBASE03_ACT_ER', wt_taxbase03_act_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE03_ACT_ER'] = wt_taxbase03_act_er

        # 离职补偿金应税金额(公司反算) = 离职补偿金计税(公司反算) - 离职补偿金免税金额(公司)
        wt_taxbase03_act_er_b = wt_taxbase03_er_b - wt_taxfree03_er

        set_pin_seg_amt('WT_TAXBASE03_ACT_ER_B', wt_taxbase03_act_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE03_ACT_ER_B'] = wt_taxbase03_act_er_b

    def calc_tax04(self):
        """计算其他所得"""

        # 其他所得的税率固定为20%（变量VR_TAXRATE04）
        rate = gv.get_var_value('VR_TAXRATE04')

        # 其他所得税 = 其他所得计税(个人) * 税率/100
        wt_taxbase04_ee = gv.get_pin('WT_TAXBASE04_EE').segment['*'].amt
        if wt_taxbase04_ee <= 0:
            pass
        else:
            wt_tax04_ee = wt_taxbase04_ee * rate / 100
            set_pin_seg_amt('WT_TAX04_EE', wt_tax04_ee, currency=self.pg_currency, ratio=rate)
            self.tax_pin_dic['WT_TAX04_EE'] = wt_tax04_ee

        # 其他所得计税(公司反算) = 其他所得计税(公司) / ( 1 - 税率/100 )
        wt_taxbase04_er = gv.get_pin('WT_TAXBASE04_ER').segment['*'].amt
        if wt_taxbase04_er <= 0:
            pass
        else:
            wt_taxbase04_er_b = wt_taxbase04_er / (1 - rate / 100)
            set_pin_seg_amt('WT_TAXBASE04_ER_B', wt_taxbase04_er_b, currency=self.pg_currency, ratio=rate)
            self.tax_pin_dic['WT_TAXBASE04_ER_B'] = wt_taxbase04_er_b

            # 其他所得税(公司) = 其他所得计税(公司反算) * 税率/100
            wt_tax04_er = wt_taxbase04_er_b * rate / 100
            set_pin_seg_amt('WT_TAX04_ER', wt_tax04_er, currency=self.pg_currency, ratio=rate)
            self.tax_pin_dic['WT_TAX04_ER'] = wt_tax04_er

    def calc_tax05_ee(self):
        """计算劳务费应税金额（个人）、劳务费免税金额（个人）、劳务费税"""

        vr_taxtype = gv.get_var_value('VR_TAXTYPE')

        # 本次计算的劳务费计税（个人）（记为aa）
        tax05_aa = gv.get_pin('WT_TAXBASE05_EE').segment['*'].amt
        if tax05_aa <= 0:
            return
        # 本月已累计的计税金额(2项总和记为bb)
        tax05_bb = self.tax_wc_dic['WC_TAXBASE05_EE_M'] + self.tax_wc_dic['WC_TAXBASE05_ER_B_M']
        # 本月已累计的免税金额（2项总和记为cc）
        tax05_cc = self.tax_wc_dic['WC_TAXFREE05_EE_M'] + self.tax_wc_dic['WC_TAXFREE05_ER_M']
        # 上述（aa+bb）计税金额，根据此金额（VR_AMT）获取减除金额（VR_AMT_DEDUCT）、减除比例（VR_RATE_DEDUCT）：
        vr_amt = tax05_aa + tax05_bb
        bracket_cd = ''
        if vr_taxtype == '1':
            bracket_cd = 'BR_LABOUR_01'
        elif vr_taxtype == '2':
            bracket_cd = 'BR_LABOUR_02'
        tax_rate, tax_deduct = self.get_tax_rate_deduct(vr_amt, bracket_cd)
        # 总劳务费免税金额（记为dd） = 减除金额 + ( aa + bb ) * 减除比例 / 100
        tax05_dd = tax_deduct + vr_amt * tax_rate / 100
        # 若 >（aa+bb），则重置为（aa+bb）
        if tax05_dd > vr_amt:
            tax05_dd = vr_amt

        # 【劳务费免税金额(个人)】 （WT_TAXFREE05_EE） = dd - cc
        wt_taxfree05_ee = tax05_dd - tax05_cc
        # 若<0,则重置为0；若>aa,则重置为aa
        if wt_taxfree05_ee < 0:
            wt_taxfree05_ee = 0
        elif wt_taxfree05_ee > tax05_aa:
            wt_taxfree05_ee = tax05_aa
        set_pin_seg_amt('WT_TAXFREE05_EE', wt_taxfree05_ee, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE05_EE'] = wt_taxfree05_ee

        # 【劳务费应税金额(个人)】 （WT_TAXBASE05_ACT_EE）= aa - 劳务费免税金额(个人)
        wt_taxbase05_act_ee = tax05_aa - wt_taxfree05_ee
        set_pin_seg_amt('WT_TAXBASE05_ACT_EE', wt_taxbase05_act_ee, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE05_ACT_EE'] = wt_taxbase05_act_ee

        """本月已累计的应税金额：
        WC_TAXBASE05_ACT_EE_M	 劳务费应税金额(个人)月度累计
        WC_TAXBASE05_ACT_ER_B_M	劳务费应税金额(公司反算)月度累计
        本次计算的应税金额：
        WT_TAXBASE05_ACT_EE	劳务费应税金额(个人)
        以上述3项之和（记为ee）找到对应的税率和速算扣除数"""
        tax05_ee = self.tax_wc_dic['WC_TAXBASE05_ACT_EE_M'] + self.tax_wc_dic[
            'WC_TAXBASE05_ACT_ER_B_M'] + wt_taxbase05_act_ee
        bracket_cd = ''
        if vr_taxtype == '1':
            bracket_cd = 'BR_LABOURTAX_EE'
        elif vr_taxtype == '2':
            bracket_cd = 'BR_TAX_EE'
        tax_rate, tax_deduct = self.get_tax_rate_deduct(tax05_ee, bracket_cd)
        gv.get_var_obj('VR_TAXRATE05_EE').value = tax_rate
        gv.get_var_obj('VR_TAXDEDUC05_EE').value = tax_deduct
        # 总劳务费税 = ee * 税率/100 - 速算扣除数
        tot_tax05_amt = tax05_ee * tax_rate / 100 - tax_deduct
        # 本月已累计的劳务费税(记为ff) =  劳务费税月度累计 + 劳务费税(公司)月度累计
        tax05_ff = self.tax_wc_dic['WC_TAX05_EE_M'] + self.tax_wc_dic['WC_TAX05_ER_M']
        # 【劳务费税】（WT_TAX05_EE） = 总劳务费税 – ff, 若<0,则重置为0
        wt_tax05_ee = tot_tax05_amt - tax05_ff
        if wt_tax05_ee < 0:
            wt_tax05_ee = 0
        set_pin_seg_amt('WT_TAX05_EE', wt_tax05_ee, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAX05_EE'] = wt_tax05_ee

    def calc_tax05_com(self):
        """计算劳务费税（公司）、劳务费计税（公司反算）、劳务费免税金额（公司）、劳务费应税金额(公司)、劳务费应税金额(公司反算)"""

        vr_taxtype = gv.get_var_value('VR_TAXTYPE')

        # 本次计算的劳务费计税（公司）
        wt_taxbase05_er = gv.get_pin('WT_TAXBASE05_ER').segment['*'].amt
        if wt_taxbase05_er <= 0:
            return
        # 总税后金额 = 劳务费计税(个人)月度累计 + 劳务费计税(公司)月度累计 + 劳务费计税(个人) + 劳务费计税(公司) -劳务费税月度累计 - 劳务费税
        wc_taxbase05_ee_m = self.tax_wc_dic['WC_TAXBASE05_EE_M']
        wc_taxbase05_er_m = self.tax_wc_dic['WC_TAXBASE05_ER_M']
        wt_taxbase05_ee = gv.get_pin('WT_TAXBASE05_EE').segment['*'].amt
        wc_tax05_ee_m = self.tax_wc_dic['WC_TAX05_EE_M']
        wt_tax05_ee = gv.get_pin('WT_TAX05_EE').segment['*'].amt
        tot_af_tax_amt = wc_taxbase05_ee_m + wc_taxbase05_er_m + wt_taxbase05_ee + wt_taxbase05_er - wc_tax05_ee_m - wt_tax05_ee

        """根据总税后金额反算总税前金额、总劳务费免税金额"""
        # 根据总税后金额找到对应的税率和速算扣除数
        bracket_cd = ''
        if vr_taxtype == '1':
            bracket_cd = 'BR_LABOURTAX_ER'
        elif vr_taxtype == '2':
            bracket_cd = 'BR_TAX_ER'
        tax_rate, tax_deduct = self.get_tax_rate_deduct(tot_af_tax_amt, bracket_cd)
        gv.get_var_obj('VR_TAXRATE05_ER').value = tax_rate
        gv.get_var_obj('VR_TAXDEDUC05_ER').value = tax_deduct
        # 总税后金额，根据此金额（VR_AMT）获取减除金额（VR_AMT_DEDUCT）、减除比例（VR_RATE_DEDUCT）
        bracket_cd = ''
        if vr_taxtype == '1':
            bracket_cd = 'BR_LABOUR_03'
        elif vr_taxtype == '2':
            bracket_cd = 'BR_LABOUR_02'
        vr_rate_deduct, vr_amt_deduct = self.get_tax_rate_deduct(tot_af_tax_amt, bracket_cd)
        # 总税前金额 = （ 总税后金额 – 速算扣除数 – 减除金额 * 税率 / 100 ） / （ 1 -（ 1 - 减除比例 / 100 ）* 税率/100 ）
        tot_pre_tax_amt = (tot_af_tax_amt - tax_deduct - vr_amt_deduct * tax_rate / 100) / (
                    1 - (1 - vr_rate_deduct / 100) * tax_rate / 100)
        bracket_cd = ''
        if vr_taxtype == '1':
            bracket_cd = 'BR_LABOUR_01'
        elif vr_taxtype == '2':
            bracket_cd = 'BR_LABOUR_02'
        vr_rate_deduct, vr_amt_deduct = self.get_tax_rate_deduct(tot_pre_tax_amt, bracket_cd)
        # 总劳务费免税金额 = 减除金额 + 总税前金额 * 减除比例 / 100，若>总税前金额，则重置为 总税前金额
        tot_tax05_free_amt = vr_amt_deduct + tot_pre_tax_amt * vr_rate_deduct / 100
        if tot_tax05_free_amt > tot_pre_tax_amt:
            tot_tax05_free_amt = tot_pre_tax_amt

        # 【劳务费税（公司）】（WT_TAX05_ER） = 总税前金额
        #  - 劳务费计税(个人)月度累计 - 劳务费计税(公司)月度累计
        #  - 劳务费计税(个人) - 劳务费计税(公司)
        #  - 劳务费税(公司)月度累计
        wt_tax05_er = tot_pre_tax_amt - wc_taxbase05_ee_m - wc_taxbase05_er_m - wt_taxbase05_ee - wt_taxbase05_er - \
                      self.tax_wc_dic['WC_TAX05_ER_M']
        set_pin_seg_amt('WT_TAX05_ER', wt_tax05_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAX05_ER'] = wt_tax05_er

        # 【劳务费计税（公司反算）】（WT_TAXBASE05_ER_B） = 劳务费计税(公司) + 劳务费税（公司）
        wt_taxbase05_er_b = wt_taxbase05_er + wt_tax05_er
        set_pin_seg_amt('WT_TAXBASE05_ER_B', wt_taxbase05_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE05_ER_B'] = wt_taxbase05_er_b

        # 本月已累计的劳务费免税金额：
        # WC_TAXFREE05_EE_M	劳务费免税金额(个人)月度累计
        # WC_TAXFREE05_ER_M	劳务费免税金额(公司)月度累计
        # 本次计算的劳务费免税金额(个人)：
        # WT_TAXFREE05_EE	劳务费免税金额(个人)
        wc_taxfree05_ee_m = self.tax_wc_dic['WC_TAXFREE05_EE_M']
        wc_taxfree05_er_m = self.tax_wc_dic['WC_TAXFREE05_ER_M']
        wt_taxfree05_ee = gv.get_pin('WT_TAXFREE05_EE').segment['*'].amt
        # 【劳务费免税金额(公司)】（WT_TAXFREE05_ER）= 总劳务费免税金额 – 上述3项之和
        wt_taxfree05_er = tot_tax05_free_amt - (wc_taxfree05_ee_m + wc_taxfree05_er_m + wt_taxfree05_ee)
        if wt_taxfree05_er < 0:
            wt_taxfree05_er = 0
        set_pin_seg_amt('WT_TAXFREE05_ER', wt_taxfree05_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXFREE05_ER'] = wt_taxfree05_er

        # 【劳务费应税金额(公司)】（WT_TAXBASE05_ACT_ER）= 劳务费计税（公司） - 劳务费免税金额(公司)
        wt_taxbase05_act_er = wt_taxbase05_er - wt_taxfree05_er
        set_pin_seg_amt('WT_TAXBASE05_ACT_ER', wt_taxbase05_act_er, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE05_ACT_ER'] = wt_taxbase05_act_er

        # 劳务费应税金额(公司反算)（WT_TAXBASE05_ACT_ER_B）= 劳务费计税（公司反算） - 劳务费免税金额(公司)
        wt_taxbase05_act_er_b = wt_taxbase05_er_b - wt_taxfree05_er
        set_pin_seg_amt('WT_TAXBASE05_ACT_ER_B', wt_taxbase05_act_er_b, currency=self.pg_currency)
        self.tax_pin_dic['WT_TAXBASE05_ACT_ER_B'] = wt_taxbase05_act_er_b

    def calc_tax_zero(self):
        """个税归零处理"""

        vr_tax_zero = gv.get_var_value('VR_TAX_ZERO')
        if vr_tax_zero == 0:
            pass
        elif vr_tax_zero == 1:
            """[工资税总额处理]"""
            # 工资税月度累计
            wc_tax01_ee_m = self.tax_wc_dic['WC_TAX01_EE_M']
            # 工资税(公司)月度累计
            wc_tax01_er_m = self.tax_wc_dic['WC_TAX01_ER_M']
            # 工资税
            wt_tax01_ee = self.tax_pin_dic['WT_TAX01_EE']
            # 工资税(公司)
            wt_tax01_er = self.tax_pin_dic['WT_TAX01_ER']
            # 上述4项之和<1,则将本次计算的工资税、工资税(公司)的金额置为0
            if wc_tax01_ee_m + wc_tax01_er_m + wt_tax01_ee + wt_tax01_er < 1:
                self.tax_pin_dic['WT_TAX01_EE'] = 0
                set_pin_seg_amt('WT_TAX01_EE', 0, currency=self.pg_currency)
                self.tax_pin_dic['WT_TAX01_ER'] = 0
                set_pin_seg_amt('WT_TAX04_ER', 0, currency=self.pg_currency)

            """[年终奖税总额处理]"""
            # 年终奖税月度累计
            wc_tax02_ee_m = self.tax_wc_dic['WC_TAX02_EE_M']
            # 年终奖税(公司)月度累计
            wc_tax02_er_m = self.tax_wc_dic['WC_TAX02_ER_M']
            # 年终奖税
            wt_tax02_ee = self.tax_pin_dic['WT_TAX02_EE']
            # 年终奖税(公司)
            wt_tax02_er = self.tax_pin_dic['WT_TAX02_ER']
            # 上述4项之和<1,则将本次计算的年终奖税、年终奖税(公司)的金额置为0
            if wc_tax02_ee_m + wc_tax02_er_m + wt_tax02_ee + wt_tax02_er < 1:
                self.tax_pin_dic['WT_TAX02_EE'] = 0
                set_pin_seg_amt('WT_TAX02_EE', 0, currency=self.pg_currency)
                self.tax_pin_dic['WT_TAX02_ER'] = 0
                set_pin_seg_amt('WT_TAX02_ER', 0, currency=self.pg_currency)

            """[离职补偿金税总额处理]"""
            # 离职补偿金税月度累计
            wc_tax03_ee_m = self.tax_wc_dic['WC_TAX03_EE_M']
            # 离职补偿金税(公司)月度累计
            wc_tax03_er_m = self.tax_wc_dic['WC_TAX03_ER_M']
            # 离职补偿金税
            wt_tax03_ee = self.tax_pin_dic['WT_TAX03_EE']
            # 离职补偿金税(公司)
            wt_tax03_er = self.tax_pin_dic['WT_TAX03_ER']
            # 上述4项之和<1,则将本次计算的离职补偿金税、离职补偿金税(公司)的金额置为0
            if wc_tax03_ee_m + wc_tax03_er_m + wt_tax03_ee + wt_tax03_er < 1:
                self.tax_pin_dic['WT_TAX03_EE'] = 0
                set_pin_seg_amt('WT_TAX03_EE', 0, currency=self.pg_currency)
                self.tax_pin_dic['WT_TAX03_ER'] = 0
                set_pin_seg_amt('WT_TAX03_ER', 0, currency=self.pg_currency)

            """[劳务报酬税总额处理]"""
            # 劳务报酬税月度累计
            wc_tax05_ee_m = self.tax_wc_dic['WC_TAX05_EE_M']
            # 劳务报酬税(公司)月度累计
            wc_tax05_er_m = self.tax_wc_dic['WC_TAX05_ER_M']
            # 劳务报酬税
            wt_tax05_ee = self.tax_pin_dic['WT_TAX05_EE']
            # 劳务报酬税(公司)
            wt_tax05_er = self.tax_pin_dic['WT_TAX05_ER']
            # 上述4项之和<1,则将本次计算的劳务报酬税、劳务报酬税(公司)的金额置为0
            if wc_tax05_ee_m + wc_tax05_er_m + wt_tax05_ee + wt_tax05_er < 1:
                self.tax_pin_dic['WT_TAX05_EE'] = 0
                set_pin_seg_amt('WT_TAX05_EE', 0, currency=self.pg_currency)
                self.tax_pin_dic['WT_TAX05_ER'] = 0
                set_pin_seg_amt('WT_TAX05_ER', 0, currency=self.pg_currency)

            """[个税总额处理]"""
            # 个税月度累计
            wc_tax_ee_m = self.tax_wc_dic['WC_TAX_EE_M']
            # 个税(公司)月度累计
            wc_tax_er_m = self.tax_wc_dic['WC_TAX_ER_M']
            # 个税
            wt_tax_ee = self.tax_pin_dic['WT_TAX_EE']
            # 个税(公司)
            wt_tax_er = self.tax_pin_dic['WT_TAX_ER']
            # 上述4项之和<1,则将本次计算的个税、个税(公司)的金额置为0
            if wc_tax_ee_m + wc_tax_er_m + wt_tax_ee + wt_tax_er < 1:
                self.tax_pin_dic['WT_TAX_EE'] = 0
                set_pin_seg_amt('WT_TAX_EE', 0, currency=self.pg_currency)
                self.tax_pin_dic['WT_TAX_ER'] = 0
                set_pin_seg_amt('WT_TAX_ER', 0, currency=self.pg_currency)
        else:
            pass

    def calc_tax(self):
        self.init_tax_pin_dic()
        self.init_tax_wc_dic()

        # 被追溯的期间(期间状态R)：计税金额(公司反算)/免税金额/应税金额/个税维持不变，即直接取历史序号对应的期间的数据
        if self.period_status == 'R':
            hist_seq = self.catalog.hist_seq
            self.copy_from_hist_tax(hist_seq)
            # 1.1 获取本年（税务年度）已累计的计税金额、个税
            self.get_tax_year_amt()

            # 1.2 获取累计月数
            self.get_mon_cum()

            # 2.获取月度免税金额(记为a)
            self.mon_tax_free_a = self.get_mon_tax_free_amt()
        else:
            # 1.获取本月已累计的计税金额、已缴纳的个税、应税金额、免税金额
            self.set_wt_tax_amt()

            # 1.1 获取本年（税务年度）已累计的计税金额、个税
            self.get_tax_year_amt()

            # 1.2 获取累计月数
            self.get_mon_cum()

            # 2.获取月度免税金额(记为a)
            self.mon_tax_free_a = self.get_mon_tax_free_amt()

            # 3.计算工资应税金额(个人)、工资免税金额(个人)、工资税
            self.calc_tax01_ee()

            # 3.1 按年度计算个税（个人）
            self.calc_year_tax01_ee()

            # 4.计算工资税(公司)、工资计税(公司反算)、工资免税金额(公司)、工资应税金额(公司)、工资应税金额(公司反算)
            self.calc_tax01_com()

            # 4.1 按年度计算个税（公司）
            self.calc_year_tax01_com()

            # 5.计算年终奖应税金额(个人)、年终奖免税金额(个人)、年终奖税
            self.calc_tax02_ee()

            # 6.计算年终奖税(公司)、年终奖计税(公司反算)、年终奖免税金额(公司)、年终奖应税金额(公司)、年终奖应税金额(公司反算)
            self.calc_tax02_com()

            # 7.计算离职补偿金应税金额(个人)、离职补偿金免税金额(个人)、离职补偿金税
            self.calc_tax03_ee()

            # 8.计算离职补偿金税(公司)、离职补偿金计税(公司反算)、离职补偿金免税金额(公司)、离职补偿金应税金额(公司)、离职补偿金应税金额(公司反算)
            self.calc_tax03_com()

            # 9.计算其他所得
            self.calc_tax04()

            # 10.计算劳务费应税金额（个人）、劳务费免税金额（个人）、劳务费税
            self.calc_tax05_ee()

            # 11.计算劳务费税（公司）、劳务费计税（公司反算）、劳务费免税金额（公司）、劳务费应税金额(公司)、劳务费应税金额(公司反算)
            self.calc_tax05_com()

            # 12.个税归零处理
            self.calc_tax_zero()
