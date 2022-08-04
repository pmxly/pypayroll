# -*- coding: utf-8 -*-

"""
薪资项目处理
"""

from ..pysysutils import global_variables as gv
from sqlalchemy import text
from ..pysysutils.func_lib_02 import ex_currency
from datetime import datetime
from ..pysysutils.py_calc_log import add_fc_log_item
from copy import deepcopy
from datetime import timedelta
from ..pyexecute.pycalculate.table.pin_sub_cls import PinSubCls

one_day = timedelta(days=1)


class PinProcess:
    """
    Desc: 处理每个员工的薪资项目
    Author: David
    Date: 2018/09/11
    """

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.fc_obj = kwargs.get('fc_obj', None)
        self.catalog = kwargs.get('catalog', None)
        self.cal_obj = kwargs.get('cal', None)
        self.onetime_pin_dic = {}  # {'pin1': {2018/01/10: [2000,'CNY', 2018/01/31, 'C'], 2018/04/08: [3000,'CNY', 9999/12/31,'U']}, 'pin2': {......}}
        self.recur_pin_dic = {}  # {'pin1': {2018/01/10: [2000,'CNY', 2018/01/31], 2018/04/08: [3000,'CNY', 9999/12/31]}, 'pin2': {......}}
        self.base_sal_dic = {}  # {'Base': {2018/01/10: [2000,'CNY', 9999/12/31], 2018/04/08: [3000,'CNY', 9999/12/31]}, 'Post': {......}}
        self.forever_date = datetime(9999, 12, 31).date()

        # 薪资项目类别（基础薪酬B、经常性R、一次性O）字典
        self.pin_class_dic = {}  # {'pin1': 'O', 'pin2': 'R', 'pin3':'B', ......}

        # 获取系统薪资项目和适用范围薪资项目
        self.pins_dic = gv.get_pin_dic()

    def process_base_pins_old(self):
        """
        Desc: 处理基础薪酬（该方法处理逻辑有问题不再使用）
        Author: David
        Date: 2018/09/11
        """

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        f_end_date = catalog.f_prd_end_dt

        # 根据人员编码、任职记录号从基础薪酬表中获取历经期内的记录
        txt = text("select a.hhr_efft_date,a.hhr_pin_cd,a.hhr_amt,a.hhr_currency from boogoo_payroll.hhr_py_base_sal a "
                   "where a.tenant_id = :b1 and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 "
                   "and a.hhr_efft_date <= :b4 and a.hhr_pin_cd is not null "
                   "order by a.hhr_empid, a.hhr_emp_rcd, a.hhr_efft_date, a.hhr_efft_seq desc ")
        rs = db.conn.execute(txt, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=f_end_date).fetchall()

        for row in rs:
            pin_cd = row['hhr_pin_cd']

            # 一次性支付扣除、经常性支付扣除的优先级高于基础薪酬，若薪资项目重复出现，则忽略该基础薪酬
            if pin_cd in self.pin_class_dic:
                process_type = self.pin_class_dic[pin_cd]
                if process_type != 'B':
                    continue

            eff_date = row['hhr_efft_date']
            amt = row['hhr_amt']
            currency = row['hhr_currency']

            if pin_cd not in self.base_sal_dic:
                self.base_sal_dic[pin_cd] = {eff_date: [amt, currency, self.forever_date]}
            elif eff_date not in self.base_sal_dic[pin_cd]:
                self.base_sal_dic[pin_cd][eff_date] = [amt, currency, self.forever_date]
            self.pin_class_dic[pin_cd] = 'B'
        self.init_pins_table(self.base_sal_dic)
        add_fc_log_item(self.fc_obj, 'WT', '', self.base_sal_dic)

    def process_base_pins(self):
        """
        Desc: 处理基础薪酬
        Author: David
        Date: 2019/11/21
        """

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        f_end_date = catalog.f_prd_end_dt

        process_tmp_dic = {}

        date_list = []
        pin_cd_list = []
        pin_date_list = []

        # 根据人员编码、任职记录号从基础薪酬表中获取历经期内的记录
        txt = text("select b.hhr_empid, b.hhr_emp_rcd, b.hhr_efft_date, b.hhr_efft_seq, "
                   "a.hhr_pin_cd,a.hhr_amt,a.hhr_currency from boogoo_payroll.hhr_py_base_sal a "
                   "right join boogoo_payroll.hhr_py_assign_pg b on b.tenant_id = a.tenant_id and b.hhr_empid = a.hhr_empid "
                   "and b.hhr_emp_rcd = a.hhr_emp_rcd and b.hhr_efft_date = a.hhr_efft_date "
                   "and b.hhr_efft_seq = a.hhr_efft_seq "
                   "where b.tenant_id = :b1 and b.hhr_empid = :b2 and b.hhr_emp_rcd = :b3 "
                   "and b.hhr_efft_date <= :b4 "
                   "order by b.hhr_empid, b.hhr_emp_rcd, b.hhr_efft_date, b.hhr_efft_seq desc ")
        rs = db.conn.execute(txt, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=f_end_date).fetchall()
        for row in rs:
            pin_cd = row['hhr_pin_cd']

            # 一次性支付扣除、经常性支付扣除的优先级高于基础薪酬，若薪资项目重复出现，则忽略该基础薪酬
            # (在运行类型中实际处理顺序是：基础薪酬 > 经常性 > 一次性，所以如下4行代码也可以直接忽略)
            if pin_cd and (pin_cd in self.pin_class_dic):
                process_type = self.pin_class_dic[pin_cd]
                if process_type != 'B':
                    continue

            if pin_cd:
                self.pin_class_dic[pin_cd] = 'B'

            eff_date = row['hhr_efft_date']
            eff_seq = row['hhr_efft_seq']
            amt = row['hhr_amt']
            currency = row['hhr_currency']

            # 取序号最大的记录
            if (emp_id, emp_rcd, eff_date) not in process_tmp_dic:
                process_tmp_dic[(emp_id, emp_rcd, eff_date)] = eff_seq
            else:
                init_eff_seq = process_tmp_dic[(emp_id, emp_rcd, eff_date)]
                if init_eff_seq != eff_seq:
                    continue

            if eff_date not in date_list:
                date_list.append(eff_date)

            if pin_cd:
                if pin_cd not in pin_cd_list:
                    pin_cd_list.append(pin_cd)

                if (eff_date, pin_cd) not in pin_date_list:
                    pin_date_list.append((eff_date, pin_cd))

                if pin_cd not in self.base_sal_dic:
                    self.base_sal_dic[pin_cd] = {eff_date: [amt, currency, self.forever_date]}
                elif eff_date not in self.base_sal_dic[pin_cd]:
                    self.base_sal_dic[pin_cd][eff_date] = [amt, currency, self.forever_date]

        for each_date in date_list:
            for each_pin_cd in pin_cd_list:
                if (each_date, each_pin_cd) not in pin_date_list:
                    if each_pin_cd in self.base_sal_dic:
                        self.base_sal_dic[each_pin_cd][each_date] = [0, 'CNY', self.forever_date]
                    else:
                        self.base_sal_dic[each_pin_cd] = {each_date: [0, 'CNY', self.forever_date]}

        if len(self.base_sal_dic) > 0:
            self.init_pins_table(self.base_sal_dic)
        add_fc_log_item(self.fc_obj, 'WT', '', self.base_sal_dic)

    def process_recurrent_pins(self):
        """
        Desc: 处理经常性支付扣除
        Author: David
        Date: 2018/09/17
        """

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        f_bgn_date = catalog.f_prd_bgn_dt
        f_end_date = catalog.f_prd_end_dt

        # 根据人员编码、任职记录号从经常性支付扣除表中获取历经期内的记录
        sql = text(
            "select a.hhr_pin_cd,a.hhr_bgn_dt,a.hhr_end_dt,a.hhr_amt,a.hhr_currency from boogoo_payroll.hhr_py_recur_ovrd a where "
            "a.tenant_id = :b1 and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 and a.hhr_pin_cd is not null "
            "and ( (:b4 <= a.hhr_end_dt and :b5 >= a.hhr_bgn_dt) or (a.hhr_end_dt is null and :b5 >=a.hhr_bgn_dt) ) "
            "order by a.hhr_pin_cd,a.hhr_bgn_dt ")
        rs = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=f_bgn_date, b5=f_end_date).fetchall()
        for row in rs:
            pin_cd = row['hhr_pin_cd']

            # 一次性支付扣除、明细类收入扣减的优先级高于经常性支付扣除，若薪资项目重复出现，则跳过该经常性支付扣除薪资项
            if pin_cd in self.pin_class_dic:
                process_type = self.pin_class_dic[pin_cd]
                if process_type == 'O' or process_type == 'D':
                    continue

            bgn_date = row['hhr_bgn_dt']
            end_date = row['hhr_end_dt']
            if end_date is None:
                end_date = self.forever_date
            amt = row['hhr_amt']
            currency = row['hhr_currency']

            # 构造薪资项目与开始日期的其他组合，以便对薪资项目做时间范围拆分
            if pin_cd not in self.recur_pin_dic:
                self.recur_pin_dic[pin_cd] = {bgn_date: [amt, currency, end_date]}
            elif bgn_date not in self.recur_pin_dic[pin_cd]:
                self.recur_pin_dic[pin_cd][bgn_date] = [amt, currency, end_date]
            self.pin_class_dic[pin_cd] = 'R'
        self.init_pins_table(self.recur_pin_dic)
        add_fc_log_item(self.fc_obj, 'WT', '', self.recur_pin_dic)

    def process_onetime_pins(self):
        """
        Desc: 处理一次性支付扣除
        Author: David
        Date: 2018/09/17
        """

        db = self.db
        catalog = self.catalog
        # cal_obj = self.cal_obj
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        f_bgn_date = catalog.f_prd_bgn_dt
        f_end_date = catalog.f_prd_end_dt
        f_cal_id = catalog.f_cal_id

        if catalog.new_entry_flg == 'Y':
            f_cal_id = ''

        # 根据人员编码、任职记录号、历经期日历编码从一次性支付扣除表中获取记录
        temp_lst = []
        s1 = "select '1', a.hhr_pin_cd,a.hhr_amt,a.hhr_currency,a.hhr_prcs_flag from boogoo_payroll.hhr_py_onetime_ovrd a where a.tenant_id = :b1 " \
             "and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 and a.hhr_py_cal_id = :b4 and a.hhr_pin_cd is not null "

        # 如果当前期间是更正类型，还需要合并被更正日历上的一次性元素（同一个一次性元素优先取当前期间的数据）
        upd_cal_id = gv.get_run_var_value('UPD_CAL_ID')
        if upd_cal_id:
            s2 = "union select '2', a.hhr_pin_cd,a.hhr_amt,a.hhr_currency,a.hhr_prcs_flag from boogoo_payroll.hhr_py_onetime_ovrd a where a.tenant_id = :b1 " \
                 "and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 and a.hhr_py_cal_id = :b5 and a.hhr_pin_cd is not null order by 1, 2 "
            stmt = s1 + s2
        else:
            stmt = s1

        rs = db.conn.execute(text(stmt), b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=f_cal_id, b5=upd_cal_id).fetchall()
        for row in rs:
            pin_cd = row['hhr_pin_cd']
            if pin_cd not in temp_lst:
                temp_lst.append(pin_cd)
            else:
                continue
            amt = row['hhr_amt']
            currency = row['hhr_currency']
            process_flag = row['hhr_prcs_flag']

            if pin_cd not in self.onetime_pin_dic:
                self.onetime_pin_dic[pin_cd] = {f_bgn_date: [amt, currency, f_end_date, process_flag, 'O']}
            elif f_bgn_date not in self.onetime_pin_dic[pin_cd]:
                self.onetime_pin_dic[pin_cd][f_bgn_date] = [amt, currency, f_end_date, process_flag, 'O']
            self.pin_class_dic[pin_cd] = 'O'
        self.init_pins_table(self.onetime_pin_dic)
        add_fc_log_item(self.fc_obj, 'WT', '', self.onetime_pin_dic)

    def process_detail_pins(self):
        """
        Desc: 处理明细类收入扣减
        Author: David
        Date: 2020/12/09
        """

        db = self.db
        catalog = self.catalog
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num
        f_bgn_date = catalog.f_prd_bgn_dt
        f_end_date = catalog.f_prd_end_dt
        f_cal_id = catalog.f_cal_id
        pg_currency = gv.get_var_value('VR_PG_CURRENCY')

        if catalog.new_entry_flg == 'Y':
            f_cal_id = ''

        # 如果当前是更正计算
        upd_cal_id = gv.get_run_var_value('UPD_CAL_ID')
        if upd_cal_id:
            temp_cal_id = upd_cal_id
        else:
            temp_cal_id = f_cal_id

        # 根据(员工号、任职记录号)从明细类收入扣减中取值，满足下列2个条件之一即可：
        # 1.日历=历经期日历编码（或者更正计算时 日历=被更正日历编码）；
        # 2.日历字段无值，但开始日期<=历经期结束日期，结束日期>=历经期开始日期。【仅针对周期性】
        s = "SELECT d.hhr_pin_sub_cls_cd, s.hhr_pin_sub_cls_type, s.hhr_pin_cd, d.hhr_count, d.hhr_unit, d.hhr_comment " \
            "FROM boogoo_payroll.hhr_py_detail_ovrd d JOIN boogoo_payroll.hhr_py_pin_sub_class s ON s.tenant_id = d.tenant_id " \
            "AND s.hhr_pin_sub_cls_cd = d.hhr_pin_sub_cls_cd WHERE d.tenant_id = :b1 AND d.hhr_empid = :b2 AND d.hhr_emp_rcd = :b3 " \
            "AND ( d.hhr_py_cal_id = :b4 "
        if self.cal_obj.run_type_entity.cycle == 'C':
            s = s + " OR ( (d.hhr_py_cal_id IS NULL OR d.hhr_py_cal_id = '') AND d.hhr_start_date <= :b5 AND d.hhr_end_date >= :b6 ) "
        s = s + " )"
        rs = db.conn.execute(text(s), b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=temp_cal_id, b5=f_end_date, b6=f_bgn_date)
        pin_sum_count_dic = {}
        for row in rs:
            pin_sub_cls_cd = row['hhr_pin_sub_cls_cd']
            pin_sub_cls_type = row['hhr_pin_sub_cls_type']
            pin_cd = row['hhr_pin_cd']

            if pin_cd not in self.pins_dic:
                continue

            # 明细类收入扣减的优先级高于基础薪酬、经常性，但低于一次性
            if pin_cd in self.pin_class_dic:
                process_type = self.pin_class_dic[pin_cd]
                if process_type == 'O':
                    continue
            self.pin_class_dic[pin_cd] = 'D'

            count = row['hhr_count']
            unit = row['hhr_unit']
            if pin_sub_cls_type == 'A' and pg_currency != unit:
                count = ex_currency(unit, pg_currency, count)
                unit = pg_currency
            comment = row['hhr_comment']

            if pin_cd not in pin_sum_count_dic:
                pin_sum_count_dic[pin_cd] = [count, unit, pin_sub_cls_type]
            else:
                pin_sum_count_dic[pin_cd][0] = pin_sum_count_dic[pin_cd][0] + count

            v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num, 'pin_cd': pin_cd,
                     'pin_sub_cls_cd': pin_sub_cls_cd, 'count': count, 'unit': unit,
                     'pin_sub_cls_type': pin_sub_cls_type, 'comment': comment}
            PinSubCls(**v_dic).insert()

        for key, val_lst in pin_sum_count_dic.items():
            pin_obj = self.pins_dic[key]
            j = 0
            seg_len = len(pin_obj.segment.values())
            # 给最后一个分段复制，前面的分段都赋值0
            for seg in pin_obj.segment.values():
                j += 1
                if j != seg_len:
                    seg.init_amt = 0
                    seg.amt = 0
                    seg.std_amt = 0
                    seg.init_currency = ''
                    seg.currency = ''
                    seg.quantity_ = 0
                    seg.quantity_unit = ''
                    seg.seg_rule_id = ''
                    seg.prcs_flag = 'S'
                    continue
                else:
                    seg.seg_rule_id = ''
                    pin_obj.seg_rule_id = ''
                    if val_lst[2] == 'A':
                        seg.amt = val_lst[0]
                        seg.std_amt = val_lst[0]
                        seg.init_amt = val_lst[0]
                        seg.init_currency = val_lst[1]
                        seg.currency = val_lst[1]
                        seg.quantity_ = 0
                        seg.quantity_unit = ''
                    elif val_lst[2] == 'B':
                        seg.amt = 0
                        seg.std_amt = 0
                        seg.init_amt = 0
                        seg.init_currency = ''
                        seg.currency = ''
                        seg.quantity_ = val_lst[0]
                        seg.quantity_unit = val_lst[1]
                    seg.prcs_flag = 'S'
                    pin_obj.prcs_flag = 'S'
        add_fc_log_item(self.fc_obj, 'WT', '', pin_sum_count_dic)

    def init_pins_table(self, pins_source_dic):
        """
        初始化薪资项目表
        Created by David on 2018/09/11
        :param pins_source_dic: 员工薪资项目各日期对应的金额数据字典
        :return:
        """

        # 将基础薪酬/经常性支付扣除/一次性支付扣除中的薪资项目整合到薪资项目表
        # 属于系统薪资项目或适用范围薪资项目的记录，整合至薪资项目表。不在范围内的薪资项目被忽略。
        for pin_cd in pins_source_dic:
            if pin_cd in self.pins_dic:
                pin_obj = self.pins_dic[pin_cd]
                pin_dt_amt_dic = pins_source_dic[pin_cd]
                pin_dt_amt_dic = dict(sorted(pin_dt_amt_dic.items(), key=lambda k: k[0]))
                self.init_pin_seg_amt(pin_obj, pin_dt_amt_dic)

    def init_pin_seg_amt(self, pin_obj, pin_dt_amt_dic):
        """
        Desc: 初始化薪资项各分段金额
        Author: David
        Date: 2018/09/11
        """

        # 设置每个薪资项每行记录的的结束日期
        i = 0
        prev_item = None
        one_time_flag = False
        for dt, info_lst in pin_dt_amt_dic.items():
            if len(info_lst) == 5:
                one_time_flag = True
            i += 1
            if i > 1:
                if info_lst[2] == self.forever_date:
                    # 将当前行的生效日期-1作为上一行的结束日期
                    prev_item[2] = dt - one_day
            prev_item = info_lst

        """薪资项目的属性是分段的，初始化的时候根据当月实际分段情况会拆分成具体的分段数，例如2段、3段、4段
        前面可能已经给此薪资项赋值了或尚未赋值,到一次性覆盖的时候，不能将此薪资项目的所有分段都赋上同一个值，
        需要把最后一个分段赋成此值，前面的分段都赋值0，
        同时薪资项目上的分段规则需要清除，因为覆盖的就是最终值，后续因子拆分的时候，不需要再乘以因子值了
        """

        j = 0
        seg_len = len(pin_obj.segment.values())
        for seg in pin_obj.segment.values():
            j += 1

            # 如果是一次性覆盖，只处理最后一个分段
            if one_time_flag:
                if j != seg_len:
                    seg.init_amt = 0
                    seg.amt = 0
                    seg.std_amt = 0
                    seg.init_currency = ''
                    seg.currency = ''
                    seg.quantity_ = 0
                    seg.quantity_unit = ''
                    seg.seg_rule_id = ''
                    continue
                else:
                    seg.seg_rule_id = ''
                    pin_obj.seg_rule_id = ''

            seg_bgn_dt = seg.bgn_dt
            seg_end_dt = seg.end_dt

            for dt, info_lst in pin_dt_amt_dic.items():
                process_flag = ''
                # 如果存在处理标识
                if len(info_lst) == 5:
                    process_flag = info_lst[3]
                pin_end_date = info_lst[2]
                if dt <= seg_end_dt and pin_end_date >= seg_bgn_dt:
                    seg.amt = info_lst[0]

                    seg.currency = info_lst[1]
                    seg.std_amt = info_lst[0]
                    seg.init_amt = info_lst[0]
                    seg.init_currency = info_lst[1]
                    if process_flag != '':
                        seg.prcs_flag = process_flag
                        pin_obj.prcs_flag = process_flag

            # 货币转换
            self.exchange_rate(seg)

    @staticmethod
    def exchange_rate(seg):
        """
        Desc: 当薪资项目的货币与薪资组货币（VR_PG_CURRENCY）不一致时，需按汇率转换。
              转换前的金额、货币赋值初始金额、初始货币，转换后的金额赋值标准金额/金额、货币
        Author: David
        Date: 2018/09/11
        """

        pg_currency = gv.get_var_value('VR_PG_CURRENCY')
        if pg_currency != seg.currency:
            new_amt = ex_currency(seg.currency, pg_currency, seg.amt)
            seg.init_amt = seg.amt
            seg.init_currency = seg.currency
            seg.amt = new_amt
            seg.std_amt = new_amt
            seg.currency = pg_currency

    def split_factor(self, pin):
        """
        对薪资项目按照分段因子值进行拆分，更新“金额”列
        Created by David on 2018/09/18
        :param pin: hhr.payroll.pypins.pin.Pin对象
        :return:
        """

        if pin.seg_rule_id:
            """
            对一个薪资项目：当|各分段的∑(金额)-(分段号最大的记录)标准金额|<=0.02时（拆分引起的2分以内的误差），需调整金额，
            使各分段的∑(金额)= (分段号最大的记录)标准金额，差异在分子最大的分段消除，最大分子超过1个，在前一个分段消除
            """

            seg_info = deepcopy(gv.get_run_var_value('SEG_INFO_OBJ'))
            temp_dic = seg_info.seg_items_dic[pin.seg_rule_id]

            # 获取各分段的∑(金额)
            sigma_amt = 0
            for seg in pin.segment.values():
                seg_item = temp_dic[seg.segment_num]
                factor_val = seg_item.factor_val
                new_amt = seg.amt * factor_val

                # 计算后的金额根据薪资项目的取整规则保留小数位。
                seg.amt = new_amt
                sigma_amt += seg.amt

            # 获取分段号最大的记录的标准金额
            segment_dic = pin.segment
            max_seg = segment_dic[len(segment_dic)]
            max_seg_std_amt = max_seg.std_amt

            gap = max_seg_std_amt - sigma_amt
            if 0 < abs(gap) <= 0.02:
                # 获取分子最大的分段
                max_numerator_val = 0
                max_numerator_seg = None

                for seg in pin.segment.values():
                    numerator_val = temp_dic[seg.segment_num].numerator_val
                    if numerator_val > max_numerator_val:
                        max_numerator_val = numerator_val
                        max_numerator_seg = seg
                max_numerator_seg.amt = max_numerator_seg.amt + gap
