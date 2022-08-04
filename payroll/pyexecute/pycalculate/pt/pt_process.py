# coding:utf-8

from ....pysysutils import global_variables as gv
from sqlalchemy import text
from ....pysysutils.py_calc_log import add_fc_log_item
from copy import deepcopy
from .....utils import get_current_dttm
from ..table.rslt_abs import Abs


class PTProcess:
    """
    员工考勤处理
    created by David 2018/11/19
    """

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.fc_obj = kwargs.get('fc_obj', None)
        self.catalog = kwargs.get('catalog', None)
        self.pg_currency = gv.get_var_value('VR_PG_CURRENCY')
        self.pt_period_beg = None
        self.pt_period_end = None
        self.abs_pins_rec_lst = []
        self.abs_obj_lst = []
        self.m_abs_pins_rec_lst = []
        self.abs_map_data_dic = {}

    def set_pt_cycle(self):
        """获取考勤周期"""

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        f_period_id = gv.get_var_value('VR_F_PERIOD')
        f_period_year = gv.get_var_value('VR_F_PERIOD_YEAR')
        f_period_num = gv.get_var_value('VR_F_PERIOD_NO')
        f_bgn_date = gv.get_var_value('VR_F_PERIOD_BEG')
        f_end_date = gv.get_var_value('VR_F_PERIOD_END')

        # 考勤期间
        vr_pg_ptperiod = gv.get_var_value('VR_PG_PTPERIOD')
        # 差异数
        vr_pg_nonum = gv.get_var_value('VR_PG_NONUM')

        # 如果“考勤期间”有值，或“差异数”>0，则需要计算考勤周期
        if vr_pg_ptperiod or vr_pg_nonum > 0:
            # 如果“考勤期间”有值，赋值给【考勤期间编码】
            if vr_pg_ptperiod:
                gv.set_var_value('VR_PT_PERIOD', vr_pg_ptperiod)
            else:
                # 直接将历经期期间编码赋值给【考勤期间编码】
                gv.set_var_value('VR_PT_PERIOD', f_period_id)

            # 先将历经期期间年度、期间序号赋值给【考勤期间年度】、【期间序号】
            gv.set_var_value('VR_PT_PERIOD_YEAR', f_period_year)
            gv.set_var_value('VR_PT_PERIOD_NO', f_period_num)

            abs_period_id = gv.get_var_value('VR_PT_PERIOD')
            abs_period_year = f_period_year
            abs_period_no = f_period_num

            prd_cal_dic = gv.get_run_var_value('PRD_CAL_DIC')
            key_tuple = (abs_period_id, abs_period_year)
            if key_tuple not in prd_cal_dic:
                prd_cal_dic[key_tuple] = []
                sql = text(
                    "select hhr_period_year, hhr_prd_num, hhr_period_start_date, hhr_period_end_date from boogoo_payroll.hhr_py_period_calendar_line "
                    "where tenant_id = :b1 and hhr_period_code = :b2 and (hhr_period_year = :b3 or hhr_period_year = :b4) "
                    "order by hhr_period_year desc, hhr_prd_num desc ")
                result = db.conn.execute(sql, b1=tenant_id, b2=abs_period_id, b3=abs_period_year,
                                         b4=abs_period_year - 1).fetchall()
                for row in result:
                    year = row['hhr_period_year']
                    prd_num = row['hhr_prd_num']
                    prd_bgn_date = row['hhr_period_start_date']
                    prd_end_date = row['hhr_period_end_date']
                    prd_cal_dic[key_tuple].append((year, prd_num, prd_bgn_date, prd_end_date))
            prd_cal_data = prd_cal_dic[key_tuple]

            # 如果“差异数”>0，则根据考勤期间编码、期间年度、期间序号，从期间日历表中获取实际的考勤周期
            # 例如：若差异数为1，则在期间日历中找到上一个期间；若差异数为2，则在期间日历中找到上两个期间
            if vr_pg_nonum > 0:
                reach_cur = False
                cnt = 0
                year = prd_num = 0
                prd_bgn_date = prd_end_date = None
                for row in prd_cal_data:
                    year = row[0]
                    prd_num = row[1]
                    prd_bgn_date = row[2]
                    prd_end_date = row[3]
                    if (not reach_cur) and (prd_num == abs_period_no):
                        reach_cur = True
                        continue
                    if reach_cur:
                        cnt += 1
                        if cnt == vr_pg_nonum:
                            gv.set_var_value('VR_PT_PERIOD_YEAR', year)
                            gv.set_var_value('VR_PT_PERIOD_NO', prd_num)
                            gv.set_var_value('VR_PT_PERIOD_BEG', prd_bgn_date)
                            gv.set_var_value('VR_PT_PERIOD_END', prd_end_date)
                            break
                # 如果差异数超过了可向上寻找的期间数，则取最后找到的期间
                if cnt < vr_pg_nonum:
                    gv.set_var_value('VR_PT_PERIOD_YEAR', year)
                    gv.set_var_value('VR_PT_PERIOD_NO', prd_num)
                    gv.set_var_value('VR_PT_PERIOD_BEG', prd_bgn_date)
                    gv.set_var_value('VR_PT_PERIOD_END', prd_end_date)

                self.pt_period_beg = prd_bgn_date
                self.pt_period_end = prd_end_date
            else:
                if len(prd_cal_data) > 0:
                    reach_cur = False
                    year = prd_num = 0
                    prd_bgn_date = prd_end_date = None
                    for row in prd_cal_data:
                        year = row[0]
                        prd_num = row[1]
                        prd_bgn_date = row[2]
                        prd_end_date = row[3]
                        if (not reach_cur) and (prd_num == abs_period_no):
                            reach_cur = True
                            break
                    if reach_cur:
                        gv.set_var_value('VR_PT_PERIOD_YEAR', year)
                        gv.set_var_value('VR_PT_PERIOD_NO', prd_num)
                        gv.set_var_value('VR_PT_PERIOD_BEG', prd_bgn_date)
                        gv.set_var_value('VR_PT_PERIOD_END', prd_end_date)
                        self.pt_period_beg = prd_bgn_date
                        self.pt_period_end = prd_end_date

        else:
            # 否则直接将历经期开始日期、结束日期赋值给考勤开始日期、结束日期
            # (考勤)开始日期
            gv.set_var_value('VR_PT_PERIOD_BEG', f_bgn_date)
            self.pt_period_beg = f_bgn_date
            # (考勤)结束日期
            gv.set_var_value('VR_PT_PERIOD_END', f_end_date)
            self.pt_period_end = f_end_date

    def get_pt_pin_records(self):
        """获取考勤项目记录"""

        db = self.db
        catalog = self.catalog
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num

        sql_abs = text(
            "select hhr_date, hhr_pt_code, hhr_seqnum, hhr_holiday_type, hhr_quantity, hhr_unit from boogoo_attendance.vw_hhr_pt_code_detail where tenant_id = :b1 "
            "and hhr_empid = :b2 and hhr_payroll_y = 'Y' and hhr_date >= :b3 and hhr_date <= :b4 "
            "and (hhr_pt_appstatues is null or hhr_pt_appstatues = '' or hhr_pt_appstatues = 'F') and hhr_status = 'Y' "
            "order by hhr_pt_code, hhr_date")
        rs = db.conn.execute(sql_abs, b1=tenant_id, b2=emp_id, b3=self.pt_period_beg, b4=self.pt_period_end).fetchall()
        for row in rs:
            abs_date = row['hhr_date']
            abs_cd = row['hhr_pt_code']
            abs_quantity = row['hhr_quantity']
            abs_unit = row['hhr_unit']
            abs_unique_seq_num = row['hhr_seqnum']
            holiday_type = row['hhr_holiday_type']
            item_dic = {'abs_date': abs_date, 'abs_unique_seq_num': abs_unique_seq_num, 'abs_cd': abs_cd,
                        'abs_quantity': abs_quantity,
                        'abs_unit': abs_unit, 'holiday_type': holiday_type, 'catalog_seq': 0, 'segment_num': 0,
                        'cal_s_days': 0, 'work_s_days': 0, 'legal_s_days': 0, 'option': ''}
            self.abs_pins_rec_lst.append(item_dic)

            val_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                       'abs_cd': abs_cd,
                       'abs_seq_num': abs_unique_seq_num, 'abs_date': abs_date, 'quantity': abs_quantity,
                       'unit': abs_unit, 'date_type': holiday_type}
            abs = Abs(**val_dic)
            self.abs_obj_lst.append(abs)

    def get_pt_rec(self):
        """获取考勤记录"""

        # 获取考勤周期
        self.set_pt_cycle()
        # 获取考勤项目记录
        self.get_pt_pin_records()

    def sum_pt_cd(self, pt_code):
        """
        汇总考勤项目
        :param pt_code: 待汇总的考勤项目代码
        :return: 总数量、单位
        """

        sum_quantity = 0
        pt_unit = ''
        for abs_item_dic in self.abs_pins_rec_lst:
            abs_cd = abs_item_dic['abs_cd']
            if abs_cd == pt_code:
                sum_quantity += abs_item_dic['abs_quantity']
                pt_unit = abs_item_dic['abs_unit']
        return sum_quantity, pt_unit

    def assign_pt_cd(self, pt_code, pt_date, quantity, unit):
        """
        赋值考勤项目
        :param pt_code: 考勤项目代码
        :param pt_date: 日期
        :param quantity: 数量
        :param unit: 单位
        :return: None
        """

        item_dic = {'abs_date': pt_date, 'abs_cd': pt_code, 'abs_quantity': quantity, 'abs_unit': unit,
                    'catalog_seq': 0, 'segment_num': 0,
                    'cal_s_days': 0, 'work_s_days': 0, 'legal_s_days': 0, 'option': ''}
        self.abs_pins_rec_lst.append(item_dic)

    def get_pt_py_map(self, tenant_id, country, eval_rule, end_dt):
        """
        获取考勤项目与薪资项目的对应关系
        :param tenant_id: 租户ID
        :param country: 国家/地区
        :param eval_rule: 考勤评估规则
        :param end_dt: 考勤结束日期/历经期结束日期
        :return abs_map_data_dic: 考勤项目与薪资项目的对应关系字典
        """

        db = self.db

        abs_pin_dic = gv.get_run_var_value('PT_PIN_DIC')

        key_tuple = (country, eval_rule, end_dt)
        if key_tuple not in abs_pin_dic:
            abs_pin_dic[key_tuple] = {}
            temp_lst = []
            sql = text(
                "select hhr_abs_cd, hhr_efft_date, hhr_pin_cd, hhr_acc_cd, hhr_calc_unit, hhr_day_hours, hhr_denominator, hhr_fixed_days, hhr_multi_type, "
                "hhr_multi_fixed_val, hhr_multi_var, hhr_status from boogoo_payroll.hhr_py_abs_pin where tenant_id = :b1 and hhr_country = :b2 and hhr_abs_eval_rule = :b3 "
                "and hhr_efft_date <= :b4 order by hhr_efft_date desc")
            rs = db.conn.execute(sql, b1=tenant_id, b2=country, b3=eval_rule, b4=end_dt).fetchall()
            for row in rs:
                abs_cd = row['hhr_abs_cd']
                if abs_cd not in temp_lst:
                    temp_lst.append(abs_cd)

                    hhr_status = row['hhr_status']
                    if hhr_status != 'Y':
                        continue

                    effdt = row['hhr_efft_date']
                    pin_cd = row['hhr_pin_cd']
                    acc_cd = row['hhr_acc_cd']
                    calc_unit = row['hhr_calc_unit']
                    day_hours = row['hhr_day_hours']
                    denominator = row['hhr_denominator']
                    fixed_days = row['hhr_fixed_days']
                    multi_type = row['hhr_multi_type']
                    multi_fixed_val = row['hhr_multi_fixed_val']
                    multi_var = row['hhr_multi_var']
                    abs_pin_dic[key_tuple][abs_cd] = {'effdt': effdt, 'pin_cd': pin_cd, 'acc_cd': acc_cd,
                                                      'calc_unit': calc_unit, 'day_hours': day_hours,
                                                      'denominator': denominator, 'fixed_days': fixed_days,
                                                      'multi_type': multi_type, 'multi_fixed_val': multi_fixed_val,
                                                      'multi_var': multi_var}
        self.abs_map_data_dic = abs_pin_dic[key_tuple]
        return self.abs_map_data_dic

    def get_cur_catalog_seg(self, abs_date):
        catalog = self.catalog
        seg_info = deepcopy(gv.get_run_var_value('SEG_INFO_OBJ'))
        seq_num = catalog.seq_num
        segment_num = 0
        temp_dic = seg_info.seg_items_dic['*']
        for i in range(1, len(temp_dic) + 1):
            seg_item = temp_dic[i]
            if seg_item.bgn_dt <= abs_date <= seg_item.end_dt:
                segment_num = seg_item.segment_num
                break
        # 如果找不到分段号，默认为1
        if segment_num == 0:
            segment_num = 1
        return seq_num, segment_num

    def get_cur_prd_value(self, emp_abs_item, pin_data_dic):
        """获取当前期间的薪资结果"""

        catalog = self.catalog
        seg_info = deepcopy(gv.get_run_var_value('SEG_INFO_OBJ'))

        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        # 分母(A-工作日(含法定)，B-工作日(不含法定)，C-日历日，D-固定天数)
        denominator = pin_data_dic['denominator']

        # 倍数类型(VR-变量，FX-固定值)
        multi_type = pin_data_dic['multi_type']
        # 倍数变量
        multi_var = pin_data_dic['multi_var']
        # 倍数固定值
        multi_fixed_val = pin_data_dic['multi_fixed_val']

        # 固定天数
        fixed_days = pin_data_dic['fixed_days']

        denominator_val = 0

        catalog_seq = emp_abs_item['catalog_seq']
        segment_num = emp_abs_item['segment_num']

        period_seg_lst = seg_info.period_seg_lst
        p_seg = None
        for p_seg in period_seg_lst:
            if p_seg.seq_num == catalog_seq and p_seg.segment_num == segment_num and \
                    p_seg.tenant_id == tenant_id and p_seg.emp_id == emp_id and p_seg.emp_rcd == emp_rcd:
                break
        if denominator == 'A':
            denominator_val = p_seg.work_sum_days + p_seg.legal_sum_days
        elif denominator == 'B':
            denominator_val = p_seg.work_sum_days
        elif denominator == 'C':
            denominator_val = p_seg.cal_sum_days
        elif denominator == 'D':
            denominator_val = fixed_days

        multi_val = 0
        if multi_type == 'VR':
            multi_val = gv.get_var_value(multi_var)
        elif multi_type == 'FX':
            multi_val = multi_fixed_val
        entry_leave_val = gv.get_var_value('VR_ENTRY_LEAVE')
        return denominator_val, multi_val, entry_leave_val

    def calc_abs_pin_amt(self, option, pin_obj, emp_abs_item, pin_data_dic, segment_flag, denominator_val, multi_val,
                         entry_leave_val):
        """
        计算考勤薪资项目金额
        :param option: C-当前期间；H-历史期间
        :param pin_obj: 考勤薪资项目对象
        :param emp_abs_item: 员工考勤记录
        :param pin_data_dic: 考勤薪资项目定义数据
        :param segment_flag: 分段标识 Y/N
        :param denominator_val: 分母值
        :param multi_val: 倍数值
        :param entry_leave_val: 月中入离职标识 Y/N
        :return:
        """
        db = self.db
        catalog = self.catalog
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        f_bgn_date = catalog.f_prd_bgn_dt
        f_end_date = catalog.f_prd_end_dt

        acc_cd_amt = 0
        abs_quantity = emp_abs_item['abs_quantity']
        abs_unit = emp_abs_item['abs_unit']
        seq_num = emp_abs_item['catalog_seq']
        segment_num = emp_abs_item['segment_num']
        day_hours = pin_data_dic['day_hours']

        # 如果为当前期间
        if option == 'C':
            # 评估基准
            acc_cd = pin_data_dic['acc_cd']
            acc_segment = gv.get_pin_acc(acc_cd).segment
            # 薪资项目分段：按日期依据“评估基准”对应分段记录进行评估。使用评估基准的“标准金额”列。
            # (需要注意的是：薪资项目如果分段，那么评估基准对应的薪资项目累计也必须分段; 薪资项目如果不分段，评估基准也不分段)
            if segment_flag == 'Y':
                acc_cd_amt = acc_segment[segment_num].std_amt
            else:
                """ 薪资项目不分段：首先判断人员是否为月中入离职 """
                # 非月中入离职(值N，全月在职)，依据“评估基准”分段号为*的记录进行评估，使用“金额”列
                if entry_leave_val == 'N':
                    acc_cd_amt = acc_segment['*'].amt
                # 月中入离职（值Y），依据“评估基准”分段号为*的记录进行评估，使用“标准金额”列
                elif entry_leave_val == 'Y':
                    acc_cd_amt = acc_segment['*'].std_amt
        # 如果为历史期间
        elif option == 'H':
            # 评估基准(必须是累计类型为P的薪资项目累计)
            acc_cd = pin_data_dic['acc_cd']
            if segment_flag != 'Y':
                temp_seg_num = 999
            else:
                temp_seg_num = segment_num
            sql_pin = text(
                "select hhr_amt, hhr_std_amt from boogoo_payroll.hhr_py_rslt_pin where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
                "and hhr_seq_num = :b4 and hhr_pin_cd = :b5 and hhr_segment_num = :b6 and hhr_accm_type = 'P' ")
            r = db.conn.execute(sql_pin, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=acc_cd,
                                b6=temp_seg_num).fetchone()
            if r is not None:
                if segment_flag == 'Y':
                    acc_cd_amt = r['hhr_std_amt']
                else:
                    if entry_leave_val == 'n':
                        acc_cd_amt = r['hhr_amt']
                    elif entry_leave_val == 'y':
                        acc_cd_amt = r['hhr_std_amt']

        """ 
        数量从考勤记录带入
        金额为“评估基准的（金额或标准金额）*倍数”后的金额除以分母乘以数量后的金额（根据取整规则保留小数位。（一步计算减少误差，不直接使用比率*数量）
        因为同一个薪资项目可能反复计算，所以标准金额、比率不能存入薪资结果表 
        """

        # 小时数
        if abs_unit == 'H':
            denominator_val = denominator_val * day_hours
        # 天数
        elif abs_unit == 'D':
            pass
        # 次数
        elif abs_unit == 'C':
            pass
        # 薪资项目的标准金额为使用的评估基准的金额或者标准金额*倍数后的金额
        std_amt = acc_cd_amt * multi_val
        quantity = abs_quantity
        amt = std_amt / denominator_val * quantity
        pg_currency = gv.get_var_value('VR_PG_CURRENCY')
        if option == 'C':
            if pin_obj.seg_flag == 'Y':
                seg = pin_obj.segment[segment_num]
                new_seg_num = segment_num
            else:
                seg = pin_obj.segment['*']
                new_seg_num = '*'
            seg.amt += amt
            seg.currency = pg_currency
            pin_obj.segment[new_seg_num] = seg
        elif option == 'H':
            # [开发者备注：如果是历史期间，则将分段号统一设置为*; 如果当前薪资项目刚好不分段，则合并金额]
            if pin_obj.seg_flag == 'Y':
                """
                如果同一个薪资项目已经存在分段号为*的记录(说明先前已经处理过该薪资项目的历史考勤)，则需要获取该分段，从而对其进行累加。
                比如:当前日历期间为9/1~9/30，考勤期间为8/1~8/31，员工有四条相同类型的考勤记录(8/6、8/7、8/24、8/27)，
                8月分两段，8/6和8/7两条在第1段，8/24和8/27在第2段，
                则需要分两段处理该考勤类型的记录，每一段金额放到9月份，分段号标记为*，处理第二段的时候，需要累计第一段的金额
                """
                if '*' in pin_obj.segment:
                    seg = pin_obj.segment['*']
                else:
                    seg = deepcopy(pin_obj.segment[1])
                seg.segment_num = '*'
            else:
                seg = pin_obj.segment['*']
            seg.amt += amt
            seg.currency = pg_currency
            seg.bgn_dt = f_bgn_date
            seg.end_dt = f_end_date
            pin_obj.segment['*'] = seg

    def set_catalog_segment(self):
        """设置考勤项目需要使用的薪资计算目录序号和分段号"""

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        f_bgn_date = gv.get_var_value('VR_F_PERIOD_BEG')
        f_end_date = gv.get_var_value('VR_F_PERIOD_END')

        # 考勤评估规则
        vr_pg_ptrule = gv.get_var_value('VR_PG_PTRULE')
        # 采用考勤周期薪资标准（标识）
        vr_pg_ptpy = gv.get_var_value('VR_PG_PTPY')
        # (历经期)国家/地区
        vr_f_country = gv.get_var_value('VR_F_COUNTRY')

        # 若VR_PG_PTPY的值为Y，则根据考勤结束日期获取考勤评估的具体规则
        if vr_pg_ptpy == 'Y':
            end_dt = gv.get_var_value('VR_PT_PERIOD_END')
        else:
            # 值<>Y，则根据历经期结束日期获取考勤评估的具体规则
            end_dt = f_end_date

        abs_pins_rec_lst = self.abs_pins_rec_lst
        abs_map_data_dic = self.get_pt_py_map(tenant_id, vr_f_country, vr_pg_ptrule, end_dt)
        for emp_abs_item in abs_pins_rec_lst:
            abs_cd = emp_abs_item['abs_cd']
            abs_date = emp_abs_item['abs_date']
            if abs_cd not in abs_map_data_dic:
                continue
            pin_data_dic = abs_map_data_dic[abs_cd]
            pin_cd = pin_data_dic['pin_cd']
            pin_obj = gv.get_pin(pin_cd)
            if pin_obj.prcs_flag != '':
                continue

            if vr_pg_ptpy == 'Y':
                # 日期在“当前历经期”内，直接使用“当前期间”的薪资结果
                if f_bgn_date <= abs_date <= f_end_date:
                    seq_num, segment_num = self.get_cur_catalog_seg(abs_date)
                    emp_abs_item['catalog_seq'] = seq_num
                    emp_abs_item['segment_num'] = segment_num
                    emp_abs_item['option'] = 'C'
                else:
                    # 日期不属于“当前历经期”，需要取日期所在的薪资结果
                    sql_catalog = text(
                        "select hhr_seq_num from boogoo_payroll.hhr_py_cal_catalog where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
                        "and hhr_seq_num < :b4 and hhr_f_prd_bgn_dt <= :b5 and hhr_f_prd_end_dt >= :b5 and hhr_f_rt_cycle = 'C' order by hhr_seq_num desc")
                    r1 = db.conn.execute(sql_catalog, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=catalog.seq_num,
                                         b5=abs_date).fetchone()
                    if r1 is None:
                        continue
                    seq_num = r1['hhr_seq_num']
                    emp_abs_item['catalog_seq'] = seq_num
                    sql_seg = text(
                        "select hhr_segment_num, hhr_cal_s_days, hhr_work_s_days, hhr_legal_s_days from boogoo_payroll.hhr_py_rslt_seg where tenant_id = :b1 "
                        "and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4 and hhr_seg_bgn_dt <= :b5 "
                        "and hhr_seg_end_dt >= :b5 order by hhr_seg_bgn_dt ")
                    r2 = db.conn.execute(sql_seg, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num,
                                         b5=abs_date).fetchone()
                    if r2 is None:
                        continue
                    segment_num = r2['hhr_segment_num']
                    emp_abs_item['segment_num'] = segment_num
                    emp_abs_item['cal_s_days'] = r2['hhr_cal_s_days']
                    emp_abs_item['work_s_days'] = r2['hhr_work_s_days']
                    emp_abs_item['legal_s_days'] = r2['hhr_legal_s_days']
                    emp_abs_item['option'] = 'H'
            else:
                seq_num, segment_num = self.get_cur_catalog_seg(abs_date)
                emp_abs_item['catalog_seq'] = seq_num
                emp_abs_item['segment_num'] = segment_num
                emp_abs_item['option'] = 'C'

    def merge_abs_items(self):
        """为减少取整规则造成的误差。相同考勤项目，若使用同一薪资结果且位于同一分段（序号及分段号相同），可先合并数量后再转换。
        合并后不用考虑日期(等到序号和分段号后，后面的计算不再需要日期) """

        db = self.db
        run_parm = gv.get_run_var_value('RUN_PARM')
        catalog = self.catalog
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num
        cur_dttm = get_current_dttm()
        user_id = run_parm['operator_user_id']

        sql_abs = text(
            "insert into boogoo_payroll.hhr_py_rslt_abs(tenant_id,hhr_empid, hhr_emp_rcd, hhr_seq_num, hhr_abs_code, "
            "hhr_date, hhr_quantity, hhr_unit, hhr_create_dttm, hhr_create_user, hhr_modify_dttm, hhr_modify_user, hhr_seqnum, hhr_holiday_type) "
            "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10, :b11, :b12, :b13, :b14) ")

        abs_pins_rec_lst = self.abs_pins_rec_lst
        m_abs_pins_rec_lst = self.m_abs_pins_rec_lst
        temp_lst = []
        for emp_abs_item in abs_pins_rec_lst:
            abs_cd = emp_abs_item['abs_cd']
            abs_date = emp_abs_item['abs_date']
            abs_quantity = emp_abs_item['abs_quantity']
            abs_unit = emp_abs_item['abs_unit']
            catalog_seq = emp_abs_item['catalog_seq']
            segment_num = emp_abs_item['segment_num']
            abs_unique_seq_num = emp_abs_item['abs_unique_seq_num']
            holiday_type = emp_abs_item['holiday_type']

            # 插入考勤记录
            db.conn.execute(sql_abs, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=abs_cd, b6=abs_date,
                            b7=abs_quantity, b8=abs_unit,
                            b9=cur_dttm, b10=user_id, b11=cur_dttm, b12=user_id, b13=abs_unique_seq_num,
                            b14=holiday_type)

            if (abs_cd, catalog_seq, segment_num) in temp_lst:
                continue
            else:
                temp_lst.append((abs_cd, catalog_seq, segment_num))
            new_item = dict()
            new_item['abs_cd'] = abs_cd
            # 数量都初始化为0
            new_item['abs_quantity'] = 0
            new_item['abs_unit'] = abs_unit
            new_item['catalog_seq'] = catalog_seq
            new_item['segment_num'] = segment_num
            new_item['cal_s_days'] = emp_abs_item['cal_s_days']
            new_item['work_s_days'] = emp_abs_item['work_s_days']
            new_item['legal_s_days'] = emp_abs_item['legal_s_days']
            new_item['option'] = emp_abs_item['option']

            for temp_item in abs_pins_rec_lst:
                temp_abs_cd = temp_item['abs_cd']
                temp_catalog_seq = temp_item['catalog_seq']
                temp_segment_num = temp_item['segment_num']
                temp_quantity = temp_item['abs_quantity']
                if temp_segment_num == segment_num and temp_catalog_seq == catalog_seq and temp_abs_cd == abs_cd:
                    new_item['abs_quantity'] += temp_quantity

            # 如果没有可以合并的，保留原来的数量
            if new_item['abs_quantity'] == 0:
                new_item['abs_quantity'] = emp_abs_item['abs_quantity']
            m_abs_pins_rec_lst.append(new_item)

    def eval_pt_cd(self):
        """根据考勤评估规则，将考勤项目转换为薪资项目"""

        self.set_catalog_segment()
        self.merge_abs_items()

        db = self.db
        catalog = self.catalog

        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        pt_period_end = self.pt_period_end

        # 采用考勤周期薪资标准（标识）
        vr_pg_ptpy = gv.get_var_value('VR_PG_PTPY')

        m_abs_pins_rec_lst = self.m_abs_pins_rec_lst
        abs_map_data_dic = self.abs_map_data_dic
        for emp_abs_item in m_abs_pins_rec_lst:
            abs_cd = emp_abs_item['abs_cd']
            option = emp_abs_item['option']
            if abs_cd not in abs_map_data_dic:
                continue
            pin_data_dic = abs_map_data_dic[abs_cd]
            pin_cd = pin_data_dic['pin_cd']
            pin_obj = gv.get_pin(pin_cd)
            if pin_obj.prcs_flag != '':
                continue
            add_fc_log_item(self.fc_obj, 'WT', pin_cd)

            """ 获取对应薪资项目的属性，获取对应评估基准、月中入离职变量、倍数使用的变量、计算分母 """
            denominator_val = 0
            multi_var_val = 0
            entry_leave_val = ''

            if vr_pg_ptpy == 'Y':
                """ 根据“考勤结束日期”获取薪资项目属性 """
                sql_pin = text(
                    "select a.hhr_segment_flag from boogoo_payroll.hhr_py_pin a where a.tenant_id =:b1 and a.hhr_pin_cd =:b2 "
                    "and a.hhr_efft_date<= :b3 order by hhr_efft_date desc")
                r = db.conn.execute(sql_pin, b1=tenant_id, b2=pin_cd, b3=pt_period_end).fetchone()
                if r is None:
                    continue
                segment_flag = r['hhr_segment_flag']
            else:
                # 若VR_PG_PTPY的值<>Y，直接根据历经期结束日期获取薪资项目属性(亦即当前处理的历经期期间);
                segment_flag = pin_obj.seg_flag

            if option == 'C':
                denominator_val, multi_var_val, entry_leave_val = self.get_cur_prd_value(emp_abs_item, pin_data_dic)
                self.calc_abs_pin_amt('C', pin_obj, emp_abs_item, pin_data_dic, segment_flag, denominator_val,
                                      multi_var_val, entry_leave_val)
            elif option == 'H':
                # 分母(A-工作日(含法定)，B-工作日(不含法定)，C-日历日，D-固定天数)
                denominator = pin_data_dic['denominator']
                # 倍数类型(VR-变量，FX-固定值)
                multi_type = pin_data_dic['multi_type']
                # 倍数变量
                multi_var = pin_data_dic['multi_var']
                add_fc_log_item(self.fc_obj, 'VR', multi_var)
                # 倍数固定值
                multi_fixed_val = pin_data_dic['multi_fixed_val']
                # 固定天数
                fixed_days = pin_data_dic['fixed_days']

                catalog_seq = emp_abs_item['catalog_seq']
                cal_s_days = emp_abs_item['cal_s_days']
                work_s_days = emp_abs_item['work_s_days']
                legal_s_days = emp_abs_item['legal_s_days']

                if denominator == 'A':
                    denominator_val = work_s_days + legal_s_days
                elif denominator == 'B':
                    denominator_val = work_s_days
                elif denominator == 'C':
                    denominator_val = cal_s_days
                elif denominator == 'D':
                    denominator_val = fixed_days

                if multi_type == 'VR':
                    sql_var = text(
                        "select hhr_varval_dec from boogoo_payroll.hhr_py_rslt_var where tenant_id = :b1 and hhr_empid = :b2 "
                        "and hhr_emp_rcd = :b3 and hhr_seq_num = :b4 and hhr_variable_id = :b5 ")
                    r3 = db.conn.execute(sql_var, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=catalog_seq,
                                         b5=multi_var).fetchone()
                    if r3 is not None:
                        multi_var_val = r3['hhr_varval_dec']
                elif multi_type == 'FX':
                    multi_var_val = multi_fixed_val

                sql_var = text(
                    "select hhr_varval_char from boogoo_payroll.hhr_py_rslt_var where tenant_id = :b1 and hhr_empid = :b2 "
                    "and hhr_emp_rcd = :b3 and hhr_seq_num = :b4 and hhr_variable_id = :b5 ")
                r4 = db.conn.execute(sql_var, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=catalog_seq,
                                     b5='VR_ENTRY_LEAVE').fetchone()
                if r4 is not None:
                    entry_leave_val = r4['hhr_varval_char']
                self.calc_abs_pin_amt('H', pin_obj, emp_abs_item, pin_data_dic, segment_flag, denominator_val,
                                      multi_var_val, entry_leave_val)
