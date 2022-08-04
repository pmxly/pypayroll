# coding:utf-8
"""
此函数用于初始化系统变量
"""

from ..pysysutils import global_variables as gv
from ..pyvariables.var_util.varutil import get_dev_variable_dic
# from ..pyvariables.__init__ import __all__ as sys_vr_list
# from ..pyfunctions.function_object import FunctionObject
from ..pyvariables.variable_object import VariableObject
# from sqlalchemy import select
from sqlalchemy import text
# from importlib import import_module
# from ..pypins.pin import Pin
# from copy import deepcopy
from ..pyexecute.pycalculate.c_emp_entity import EmpEntity


class InitVariables:

    def __init__(self):
        self._db = gv.get_db()

    # @staticmethod
    # def init_sys_variable_dynamic(value, module_name):
    #     """设置系统变量值的通用方法
    #         value      : 变量值
    #         module_name：变量的模块名"""
    #     module_meta = import_module('hhr.payroll.pyvariables.' + module_name)
    #     class_meta = getattr(module_meta, 'SysVariable')
    #     var_object = class_meta()
    #     # var_object.value = value
    #     gv.set_val_object(var_object.id, var_object)

    def init_all_variables(self, tenant_id):
        """
        初始化所有变量
        :param tenant_id:租户ID
        :return:
        """

        # 初始化dev变量
        run_var_dic = get_dev_variable_dic()
        for k in run_var_dic.keys():
            if run_var_dic[k][1] == 'STRING':
                gv.set_run_var_obj(k, run_var_dic[k][0], '')
            elif run_var_dic[k][1] == 'NUMBER':
                gv.set_run_var_obj(k, run_var_dic[k][0], 0)
            elif run_var_dic[k][1] == 'OBJECT':
                gv.set_run_var_obj(k, run_var_dic[k][0], object())
            elif run_var_dic[k][1] == 'DATE':
                gv.set_run_var_obj(k, run_var_dic[k][0], None)
            elif run_var_dic[k][1] == 'LIST':
                gv.set_run_var_obj(k, run_var_dic[k][0], list())
            elif run_var_dic[k][1] == 'DICT':
                gv.set_run_var_obj(k, run_var_dic[k][0], dict())

        # 初始化SYS
        # for sys_vr in sys_vr_list:
            # self.init_sys_variable_dynamic(None, sys_vr)

        # 初始化SYS变量和CUM变量
        calendar_group_obj = gv.get_run_var_value('CAL_GRP_OBJ')
        grp_country = calendar_group_obj.country
        sql = text("select a.tenant_id,a.hhr_variable_id,a.hhr_description,a.hhr_data_type,a.hhr_country,a.hhr_cover_enable,a.hhr_edit_text, "
                   "a.hhr_var_type from boogoo_payroll.hhr_py_variable a where a.tenant_id=:b1 and a.hhr_status = 'Y' and (hhr_country = 'ALL' or hhr_country = :b2 ) ")
        result = self._db.conn.execute(sql, b1=tenant_id, b2=grp_country).fetchall()
        for result_line in result:
            var_id = result_line['hhr_variable_id']
            # var_type = result_line['HHR_VAR_TYPE']
            gv.set_val_object(var_id, VariableObject('', '', result_line))

            # if var_type == 'C':
            #     gv.set_val_object(var_id, VariableObject('', '', result_line))
            # elif var_type == 'S':
            #     module_meta = import_module('hhr.payroll.pyvariables.' + var_id)
            #     class_meta = getattr(module_meta, 'SysVariable')
            #     var_object = class_meta('', '', result_line)
            #     gv.set_val_object(var_object.id, var_object)

    def init_all_vars_by_period(self, catalog):
        """
        按单个人员单个期间初始化所有变量
        :param tenant_id:租户ID
        :return:
        """

        tenant_id = catalog.tenant_id
        country = catalog.f_country

        # 获取所有按期间初始化的变量
        sql = text("select a.tenant_id,a.hhr_variable_id,a.hhr_description,a.hhr_data_type,a.hhr_country,a.hhr_cover_enable,a.hhr_edit_text, "
                   "a.hhr_var_type from boogoo_payroll.hhr_py_variable a where a.tenant_id=:b1 and a.hhr_status = 'Y' "
                   "and (hhr_country = 'ALL' or hhr_country = :b2 ) and a.hhr_var_type = 'P' ")
        result = self._db.conn.execute(sql, b1=tenant_id, b2=country).fetchall()
        for result_line in result:
            var_id = result_line['hhr_variable_id']
            gv.set_val_object(var_id, VariableObject('', '', result_line))

    @staticmethod
    def init_cal_grp_variable(calendar_group_obj):
        """
        初始化日历组相关变量，每次算薪只初始化换一次
        :param calendar_group_obj:日历组对象 payroll\pycalendar\calender.py
        :return:
        """
        # gv.set_run_var_value('CAL_OBJ_DIC', deepcopy(calendar_group_obj.calendar_dic))

        # 根据运行类型、日历组和薪资组获取所有需要追溯的日历
        retro_cal_lst = []
        for cal_id, cal in calendar_group_obj.calendar_dic.items():
            if cal.run_type_entity.cycle == 'C':
                if calendar_group_obj.retro == 'Y' and cal.py_group_entity.retro_flag == 'Y':
                    retro_cal_lst.append(cal_id)
        gv.set_run_var_value('RETRO_CAL_LST', retro_cal_lst)

    # @log
    @staticmethod
    def init_cur_cal_variable(catalog):
        """
        初始化历经期日历相关变量
        :param catalog:薪资计算目录
        :return:
        """
        # (历经期)国家/地区
        gv.set_var_value('VR_F_COUNTRY', catalog.f_country)
        # (历经期)薪资组
        gv.set_var_value('VR_F_PYGROUP', catalog.f_pygrp_id)
        # (历经期)运行类型
        gv.set_var_value('VR_F_RUNTYPE', catalog.f_runtype_id)
        # (历经期)期间编码
        gv.set_var_value('VR_F_PERIOD', catalog.f_period_cd)
        # (历经期)期间年度
        gv.set_var_value('VR_F_PERIOD_YEAR', catalog.f_period_year)
        # (历经期)期间序号
        gv.set_var_value('VR_F_PERIOD_NO', catalog.f_prd_num)
        # (历经期)支付日期
        gv.set_var_value('VR_F_PAYDATE', catalog.f_pay_date)
        # (历经期)计算类型
        gv.set_var_value('VR_F_CALTYPE', catalog.f_cal_type)
        # (历经期)开始日期
        gv.set_var_value('VR_F_PERIOD_BEG', catalog.f_prd_bgn_dt)
        # (历经期)结束日期
        gv.set_var_value('VR_F_PERIOD_END', catalog.f_prd_end_dt)
        # (历经期)运行类型性质
        gv.set_var_value('VR_F_RUNTYPE_C', catalog.f_rt_cycle)
        # (历经期)期间频率
        # gv.set_var_value('VR_F_PERIOD_FREQ', '')

    # @log
    @staticmethod
    def init_cal_variable(calendar_obj):
        """
        初始化所在期日历相关变量
        :param calendar_obj:日历对象 payroll\pycalendar\calender.py
        :return:
        """
        # 所在期间薪资组
        gv.set_var_value('VR_PYGROUP', calendar_obj.py_group_id)
        # 所在期间运行类型
        gv.set_var_value('VR_RUNTYPE', calendar_obj.run_type_id)
        # 所在期间期间编码
        gv.set_var_value('VR_PERIOD', calendar_obj.period_id)
        # 所在期间年度
        gv.set_var_value('VR_PERIOD_YEAR', calendar_obj.period_year)
        # 所在期间序号
        gv.set_var_value('VR_PERIOD_NO', calendar_obj.period_num)
        # 所在期间支付日期
        gv.set_var_value('VR_PAYDATE', calendar_obj.pay_date)
        # 所在期间计算类型
        gv.set_var_value('VR_CALTYPE', calendar_obj.pay_cal_type)
        # 所在期间期间开始日期
        gv.set_var_value('VR_PERIOD_BEG', calendar_obj.period_cal_entity.start_date)
        # 所在期间期间结束日期
        gv.set_var_value('VR_PERIOD_END', calendar_obj.period_cal_entity.end_date)
        # 所在期间运行类型性质
        gv.set_var_value('VR_RUNTYPE_C', calendar_obj.run_type_entity.cycle)

    @staticmethod
    def init_cur_py_group_var(cal_obj):
        """
        初始化历经期薪资组对象相关变量
        :param cal_obj:日历对象 payroll\pycalendar\calender.py
        :return:
        """

        # 薪资组分段规则
        gv.set_var_value('VR_PG_SEG_RULE', cal_obj.py_group_entity.seg_rule_cd)
        # 薪资组取整规则
        gv.set_var_value('VR_PG_ROUND', cal_obj.py_group_entity.round_rule)
        # 薪资组工作计划
        gv.set_var_value('VR_PG_SCHEDULE', cal_obj.py_group_entity.work_plan)
        # 薪资组货币
        gv.set_var_value('VR_PG_CURRENCY', cal_obj.py_group_entity.currency)

        # 考勤评估规则
        gv.set_var_value('VR_PG_PTRULE', cal_obj.py_group_entity.abs_eval_rule)
        # 考勤期间
        gv.set_var_value('VR_PG_PTPERIOD', cal_obj.py_group_entity.abs_period)
        # 差异数
        gv.set_var_value('VR_PG_NONUM', cal_obj.py_group_entity.diff_num)
        # 采用考勤周期薪资标准（标识）
        gv.set_var_value('VR_PG_PTPY', cal_obj.py_group_entity.abs_cycle_std_sw)

    @staticmethod
    def init_job_seg_var():
        """初始化任职分段相关变量"""

        gv.set_var_value('VR_SECTION', None)                # 分段号
        gv.set_var_value('VR_ACTION', None)                 # 操作
        gv.set_var_value('VR_ACREASON', None)               # 原因
        gv.set_var_value('VR_RELATION', None)               # 聘用关系
        gv.set_var_value('VR_HRSTATUS', None)               # 人员状态
        gv.set_var_value('VR_EMPCLASS', None)               # 人员类别
        gv.set_var_value('VR_EMPSUBCLASS', None)            # 人员子类别
        gv.set_var_value('VR_COMPANY', None)                # 公司
        gv.set_var_value('VR_POSITION', None)               # 职位
        gv.set_var_value('VR_DEPARTMENT', None)             # 部门
        gv.set_var_value('VR_BU', None)                     # 业务单位
        gv.set_var_value('VR_LOCATION', None)               # 工作地点
        gv.set_var_value('VR_RPTPOST', None)                # 直接汇报职位
        gv.set_var_value('VR_INRPTPOST', None)              # 虚线汇报职位
        gv.set_var_value('VR_JOB', None)                    # 职务
        gv.set_var_value('VR_JOBGROUP', None)               # 族群
        gv.set_var_value('VR_JOBFAMILY', None)              # 序列
        gv.set_var_value('VR_JOBGRADE', None)               # 职级
        gv.set_var_value('VR_JOBPYGROUP', None)             # 任职薪资组
        gv.set_var_value('VR_JOBSCOPE', None)               # 任职适用范围
        gv.set_var_value('VR_JOBTAXAREA', None)             # 任职纳税地
        gv.set_var_value('VR_JOBSIPHFID', None)             # 任职缴交社保公积金
        gv.set_var_value('VR_JOBPYPLAN', None)              # 任职薪资计划
        gv.set_var_value('VR_JOBPYGRADE', None)             # 任职薪等
        gv.set_var_value('VR_JOBPYSTEP', None)              # 任职薪级

    # @staticmethod
    # def select_all_cum_variable(tenant_id):
    #     """
    #     初始化所有变量
    #     :param tenant_id:系统ID
    #     :return:
    #     """
    #     db = gv.get_db()
    #     t = db.get_table('hhr_variable_b')
    #
    #     stmt = select([t.c.HHR_SYS_CODE, t.c.HHR_VARIABLE_ID, t.c.HHR_DESCRIPTION, t.c.HHR_DATA_TYPE, t.c.HHR_COUNTRY,
    #                    t.c.HHR_COVER_ENABLE, t.c.HHR_EDIT_TEXT, t.c.HHR_VAR_TYPE]).where(
    #         t.c.HHR_SYS_CODE == tenant_id)
    #
    #     result = db.conn.execute(stmt).fetchall()
    #     if result is not None:
    #         for result_line in result:
    #             var_object = VariableObject(tenant_id, result_line['HHR_VARIABLE_ID'], result_line)
    #             gv.set_val_object(var_object.id, var_object)
    #     else:
    #         pass

    # @log
    @staticmethod
    def init_emp_variable(emp):
        """
        初始化员工相关变量，每循环一个员工，初始化一次
        :param emp:参数对象参数对象hhr.payroll.pyexecute.parameterentity.EmpParameter
        """
        # self.select_all_cum_variable(emp.tenant_id)

        emp_entity = EmpEntity(emp)
        gv.set_run_var_value('EMP_ENT', emp_entity)

        # 设置员工系统ID
        # gv.set_var_value('VR_SYS_ID', emp.tenant_id)
        # 设置员工ID
        gv.set_var_value('VR_EMP_ID', emp.emp_id)
        # 设置员工记录
        gv.set_var_value('VR_EMP_RCD', emp.emp_rcd)
        # 姓名
        gv.set_var_value('VR_EMP_NAME', emp_entity.name)
        # 性别
        gv.set_var_value('VR_GENDER', emp_entity.sex)
        # 国籍
        gv.set_var_value('VR_NATIO', emp_entity.national)
        # 身份证件类型
        gv.set_var_value('VR_IDTYPE', emp_entity.id_type)
        # 身份证件号码
        gv.set_var_value('VR_IDNO', emp_entity.id_no)
        # 出生日期
        gv.set_var_value('VR_BIRTHDATE', emp_entity.birthday)
        # 首次工作日期
        gv.set_var_value('VR_FIRSTWORKDATE', emp_entity.first_work_day)
        # 调整工龄扣除
        gv.set_var_value('VR_WORK_DEDUC', emp_entity.work_deduct)
        # 民族
        gv.set_var_value('VR_ETHNIC', emp_entity.ethnic)
        # 婚姻状况
        gv.set_var_value('VR_MARRIAGE', emp_entity.marriage)
        # 政治面貌
        gv.set_var_value('VR_POLITIC', emp_entity.politic)
        # 籍贯
        gv.set_var_value('VR_NATIVE', emp_entity.native)
        # 户口性质
        gv.set_var_value('VR_HUKOU', emp_entity.hukou)
        # 宗教信仰
        gv.set_var_value('VR_RELIGION', emp_entity.religion)
        # 残障人士
        gv.set_var_value('VR_DISABLED', emp_entity.disabled)

        # 首次雇佣日期(不再使用)
        # if emp_entity.first_hire_date is None:
        #     gv.set_var_value('VR_FIRSTDATE', emp_entity.hire_date)
        # else:
        #     gv.set_var_value('VR_FIRSTDATE', emp_entity.first_hire_date)
        # 入职日期
        gv.set_var_value('VR_ENTRYDATE', emp_entity.hire_date)
        # 最后工作日
        gv.set_var_value('VR_LASTDATE', emp_entity.last_date)
        # 离职日期
        gv.set_var_value('VR_LEAVEDATE', emp_entity.term_date)

        # 入司时间
        gv.set_var_value('VR_JOB_DATE01', emp_entity.job_date01)
        # 视同入司时间
        gv.set_var_value('VR_JOB_DATE02', emp_entity.job_date02)
        # 用工开始时间
        gv.set_var_value('VR_JOB_DATE03', emp_entity.job_date03)
        # 实习开始时间
        gv.set_var_value('VR_JOB_DATE04', emp_entity.job_date04)
        # 实习生转派遣工时间
        gv.set_var_value('VR_JOB_DATE05', emp_entity.job_date05)
        # 未被买断的兼并前单位入司时间
        gv.set_var_value('VR_JOB_DATE06', emp_entity.job_date06)
        # 日期07
        gv.set_var_value('VR_JOB_DATE07', emp_entity.job_date07)
        # 日期08
        gv.set_var_value('VR_JOB_DATE08', emp_entity.job_date08)
        # 日期09
        gv.set_var_value('VR_JOB_DATE09', emp_entity.job_date09)
        # 日期10
        gv.set_var_value('VR_JOB_DATE10', emp_entity.job_date10)

        # 初次雇佣日期
        gv.set_var_value('VR_ORIG_HIRE', emp_entity.orig_hire_date)
        # 是否覆盖初次雇佣
        gv.set_var_value('VR_COVER_ORIG', emp_entity.cover_orig_hire)
        # 集团司龄起算日期
        gv.set_var_value('VR_GROUP_BEGIN', emp_entity.group_begin_date)
        # 是否覆盖集团司龄起算
        gv.set_var_value('VR_COVER_GROUP', emp_entity.cover_group_begin)
        # 公司司龄起算日期
        gv.set_var_value('VR_COMP_BEGIN', emp_entity.comp_begin_date)
        # 是否覆盖公司司龄起算
        gv.set_var_value('VR_COVER_COMP', emp_entity.cover_comp_begin)
        # 调整公司司龄扣除
        gv.set_var_value('VR_COMP_DEDUCT', emp_entity.company_age_deduction)
        # 调整集团司龄扣除
        gv.set_var_value('VR_GROUP_DEDUCT', emp_entity.group_age_deduction)

    @staticmethod
    def init_var_by_period():
        """按期间初始化变量"""

        gv.set_var_value('VR_PSP', None)        # 工作日历类型
        gv.set_var_value('VR_TAX01_EE', None)   # 工资个人纳税标识
        gv.set_var_value('VR_TAX01_ER', None)   # 工资公司纳税标识
        gv.set_var_value('VR_TAX02_EE', None)   # 年终奖个人纳税标识
        gv.set_var_value('VR_TAX02_ER', None)   # 年终奖公司纳税标识
        gv.set_var_value('VR_TAX03_EE', None)   # 离职补偿金个人纳税标识
        gv.set_var_value('VR_TAX03_ER', None)   # 离职补偿金个人纳税标识
        gv.set_var_value('VR_TAX04_EE', None)   # 其他所得个人纳税标识
        gv.set_var_value('VR_TAX04_ER', None)   # 其他所得公司纳税标识
        gv.set_var_value('VR_TAXRATE04', None)  # 其他所得税率

        gv.set_var_value('VR_SCOPE', None)          # 适用范围
        gv.set_var_value('VR_TAXAREA', None)        # 纳税地
        gv.set_var_value('VR_SIPHFID', None)        # 缴交社保公积金
        gv.set_var_value('VR_PYPLAN', None)         # 薪资计划
        gv.set_var_value('VR_PYGRADE', None)        # 薪等
        gv.set_var_value('VR_PYSTEP', None)         # 薪级
        gv.set_var_value('VR_CAL_PTPY', None)       # 处理考勤结果

        gv.set_var_value('VR_ENTRY_LEAVE', None)    # 月中入离职标识

        gv.set_var_value('VR_RATE_SICK', None)      # 病假扣款比例
        gv.set_var_value('VR_RATE_LONGSICK', None)  # 长病假扣款比例

        gv.set_var_value('VR_TAXBASE_EE', 0)        # 应税金额(个人)
        gv.set_var_value('VR_TAXRATE_EE', 0)        # 税率(个人)
        gv.set_var_value('VR_TAXDEDUC_EE', 0)       # 速算扣除数(个人)
        gv.set_var_value('VR_TAXRATE01_EE', 0)      # 工资税率(个人)
        gv.set_var_value('VR_TAXDEDUC01_EE', 0)     # 工资速算扣除数(个人)
        gv.set_var_value('VR_TAXRATE02_EE', 0)      # 年终奖税率(个人)
        gv.set_var_value('VR_TAXDEDUC02_EE', 0)     # 年终奖速算扣除数(个人)
        gv.set_var_value('VR_TAXRATE03_EE', 0)      # 离职补偿金税率(个人)
        gv.set_var_value('VR_TAXDEDUC03_EE', 0)     # 离职补偿金速算扣除数(个人)
        gv.set_var_value('VR_TAXBASE_ER', 0)        # 应税金额(公司)
        gv.set_var_value('VR_TAXRATE_ER', 0)        # 税率(公司)
        gv.set_var_value('VR_TAXDEDUC_ER', 0)       # 速算扣除数(公司)
        gv.set_var_value('VR_TAXRATE01_ER', 0)      # 工资税率(公司)
        gv.set_var_value('VR_TAXDEDUC01_ER', 0)     # 工资速算扣除数(公司)
        gv.set_var_value('VR_TAXRATE02_ER', 0)      # 年终奖税率(公司)
        gv.set_var_value('VR_TAXDEDUC02_ER', 0)     # 年终奖速算扣除数(公司)
        gv.set_var_value('VR_TAXRATE03_ER', 0)      # 离职补偿金税率(公司)
        gv.set_var_value('VR_TAXDEDUC03_ER', 0)     # 离职补偿金速算扣除数(公司)
        gv.set_var_value('VR_TAXTYPE', 0)           # 税类型
        gv.set_var_value('VR_TAXFREE', 0)           # 免税金额
        gv.set_var_value('VR_AGE', 0)               # 年龄
        gv.set_var_value('VR_WORKYEAR', 0)          # 工龄
        gv.set_var_value('VR_COMPYEAR', 0)          # 司龄

        # gv.set_var_value('VR_TAX_PRE', 0)           # 税务年度累计标识
        gv.set_var_value('VR_TAX_NEW', 0)           # 税务年度累计标识

        # 重新初始化所有客户化变量
        var_dic = gv.get_variable_dic()
        for val_obj in var_dic.values():
            # if val_obj.var_type == 'C':
            #     val_obj.value = None
            # 重置被覆盖过的变量
            val_obj.has_covered = 'N'
        # 清除薪资项目字典
        gv.clear_pin_dic()
        # 清除薪资项目累计字典
        gv.clear_pin_acc_dic()
