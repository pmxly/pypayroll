# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log, add_fc_log_item


class PyFunction(FunctionObject):
    """
    Desc: 获取任职属性函数
    Author: David
    Date: 2018/11/12
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_JOBDATA'
        self.country = 'CHN'
        self.desc = '获取任职属性函数'
        self.descENG = '获取任职属性函数'
        self.func_type = 'B'
        self.instructions = "获取任职属性函数。传入分段号"
        self.instructionsENG = self.instructions

        self.log_flag = gv.get_run_var_value('LOG_FLAG')
        if self.log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [],
                'VR': [],
                'PA': ['seg_num']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, seg_num=0):
        seg_info = gv.get_run_var_value('SEG_INFO_OBJ')

        if seg_num == 0:
            items_dic = seg_info.seg_items_dic['*']
            max_seg_num = 0
            for seg_num_key in items_dic.keys():
                max_seg_num = seg_num_key
            seg_num = max_seg_num

        job_seg_dic = seg_info.job_seg_dic
        if seg_num in job_seg_dic:
            job_seg = job_seg_dic[seg_num]
            gv.set_var_value('VR_ACTION', job_seg.action_cd)                # 操作
            gv.set_var_value('VR_ACREASON', job_seg.reason_cd)              # 原因
            gv.set_var_value('VR_RELATION', job_seg.relation)               # 聘用关系
            gv.set_var_value('VR_HRSTATUS', job_seg.hr_status)              # 人员状态
            gv.set_var_value('VR_EMPCLASS', job_seg.emp_class)              # 人员类别
            gv.set_var_value('VR_EMPSUBCLASS', job_seg.sub_emp_class)       # 人员子类别
            gv.set_var_value('VR_COMPANY', job_seg.company_cd)              # 公司
            gv.set_var_value('VR_POSITION', job_seg.position_cd)            # 职位
            gv.set_var_value('VR_DEPARTMENT', job_seg.dept_cd)              # 部门
            gv.set_var_value('VR_BU', job_seg.bu_cd)                        # 业务单位
            gv.set_var_value('VR_LOCATION', job_seg.location)               # 工作地点
            gv.set_var_value('VR_RPTPOST', job_seg.direct)                  # 直接汇报职位
            gv.set_var_value('VR_INRPTPOST', job_seg.indirect)              # 虚线汇报职位
            gv.set_var_value('VR_JOB', job_seg.job_cd)                      # 职务
            gv.set_var_value('VR_JOBGROUP', job_seg.group_cd)               # 族群
            gv.set_var_value('VR_JOBFAMILY', job_seg.sequence_cd)           # 序列
            gv.set_var_value('VR_JOBGRADE', job_seg.rank_cd)                # 职级

            gv.set_var_value('VR_JOBDATA01', job_seg.job_data_attr01)       # 人员类别
            gv.set_var_value('VR_JOBDATA03', job_seg.job_data_attr03)       # 是否大专班
            gv.set_var_value('VR_JOBDATA09', job_seg.job_data_attr09)       # 员工组
            gv.set_var_value('VR_JOBDATA10', job_seg.job_data_attr10)       # 员工子组
            gv.set_var_value('VR_JOBTYPE', job_seg.job_type_cd)             # 职种

            # gv.set_var_value('VR_JOBPYGROUP', job_seg.py_group_id)          # 任职薪资组

            gv.set_var_value('VR_SCOPE', job_seg.apply_scope)            # 任职适用范围
            gv.set_var_value('VR_TAXAREA', job_seg.tax_location)         # 任职纳税地
            gv.set_var_value('VR_SIPHFID', job_seg.maintain_si_phf)      # 任职缴交社保公积金
            gv.set_var_value('VR_CAL_PTPY', job_seg.prcs_pt_rslt)        # 处理考勤结果
            gv.set_var_value('VR_RESIDENT_TYPE', job_seg.resident_type)  # 居民类型
            gv.set_var_value('VR_TAX_TYPE', job_seg.tax_type)            # 税类型
            gv.set_var_value('VR_TAX_MAP', job_seg.tax_year_mapping)     # 税务年度映射
            gv.set_var_value('VR_PY_METHOD', job_seg.pay_method)         # 发薪方式
            gv.set_var_value('VR_PY_GRADE', job_seg.per_rank)            # 个人职级
            gv.set_var_value('VR_PY_CURVE', job_seg.py_curve_type)       # 薪酬曲线
            gv.set_var_value('VR_PYPLAN', job_seg.sal_plan_cd)           # 薪资计划
            gv.set_var_value('VR_PYGRADE', job_seg.sal_grade_cd)         # 薪等
            gv.set_var_value('VR_PYSTEP', job_seg.sal_step_cd)           # 薪级

        if self.log_flag == 'Y':
            # vr_lst = ['VR_ACTION', 'VR_ACREASON', 'VR_RELATION', 'VR_HRSTATUS', 'VR_EMPCLASS', 'VR_EMPSUBCLASS',
            #           'VR_COMPANY', 'VR_POSITION', 'VR_DEPARTMENT', 'VR_BU', 'VR_LOCATION', 'VR_RPTPOST',
            #           'VR_INRPTPOST', 'VR_JOB', 'VR_JOBGROUP', 'VR_JOBFAMILY', 'VR_JOBGRADE', 'VR_JOBPYGROUP',
            #           'VR_JOBSCOPE', 'VR_JOBTAXAREA', 'VR_JOBSIPHFID', 'VR_JOBPYPLAN', 'VR_JOBPYGRADE', 'VR_JOBPYSTEP']

            vr_lst = ['VR_ACTION', 'VR_ACREASON', 'VR_RELATION', 'VR_HRSTATUS', 'VR_EMPCLASS', 'VR_EMPSUBCLASS',
                      'VR_COMPANY', 'VR_POSITION', 'VR_DEPARTMENT', 'VR_BU', 'VR_LOCATION', 'VR_RPTPOST',
                      'VR_INRPTPOST', 'VR_JOB', 'VR_JOBGROUP', 'VR_JOBFAMILY', 'VR_JOBGRADE',
                      'VR_JOBDATA01', 'VR_JOBDATA03', 'VR_JOBDATA09', 'VR_JOBDATA10', 'VR_JOBTYPE',
                      'VR_SCOPE', 'VR_TAXAREA', 'VR_SIPHFID', 'VR_CAL_PTPY', 'VR_RESIDENT_TYPE', 'VR_TAX_TYPE', 'VR_TAX_MAP', 'VR_PY_METHOD',
                      'VR_PY_GRADE', 'VR_PY_CURVE', 'VR_PYPLAN', 'VR_PYGRADE', 'VR_PYSTEP']
            add_fc_log_item(self, 'VR', '', vr_lst)
