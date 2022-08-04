# coding:utf-8
"""
create by wangling 2018/8/13
    薪资计算参数对象
"""


class RunParameter:
    """
    create by wangling 2018/8/7
    薪资计算参数对象
    """

    def __init__(self):
        # 数据库连接db
        self.db = None
        # 运行控制ID
        self.run_ctl_id = ''
        # 进程ID
        self.task_id = ''
        # 进程名
        self.task_name = ''
        # 租户ID
        self.tenant_id = ''
        # 日历组ID
        self.cal_grp_id = ''
        # 日历ID
        self.cal_id = ''

        self.identify_emp_flag = ''   # 标记人员标识，Y：标识，N：不做标识受款人操作
        self.re_iden_flag = ''        # 重新标记标识：Y：重新标记，N：不做重新标记操作
        self.py_calculate_flag = ''   # 薪资计算标识：Y：计算，N：不做薪资计算操作
        self.re_calc_flag = ''        # 重新计算标识：Y：重新计算，N：不做重新计算操作
        self.re_iden_calc_flag = ''   # 重新标记&计算：Y：重新标记&计算，N：不做重新标记&计算操作
        self.cancel_iden_flag = ''    # 取消标记：Y：取消，N：不做取消操作
        self.cancel_calc_flag = ''    # 取消计算：Y：取消，N：不做取消操作
        self.cancel_flag = ''         # 取消标识&计算：Y：取消，N：不做取消操作
        self.finish_flag = ''         # 完成计算标识：Y：完成计算，N：不做完成计算操作
        self.close_flag = ''          # 关闭计算标识：Y：关闭计算，N：不做关闭计算操作
        self.log_flag = ''            # 打开日志标识：Y：记录计算日志，N：不记录日志
        self.operator_user_id = ''    # 发起计算的用户ID
        self.payees = []              # 需要处理的员工列表（EmpParameter对象列表）


class EmpParameter:
    """
    create by wangling 2018/8/13
    员工对象参数
    """

    def __init__(self):
        # 租户ID
        self.tenant_id = ''
        # 日历组ID
        self.cal_grp_id = ''
        # 日历ID
        self.cal_id = ''
        # 更正日历ID
        self.upd_cal_id = ''
        # 员工id
        self.emp_id = ''
        # 员工记录
        self.emp_rcd = ''
