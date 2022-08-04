"""
变量工具方法
created by wangling 2018/9/7
"""

from ...pysysutils import global_variables as gv
from ...pyexecute.pycalculate.table.var import Var


def get_dev_variable_dic():
    """
    获取包含所有开发变量的字典
    key：变量名
    value:变量描述
    :return:
    包含所有开发变量的字典
    key：变量名
    tuple[0]:变量描述
    tuple[1]:初始化类型
            STRING:
            OBJECT:
            DICT:
            LIST:

    """

    dev_var_dic = dict()

    dev_var_dic['IDENTIFY_EMP_FLAG'] = ('受款人标志，Y：标识，N：不做标识受款人操作', 'STRING')
    dev_var_dic['PY_CALCULATE_FAG'] = ('薪资计算标识：Y：计算，N：不做薪资计算操作', 'STRING')
    dev_var_dic['CANCEL_CALCULATE_FLAG'] = ('受款人标志，Y：标识，N：不做标识受款人操作', 'STRING')
    dev_var_dic['COMPLETE_FLAG'] = ('完成计算标识：Y：完成计算，N：不做完成计算操作', 'STRING')
    dev_var_dic['LOG_FLAG'] = ('单个人员打开日志标识：Y：记录计算日志，N：不记录日志', 'STRING')
    dev_var_dic['COMM_LOG_FLAG'] = ('总日志标识开关：Y：记录计算日志，N：不记录日志', 'STRING')
    dev_var_dic['PRE_LOG_FLAG'] = ('上一个人员打开日志标识：Y：记录计算日志，N：不记录日志', 'STRING')
    dev_var_dic['LOGGER'] = ('日志对象', 'OBJECT')
    # dev_var_dic['CAL_GRP_OBJ'] = ('所在期日历组', 'OBJECT')
    dev_var_dic['CAL_OBJ'] = ('所在其日历对象', 'OBJECT')
    dev_var_dic['CUR_CAL_OBJ'] = ('历经期日历对象', 'OBJECT')
    dev_var_dic['CAL_OBJ_DIC'] = ('所有已经实例化的日历实例', 'OBJECT')
    dev_var_dic['CUR_CALCULATE_TYPE'] = ('当前计算类型R：追溯，N：正常计算，C：更正', 'STRING')
    dev_var_dic['RETRO_FLAG'] = ('当前计算是否考虑追溯，Y:需要考虑追溯，N：不考虑追溯', 'STRING')
    dev_var_dic['RETRO_MIN'] = ('最小追溯日期', 'DATE')
    dev_var_dic['RETRO_ACT'] = ('实际追溯日期', 'DATE')
    dev_var_dic['RUN_PARM'] = ('运行参数对象,hhr.payroll.pyexecute.parameterentity.RunParameter', 'OBJECT')
    dev_var_dic['PY_CATALOG'] = ('薪资计算当前目录', 'OBJECT')
    dev_var_dic['RETRO_CATALOG_LIST'] = ('追溯的目录', 'LIST')
    dev_var_dic['SEG_INFO_OBJ'] = ('分段信息对象', 'OBJECT')
    dev_var_dic['PREV_SEG_INFO_OBJ'] = ('上一期间分段信息对象', 'OBJECT')
    dev_var_dic['TASK_ID'] = ('任务ID', 'STRING')
    dev_var_dic['RETRO_CAL_LST'] = ('需要追溯的日历', 'LIST')
    dev_var_dic['EMP_ENT'] = ('员工实体对象', 'OBJECT')
    dev_var_dic['ACC_CAL_DIC'] = ('历经期累计日历字典', 'DICT')
    dev_var_dic['EX_RATE_DIC'] = ('历经期汇率字典', 'DICT')
    dev_var_dic['GENERAL_PINS_DIC'] = ('历经期通用系统薪资项目字典', 'DICT')
    dev_var_dic['SCOPE_PINS_DIC'] = ('历经期适用范围薪资项目字典', 'DICT')
    dev_var_dic['SCOPE_AGENCY_DIC'] = ('历经期适用范围其他信息字段', 'DICT')
    dev_var_dic['CAL_ALL_PIN_DIC'] = ('每个日历对应一套初始化的薪资项目', 'DICT')
    dev_var_dic['PIN_PROCESS_OBJ'] = ('薪资项目处理对象', 'OBJECT')
    dev_var_dic['PT_PROCESS_OBJ'] = ('考勤处理对象', 'OBJECT')
    dev_var_dic['PT_PIN_DIC'] = ('考勤项目与薪资项目的对应关系字典', 'DICT')
    dev_var_dic['ALL_ACCUMULATE_DIC'] = ('所有被实例化过的薪资项目累计', 'DICT')
    dev_var_dic['ALL_FORMULA_DIC'] = ('按历经期所有初始化过的公式', 'DICT')
    dev_var_dic['CONTRIB_RULE_DIC'] = ('历经期缴纳规则字典', 'DICT')
    dev_var_dic['INSURANCE_PINS_DIC'] = ('全局保险类型对应的薪资项目字典', 'DICT')
    dev_var_dic['AREA_SAL_LVL_DIC'] = ('历经期缴纳地工资水平字典', 'DICT')
    dev_var_dic['PHF_TAX_DIC'] = ('历经期公积金超额纳税规则字典', 'DICT')
    dev_var_dic['SEG_RULES_DIC'] = ('历经期内所有有效的分段规则', 'DICT')
    dev_var_dic['RUN_TYPE_DIC'] = ('全局运行类型字典', 'DICT')
    dev_var_dic['BRACKET_DIC'] = ('分级字典', 'DICT')
    dev_var_dic['BR_TAX_EE_DESC_DIC'] = ('历经期内个人税率降序排列字典', 'DICT')
    dev_var_dic['PRD_CAL_DIC'] = ('期间日历字典', 'DICT')
    dev_var_dic['LOG_TREE_NEXT_NUM'] = ('过程日志树下一节点编号', 'NUMBER')
    dev_var_dic['LOG_TREE_PARENT_NUM'] = ('过程日志树当前父节点编号', 'NUMBER')
    dev_var_dic['LOG_TREE_CAL_NUM'] = ('过程日志树当前日历节点编号', 'NUMBER')
    dev_var_dic['LOG_TREE_NODE_LIST'] = ('过程日志树节点列表', 'LIST')
    dev_var_dic['PRD_YEAR_SEQ_DIC'] = ('期间年度序号字典', 'DICT')
    dev_var_dic['PRD_CAL_DATE_DIC'] = ('期间日期日历字典', 'DICT')
    dev_var_dic['UPD_CAL_ID'] = ('被更正日历ID', 'STRING')
    dev_var_dic['TENANT_ID'] = ('租户ID', 'NUMBER')
    dev_var_dic['POSN_ASSOC'] = ('岗位关联信息', 'DICT')
    dev_var_dic['TREE_EFFT_DIC'] = ('部门树生效日期', 'DICT')
    dev_var_dic['FLD_ASSO_INFO_DIC'] = ('字段部门关系信息', 'DICT')
    dev_var_dic['MAIN_COST_DAY'] = ('薪资过账-主成本日期', 'NUMBER')
    dev_var_dic['POSN_JOB_DIC'] = ('岗位职位信息', 'DICT')
    dev_var_dic['ATT_TYPE_DATA'] = ('视同出勤补贴考勤项目', 'DICT')
    dev_var_dic['ABS_PTCODE_DATA'] = ('缺勤考勤项目', 'DICT')
    dev_var_dic['CONSTANT_STD_DATA'] = ('固定标准', 'DICT')

    return dev_var_dic


def persist_vars_data():
    """将变量写入到变量结果表中"""

    catalog = gv.get_run_var_value('PY_CATALOG')
    tenant_id = catalog.tenant_id
    emp_id = catalog.emp_id
    emp_rcd = catalog.emp_rcd
    seq_num = catalog.seq_num
    var_dic = gv.get_variable_dic()

    var_val_char = ''
    var_dt = None
    var_val_dec = 0

    # trans = gv.get_db().conn.begin() 2021-02-02
    for var_id, var_obj in var_dic.items():
        var_type = var_obj.data_type
        if var_type == 'datetime':
            var_val_char = ''
            var_dt = var_obj.value
            var_val_dec = 0
        elif var_type == 'float':
            var_val_char = ''
            var_dt = None
            var_val_dec = var_obj.value
        elif var_type == 'string':
            var_val_char = var_obj.value
            var_dt = None
            var_val_dec = 0

        if var_obj.has_covered == 'Y':
            prc_flag = 'C'
            # 重置标识
            var_obj.has_covered = 'N'
        else:
            prc_flag = ''

        # 不存储值为空且没有被覆盖过的变量
        if (not var_obj.value) and (prc_flag == ''):
            continue
        v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                 'var_id': var_id, 'var_type': var_type, 'var_val_char': var_val_char,
                 'var_dt': var_dt, 'var_val_dec': var_val_dec, 'prc_flag': prc_flag}
        Var(**v_dic).insert()
    # trans.commit() 2021-02-02
