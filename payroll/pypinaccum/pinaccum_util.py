# coding:utf-8
"""
薪资项目累计工具类
create by wangling 2018/8/22
"""
from ..pysysutils import global_variables as gv
from sqlalchemy import text


def get_accumulate_value(**kwargs):
    """
    create by wangling 2018/8/22
    获取薪资项目累计值
    :param kwargs: acc_obj：薪资项目累计对象
    :return:包含金额金额数量的元组(HHR_AMT，HHR_QUANTITY)
    """
    catalog = gv.get_run_var_value('PY_CATALOG')
    acc_obj = kwargs.get('acc_obj', None)
    acc_year_seq = kwargs.get('acc_year_seq', None)

    if acc_obj is not None:
        sql = text(
            "select a.hhr_seq_num,a.hhr_amt,a.hhr_quantity from boogoo_payroll.hhr_py_rslt_accm a where a.tenant_id = :b1 and a.hhr_empid = :b2 "
            "and a.hhr_emp_rcd = :b3 and a.hhr_acc_cd = :b4 and a.hhr_accm_type = :b5 and a.hhr_period_add_year = :b6 "
            "and a.hhr_period_add_number = :b7 and a.hhr_seq_num = :b8 order by a.hhr_seq_num desc")
        result = gv.get_db().conn.execute(sql, b1=catalog.tenant_id, b2=catalog.emp_id, b3=catalog.emp_rcd,
                                          b4=acc_obj.acc_code,
                                          b5=acc_obj.acc_type, b6=acc_year_seq[0], b7=acc_year_seq[1], b8=catalog.prev_seq).fetchone()

        if result is not None:
            if result['hhr_seq_num'] < catalog.seq_num:
                return result['hhr_amt'], result['hhr_quantity']
        else:
            return 0, 0



