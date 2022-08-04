from datetime import datetime
from .hr_rpt_data import run_per_trans


def run_pers_trans_rpt(run_param, conn):
    """
    Desc: 生成人事异动情况表
    Author: 陶雨
    Date: 2019/04/23
    """

    start_date = run_param['start_date']
    end_date = run_param['end_date']

    if start_date and isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        run_param['start_date'] = start_date
    if end_date and isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        run_param['end_date'] = end_date

    run_param['conn'] = conn

    run_per_trans(run_param)
