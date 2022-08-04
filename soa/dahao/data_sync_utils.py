# -*- coding: utf-8 -*-

from suds.client import Client
from suds.transport.http import HttpAuthenticated
from sqlalchemy import text
from ...utils import get_current_dttm
from datetime import datetime
from uuid import uuid4

un = 'HR_USER'
pw = 'HR_USER'

# 部门
dept_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B12B4B91C94196/wsdl11/allinone/ws_policy/document?sap-client=800"
dept_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_16/800/zhr_16/zhr_16?sap-language=ZH"

# 职位
posn_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B1315A967FC197/wsdl11/allinone/ws_policy/document?sap-client=800"
posn_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_18/800/zhr_18/zhr_18?sap-language=ZH"

# 任职信息
job_wsld_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B12EBDADDC0196/wsdl11/allinone/ws_policy/document?sap-client=800"
job_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_17/800/zhr_17/zhr_17?sap-language=ZH"

# 职称信息
pos_title_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B158CE1C7821A4/wsdl11/allinone/ws_policy/document?sap-client=800"
pos_title_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_32/800/zhr_32/zhr_32?sap-language=ZH"

# 基本信息
bio_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B13A9C3E7C8198/wsdl11/allinone/ws_policy/document?sap-client=800"
bio_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_21/800/zhr_21/zhr_21?sap-language=ZH"

# 身份信息
iden_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B1489DF0EA019B/wsdl11/allinone/ws_policy/document?sap-client=800"
iden_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_26/800/zhr_26/zhr_26?sap-language=ZH"

# 地址信息
addr_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B133DB434AC197/wsdl11/allinone/ws_policy/document?sap-client=800"
addr_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_19/800/zhr_19/zhr_19?sap-language=ZH"

# 档案信息
arch_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B136D8A2E0C197/wsdl11/allinone/ws_policy/document?sap-client=800"
arch_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_20/800/zhr_20/zhr_20?sap-language=ZH"

# 证件信息
cert_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B13D188ACA8199/wsdl11/allinone/ws_policy/document?sap-client=800"
cert_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_22/800/zhr_22/zhr_22?sap-language=ZH"

# 试用期信息
prob_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B15B7505EEE1A4/wsdl11/allinone/ws_policy/document?sap-client=800"
prob_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_33/800/zhr_33/zhr_33?sap-language=ZH"

# 合同信息
cont_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B13F83781BC199/wsdl11/allinone/ws_policy/document?sap-client=800"
cont_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_23/800/zhr_23/zhr_23?sap-language=ZH"

# 协议信息
# prot_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B15ECBF49081A5/wsdl11/allinone/ws_policy/document?sap-client=800"
# prot_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_34/800/zhr_34/zhr_34"

# 证书信息
cred_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B1425E65DB819A/wsdl11/allinone/ws_policy/document?sap-client=800"
cred_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_24/800/zhr_24/zhr_24?sap-language=ZH"

# 绩效信息
perf_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B150FFF382019F/wsdl11/allinone/ws_policy/document?sap-client=800"
perf_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_29/800/zhr_29/zhr_29?sap-language=ZH"

# 培训信息
tran_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B1537759C641A0/wsdl11/allinone/ws_policy/document?sap-client=800"
tran_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_30/800/zhr_30/zhr_30?sap-language=ZH"

# 电话信息
phone_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B15624F65001A1/wsdl11/allinone/ws_policy/document?sap-client=800"
phone_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_31/800/zhr_31/zhr_31?sap-language=ZH"

# 邮箱信息
mail_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B14E407F87419C/wsdl11/allinone/ws_policy/document?sap-client=800"
mail_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_28/800/zhr_28/zhr_28?sap-language=ZH"

# 教育信息
edu_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B145D8D2DF019A/wsdl11/allinone/ws_policy/document?sap-client=800"
edu_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_25/800/zhr_25/zhr_25?sap-language=ZH"

# 奖惩信息
reward_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B1614EB1E0C1A7/wsdl11/allinone/ws_policy/document?sap-client=800"
reward_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_35/800/zhr_35/zhr_35?sap-language=ZH"

# 社会关系信息
rela_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B1642899F001A7/wsdl11/allinone/ws_policy/document?sap-client=800"
rela_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_36/800/zhr_36/zhr_36?sap-language=ZH"

# 工作经历信息
work_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B166A3054901A8/wsdl11/allinone/ws_policy/document?sap-client=800"
work_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_37/800/zhr_37/zhr_37?sap-language=ZH"

# 语言信息
lang_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B14B98B246819C/wsdl11/allinone/ws_policy/document?sap-client=800"
lang_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_27/800/zhr_27/zhr_27?sap-language=ZH"

# 银行信息
bank_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B1694B603901A8/wsdl11/allinone/ws_policy/document?sap-client=800"
bank_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_38/800/zhr_38/zhr_38?sap-language=ZH"

# 删除人员
del_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B16BBF8D8B01A9/wsdl11/allinone/ws_policy/document?sap-client=800"
del_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/zhr_39/800/zhr_39/zhr_39?sap-language=ZH"

# 删除单个人员单个信息类型
info_del_wsdl_url = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/wsdl/srvc_5CF3FCDD6BB01EE999B16E64C8C761AA/wsdl11/allinone/ws_policy/document?sap-client=800"
info_del_end_point = "http://ERPAP.DAHAO.NET:8001/sap/bc/srt/rfc/sap/ZHR_40/800/ZHR_40/ZHR_40?sap-language=ZH"


def get_last_call_dttm(conn, sys_code, tgt_sys_id, ifc_type):
    call_log_sel = text("select HHR_INTFC_CALL_DTTM from HHR_INTFC_CALL_LOG where HHR_SYS_CODE = :b1 "
                        "and HHR_TARGET_SYS_ID = :b2 and HHR_INTFC_TYPE = :b3 order by HHR_INTFC_CALL_DTTM desc ")
    call_log_row = conn.execute(call_log_sel, b1=sys_code, b2=tgt_sys_id, b3=ifc_type).fetchone()
    if call_log_row is not None:
        last_call_dttm = call_log_row['HHR_INTFC_CALL_DTTM']
    else:
        last_call_dttm = datetime(1970, 1, 1).date()
    return last_call_dttm


def get_soap_client(wsdl_url, end_point):
    client = Client(wsdl_url, transport=HttpAuthenticated(username=un, password=pw))
    client.set_options(location=end_point)
    return client


def insert_call_log(conn, uuid, sys_code, tgt_sys_id, intfc_type, call_dttm):
    """记录调用日志"""
    call_log_ins = text(
        "insert into HHR_INTFC_CALL_LOG(HHR_INTFC_UUID, HHR_SYS_CODE, HHR_TARGET_SYS_ID, HHR_INTFC_TYPE, HHR_INTFC_CALL_DTTM) "
        "values(:b1, :b2, :b3, :b4, :b5) ")
    conn.execute(call_log_ins, b1=uuid, b2=sys_code, b3=tgt_sys_id, b4=intfc_type, b5=call_dttm)


def get_last_error_rec(conn, sys_code, tgt_sys_id, ifc_type):
    """获取上次未处理成功的记录"""

    error_rec_sel = text(
        "select HHR_INTFC_UUID, HHR_INTFC_KEY1, HHR_INTFC_KEY2 from HHR_INTFC_PUSH_LOG where HHR_INTFC_PRCS_STAT = 'E' and HHR_SYS_CODE = :b1 "
        "and HHR_TARGET_SYS_ID = :b2 and HHR_INTFC_TYPE = :b3 ")
    error_rs = conn.execute(error_rec_sel, b1=sys_code, b2=tgt_sys_id, b3=ifc_type).fetchall()
    return error_rs


def insert_push_log(conn, **kwargs):
    """记录推送日志"""
    push_log_ins = text(
        "insert into HHR_INTFC_PUSH_LOG(HHR_INTFC_UUID, HHR_SYS_CODE, HHR_TARGET_SYS_ID, HHR_INTFC_TYPE, HHR_INTFC_KEY1, HHR_INTFC_KEY2, "
        "HHR_INTFC_KEY3, HHR_INTFC_KEY4, HHR_INTFC_KEY5, HHR_INTFC_CALL_DTTM, HHR_INTFC_PRCS_STAT, HHR_INTFC_PRCS_DTTM, HHR_MSG_TXT) "
        "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10, :b11, :b12, :b13) ")
    sys_cd = kwargs.get('sys_code', '')
    uuid = kwargs.get('uuid', '')
    tgt_sys_id = kwargs.get('tgt_sys_id', '')
    ifc_type = kwargs.get('ifc_type', '')
    ifc_key1 = kwargs.get('ifc_key1', '')
    ifc_key2 = kwargs.get('ifc_key2', '')
    ifc_key3 = kwargs.get('ifc_key3', '')
    ifc_key4 = kwargs.get('ifc_key4', '')
    ifc_key5 = kwargs.get('ifc_key5', '')
    ifc_call_dtm = kwargs.get('ifc_call_dtm', None)
    ifc_prc_stat = kwargs.get('ifc_prc_stat', None)
    ifc_prc_dtm = kwargs.get('ifc_prc_dtm', None)
    msg_txt = kwargs.get('msg_txt', None)
    conn.execute(push_log_ins, b1=uuid, b2=sys_cd, b3=tgt_sys_id, b4=ifc_type, b5=ifc_key1, b6=ifc_key2, b7=ifc_key3,
                 b8=ifc_key4, b9=ifc_key5, b10=ifc_call_dtm, b11=ifc_prc_stat, b12=ifc_prc_dtm, b13=msg_txt)


def update_push_log(conn, uuid, ifc_prc_stat, ifc_prc_dtm, msg_txt):
    """更新推送日志表"""
    push_log_upd = text(
        "update HHR_INTFC_PUSH_LOG set HHR_INTFC_PRCS_STAT = :b1, HHR_INTFC_PRCS_DTTM = :b2, HHR_MSG_TXT = :b3 "
        "where HHR_INTFC_UUID = :b4")
    conn.execute(push_log_upd, b1=ifc_prc_stat, b2=ifc_prc_dtm, b3=msg_txt, b4=uuid)


def update_ifc_push_stat(conn, uuid, resp, error_uuid_lst):
    msg_type = resp.HHR_MSG_TYPE
    msg_txt = None
    if msg_type == '100':
        ifc_prc_stat = 'S'
    elif msg_type == '200':
        ifc_prc_stat = 'E'
        msg_txt = resp.HHR_MSG_TEXT
    elif msg_type == '500':
        ifc_prc_stat = 'E'
        msg_txt = resp.HHR_MSG_TEXT
    else:
        ifc_prc_stat = 'E'

    cur_date = get_current_dttm()
    update_push_log(conn, uuid, ifc_prc_stat, cur_date, msg_txt)
    if len(error_uuid_lst) > 0:
        for err_uuid in error_uuid_lst:
            update_push_log(conn, err_uuid, ifc_prc_stat, cur_date, msg_txt)


def build_dept_method_param(param, rw):
    if rw is not None:
        eff_dt = rw['HHR_EFFE_DATE']
        d = {'AUDIT_ID': rw['AUDIT_ID'], 'AUDIT_TIMESTAMP': rw['AUDIT_TIMESTAMP'],
             'AUDIT_TRANSACTION_TYPE': rw['AUDIT_TRANSACTION_TYPE'],
             'HHR_DEPT_CODE': rw['HHR_DEPT_CODE'], 'HHR_EFFE_DATE': eff_dt, 'HHR_STATUS': rw['HHR_STATUS'],
             'HHR_DEPT_NAME': rw['HHR_DEPT_NAME'], 'HHR_DEPT_SHORT_NAME': rw['HHR_DEPT_SHORT_NAME'],
             'HHR_DEPT_DETAIL_DESC': rw['HHR_DEPT_DETAIL_DESC'],
             'HHR_DEPT_SORT': rw['HHR_DEPT_SORT'], 'HHR_DEPT_TYPE': rw['HHR_DEPT_TYPE'],
             'HHR_DEPT_LEVEL': rw['HHR_DEPT_LEVEL'],
             'HHR_DEPT_HIGHER_DEPT': rw['HHR_DEPT_HIGHER_DEPT'],
             'HHR_DEPT_MANAGER_POSITION': rw['HHR_DEPT_MANAGER_POSITION'],
             'HHR_ORG_DEPT_ATTR01': rw['HHR_ORG_DEPT_ATTR01'], 'HHR_ORG_DEPT_ATTR02': rw['HHR_ORG_DEPT_ATTR02'],
             'HHR_ORG_DEPT_ATTR03': rw['HHR_ORG_DEPT_ATTR03'], 'HHR_ORG_DEPT_ATTR04': rw['HHR_ORG_DEPT_ATTR04'],
             'HHR_ORG_DEPT_ATTR05': rw['HHR_ORG_DEPT_ATTR05'], 'HHR_ORG_DEPT_ATTR06': rw['HHR_ORG_DEPT_ATTR06'],
             'HHR_ORG_DEPT_ATTR07': rw['HHR_ORG_DEPT_ATTR07'], 'HHR_ORG_DEPT_ATTR08': rw['HHR_ORG_DEPT_ATTR08'],
             'HHR_ORG_DEPT_ATTR09': rw['HHR_ORG_DEPT_ATTR09'], 'HHR_ORG_DEPT_ATTR10': rw['HHR_ORG_DEPT_ATTR10'],
             'HHR_ORG_DEPT_ATTR11': rw['HHR_ORG_DEPT_ATTR11'],
             'HHR_ORG_DEPT_ATTR12': rw['HHR_ORG_DEPT_ATTR12'],
             'HHR_ORG_DEPT_ATTR13': rw['HHR_ORG_DEPT_ATTR13'], 'HHR_ORG_DEPT_ATTR14': rw['HHR_ORG_DEPT_ATTR14'],
             'HHR_ORG_DEPT_ATTR15': rw['HHR_ORG_DEPT_ATTR15'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '',
             'NUM': ''}
        param.item.append(d)
    return param


def build_posn_method_param(param, rw):
    if rw is not None:
        eff_dt = rw['HHR_EFFE_DATE']
        d = {'AUDIT_ID': rw['AUDIT_ID'], 'AUDIT_TIMESTAMP': rw['AUDIT_TIMESTAMP'],
             'AUDIT_TRANSACTION_TYPE': rw['AUDIT_TRANSACTION_TYPE'],
             'HHR_POSITION_CODE': rw['HHR_POSITION_CODE'], 'HHR_EFFE_DATE': eff_dt, 'HHR_STATUS': rw['HHR_STATUS'],
             'HHR_POSITION_NAME': rw['HHR_POSITION_NAME'], 'HHR_POSITION_SHORT_NAME': rw['HHR_POSITION_SHORT_NAME'],
             'HHR_POSN_DESCRIPTION': rw['HHR_POSN_DESCRIPTION'],
             'HHR_POSN_REQUIRE': rw['HHR_POSN_REQUIRE'], 'HHR_POSITION_SORT': rw['HHR_POSITION_SORT'],
             'HHR_DEPT_CODE': rw['HHR_DEPT_CODE'],
             'HHR_BU_CODE': rw['HHR_BU_CODE'],
             'HHR_POST_CODE': rw['HHR_POST_CODE'],
             'HHR_REPORT_POSITION': rw['HHR_REPORT_POSITION'], 'HHR_RPT_POSITION': rw['HHR_RPT_POSITION'],
             'HHR_LOCATION': rw['HHR_LOCATION'], 'HHR_ORG_POSN_ATTR01': rw['HHR_ORG_POSN_ATTR01'],
             'HHR_ORG_POSN_ATTR02': rw['HHR_ORG_POSN_ATTR02'], 'HHR_ORG_POSN_ATTR03': rw['HHR_ORG_POSN_ATTR03'],
             'HHR_ORG_POSN_ATTR04': rw['HHR_ORG_POSN_ATTR04'], 'HHR_ORG_POSN_ATTR05': rw['HHR_ORG_POSN_ATTR05'],
             'HHR_ORG_POSN_ATTR06': rw['HHR_ORG_POSN_ATTR06'], 'HHR_ORG_POSN_ATTR07': rw['HHR_ORG_POSN_ATTR07'],
             'HHR_ORG_POSN_ATTR08': rw['HHR_ORG_POSN_ATTR08'],
             'HHR_ORG_POSN_ATTR09': rw['HHR_ORG_POSN_ATTR09'],
             'HHR_ORG_POSN_ATTR10': rw['HHR_ORG_POSN_ATTR10'], 'HHR_ORG_POSN_ATTR11': rw['HHR_ORG_POSN_ATTR11'],
             'HHR_ORG_POSN_ATTR12': rw['HHR_ORG_POSN_ATTR12'], 'HHR_ORG_POSN_ATTR13': rw['HHR_ORG_POSN_ATTR13'],
             'HHR_ORG_POSN_ATTR14': rw['HHR_ORG_POSN_ATTR14'], 'HHR_ORG_POSN_ATTR15': rw['HHR_ORG_POSN_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_job_method_param(param, rw):
    if rw is not None:
        eff_dt = rw['HHR_EFFE_DATE']
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_EMP_RCD': rw['HHR_EMP_RCD'],
             'HHR_EFFE_DATE': eff_dt,
             'HHR_EFFDT_SEQ': rw['HHR_EFFDT_SEQ'], 'HHR_STATUS': rw['HHR_STATUS'],
             'HHR_ACTION_CODE': rw['HHR_ACTION_CODE'],
             'HHR_REASON_CODE': rw['HHR_REASON_CODE'], 'HHR_RELATION': rw['HHR_RELATION'],
             'HHR_POSITION_CODE': rw['HHR_POSITION_CODE'],
             'HHR_DEPT_CODE': rw['HHR_DEPT_CODE'], 'HHR_BU_CODE': rw['HHR_BU_CODE'],
             'HHR_COMPANY_CODE': rw['HHR_COMPANY_CODE'],
             'HHR_LOCATION': rw['HHR_LOCATION'],
             'HHR_COVER_LOC': rw['HHR_COVER_LOC'],
             'HHR_DIRECT': rw['HHR_DIRECT'], 'HHR_COVER_DIRECT': rw['HHR_COVER_DIRECT'],
             'HHR_INDIRECT': rw['HHR_INDIRECT'], 'HHR_COVER_INDIRECT': rw['HHR_COVER_INDIRECT'],
             'HHR_EMP_CLASS': rw['HHR_EMP_CLASS'], 'HHR_SUB_EMP_CLASS': rw['HHR_SUB_EMP_CLASS'],
             'HHR_POST_CODE': rw['HHR_POST_CODE'], 'HHR_RANK_CODE': rw['HHR_RANK_CODE'],
             'HHR_ORIG_HIRE_DATE': rw['HHR_ORIG_HIRE_DATE'], 'HHR_LAST_HIRE_DATE': rw['HHR_LAST_HIRE_DATE'],
             'HHR_LAST_DATE_WORKED': rw['HHR_LAST_DATE_WORKED'],
             'HHR_TERMINATION_DATE': rw['HHR_TERMINATION_DATE'],
             'HHR_HANDLE_LEAVE_DATE': rw['HHR_HANDLE_LEAVE_DATE'],
             'HHR_PERSONAL_MAIN_REASON': rw['HHR_PERSONAL_MAIN_REASON'],
             'HHR_COMPANY_MAIN_REASON': rw['HHR_COMPANY_MAIN_REASON'], 'HHR_LEAVE_DETAIL': rw['HHR_LEAVE_DETAIL'],
             'HHR_PER_JOBDATA_ATTR01': rw['HHR_PER_JOBDATA_ATTR01'],
             'HHR_PER_JOBDATA_ATTR02': rw['HHR_PER_JOBDATA_ATTR02'],
             'HHR_PER_JOBDATA_ATTR03': rw['HHR_PER_JOBDATA_ATTR03'],
             'HHR_PER_JOBDATA_ATTR04': rw['HHR_PER_JOBDATA_ATTR04'],
             'HHR_PER_JOBDATA_ATTR05': rw['HHR_PER_JOBDATA_ATTR05'],
             'HHR_PER_JOBDATA_ATTR06': rw['HHR_PER_JOBDATA_ATTR06'],
             'HHR_PER_JOBDATA_ATTR07': rw['HHR_PER_JOBDATA_ATTR07'],
             'HHR_PER_JOBDATA_ATTR08': rw['HHR_PER_JOBDATA_ATTR08'],
             'HHR_PER_JOBDATA_ATTR09': rw['HHR_PER_JOBDATA_ATTR09'],
             'HHR_PER_JOBDATA_ATTR10': rw['HHR_PER_JOBDATA_ATTR10'],
             'HHR_PER_JOBDATA_ATTR11': rw['HHR_PER_JOBDATA_ATTR11'],
             'HHR_PER_JOBDATA_ATTR12': rw['HHR_PER_JOBDATA_ATTR12'],
             'HHR_PER_JOBDATA_ATTR13': rw['HHR_PER_JOBDATA_ATTR13'],
             'HHR_PER_JOBDATA_ATTR14': rw['HHR_PER_JOBDATA_ATTR14'],
             'HHR_PER_JOBDATA_ATTR15': rw['HHR_PER_JOBDATA_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_pos_title_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'],
             'HHR_QUALIFICATION_CLASS': rw['HHR_QUALIFICATION_CLASS'],
             'HHR_QUALIFICATION_NAME': rw['HHR_QUALIFICATION_NAME'], 'HHR_HIRE_THE_TITLE': rw['HHR_HIRE_THE_TITLE'],
             'HHR_QUALIFICATION_LEVEL': rw['HHR_QUALIFICATION_LEVEL'],
             'HHR_QUALIFICATION_NBR': rw['HHR_QUALIFICATION_NBR'],
             'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'], 'HHR_END_DATE': rw['HHR_END_DATE'],
             'HHR_EXPIRATION_REMIND': rw['HHR_EXPIRATION_REMIND'], 'HHR_REGISTERED_UNIT': rw['HHR_REGISTERED_UNIT'],
             'HHR_HIRE_BEGIN_DATE': rw['HHR_HIRE_BEGIN_DATE'], 'HHR_HIRE_END_DATE': rw['HHR_HIRE_END_DATE'],
             'HHR_HIRE_EXPIRATION_REMIND': rw['HHR_HIRE_EXPIRATION_REMIND'],
             'HHR_HIRE_QUALIFICATION_LEVEL': rw['HHR_HIRE_QUALIFICATION_LEVEL'],
             'HHR_PER_POSTTITLE_ATTR01': rw['HHR_PER_POSTTITLE_ATTR01'],
             'HHR_PER_POSTTITLE_ATTR02': rw['HHR_PER_POSTTITLE_ATTR02'],
             'HHR_PER_POSTTITLE_ATTR03': rw['HHR_PER_POSTTITLE_ATTR03'],
             'HHR_PER_POSTTITLE_ATTR04': rw['HHR_PER_POSTTITLE_ATTR04'],
             'HHR_PER_POSTTITLE_ATTR05': rw['HHR_PER_POSTTITLE_ATTR05'],
             'HHR_PER_POSTTITLE_ATTR06': rw['HHR_PER_POSTTITLE_ATTR06'],
             'HHR_PER_POSTTITLE_ATTR07': rw['HHR_PER_POSTTITLE_ATTR07'],
             'HHR_PER_POSTTITLE_ATTR08': rw['HHR_PER_POSTTITLE_ATTR08'],
             'HHR_PER_POSTTITLE_ATTR09': rw['HHR_PER_POSTTITLE_ATTR09'],
             'HHR_PER_POSTTITLE_ATTR10': rw['HHR_PER_POSTTITLE_ATTR10'],
             'HHR_PER_POSTTITLE_ATTR11': rw['HHR_PER_POSTTITLE_ATTR11'],
             'HHR_PER_POSTTITLE_ATTR12': rw['HHR_PER_POSTTITLE_ATTR12'],
             'HHR_PER_POSTTITLE_ATTR13': rw['HHR_PER_POSTTITLE_ATTR13'],
             'HHR_PER_POSTTITLE_ATTR14': rw['HHR_PER_POSTTITLE_ATTR14'],
             'HHR_PER_POSTTITLE_ATTR15': rw['HHR_PER_POSTTITLE_ATTR15'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '',
             'NUM': ''}
        param.item.append(d)
    return param


def build_bio_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_NAME': rw['HHR_NAME'], 'HHR_OTHER_NAME': rw['HHR_OTHER_NAME'],
             'HHR_GENDER': rw['HHR_GENDER'],
             'HHR_PER_BIOGRAPHICAL_ATTR01': rw['HHR_PER_BIOGRAPHICAL_ATTR01'],
             'HHR_PER_BIOGRAPHICAL_ATTR02': rw['HHR_PER_BIOGRAPHICAL_ATTR02'],
             'HHR_PER_BIOGRAPHICAL_ATTR03': rw['HHR_PER_BIOGRAPHICAL_ATTR03'],
             'HHR_PER_BIOGRAPHICAL_ATTR04': rw['HHR_PER_BIOGRAPHICAL_ATTR04'],
             'HHR_PER_BIOGRAPHICAL_ATTR05': rw['HHR_PER_BIOGRAPHICAL_ATTR05'],
             'HHR_PER_BIOGRAPHICAL_ATTR06': rw['HHR_PER_BIOGRAPHICAL_ATTR06'],
             'HHR_PER_BIOGRAPHICAL_ATTR07': rw['HHR_PER_BIOGRAPHICAL_ATTR07'],
             'HHR_PER_BIOGRAPHICAL_ATTR08': rw['HHR_PER_BIOGRAPHICAL_ATTR08'],
             'HHR_PER_BIOGRAPHICAL_ATTR09': rw['HHR_PER_BIOGRAPHICAL_ATTR09'],
             'HHR_PER_BIOGRAPHICAL_ATTR10': rw['HHR_PER_BIOGRAPHICAL_ATTR10'],
             'HHR_PER_BIOGRAPHICAL_ATTR11': rw['HHR_PER_BIOGRAPHICAL_ATTR11'],
             'HHR_PER_BIOGRAPHICAL_ATTR12': rw['HHR_PER_BIOGRAPHICAL_ATTR12'],
             'HHR_PER_BIOGRAPHICAL_ATTR13': rw['HHR_PER_BIOGRAPHICAL_ATTR13'],
             'HHR_PER_BIOGRAPHICAL_ATTR14': rw['HHR_PER_BIOGRAPHICAL_ATTR14'],
             'HHR_PER_BIOGRAPHICAL_ATTR15': rw['HHR_PER_BIOGRAPHICAL_ATTR15'], 'REC_DATE': None, 'REC_TIME': None,
             'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_identity_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_COUNTRY': rw['HHR_COUNTRY'],
             'HHR_CERTIFICATE_TYPE': rw['HHR_CERTIFICATE_TYPE'], 'HHR_IDENTITY_NUM': rw['HHR_IDENTITY_NUM'],
             'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'], 'HHR_END_DATE': rw['HHR_END_DATE'],
             'HHR_BIRTH_DATE': rw['HHR_BIRTH_DATE'], 'HHR_EXPIRATION_REMIND': rw['HHR_EXPIRATION_REMIND'],
             'HHR_PER_IDENTITY_ATTR01': rw['HHR_PER_IDENTITY_ATTR01'],
             'HHR_PER_IDENTITY_ATTR02': rw['HHR_PER_IDENTITY_ATTR02'],
             'HHR_PER_IDENTITY_ATTR03': rw['HHR_PER_IDENTITY_ATTR03'],
             'HHR_PER_IDENTITY_ATTR04': rw['HHR_PER_IDENTITY_ATTR04'],
             'HHR_PER_IDENTITY_ATTR05': rw['HHR_PER_IDENTITY_ATTR05'],
             'HHR_PER_IDENTITY_ATTR06': rw['HHR_PER_IDENTITY_ATTR06'],
             'HHR_PER_IDENTITY_ATTR07': rw['HHR_PER_IDENTITY_ATTR07'],
             'HHR_PER_IDENTITY_ATTR08': rw['HHR_PER_IDENTITY_ATTR08'],
             'HHR_PER_IDENTITY_ATTR09': rw['HHR_PER_IDENTITY_ATTR09'],
             'HHR_PER_IDENTITY_ATTR10': rw['HHR_PER_IDENTITY_ATTR10'],
             'HHR_PER_IDENTITY_ATTR11': rw['HHR_PER_IDENTITY_ATTR11'],
             'HHR_PER_IDENTITY_ATTR12': rw['HHR_PER_IDENTITY_ATTR12'],
             'HHR_PER_IDENTITY_ATTR13': rw['HHR_PER_IDENTITY_ATTR13'],
             'HHR_PER_IDENTITY_ATTR14': rw['HHR_PER_IDENTITY_ATTR14'],
             'HHR_PER_IDENTITY_ATTR15': rw['HHR_PER_IDENTITY_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_addr_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_ADDRESS_TYPE': rw['HHR_ADDRESS_TYPE'], 'HHR_COUNTRY': rw['HHR_COUNTRY'],
             'HHR_PROVINCE': rw['HHR_PROVINCE'],
             'HHR_CITY': rw['HHR_CITY'], 'HHR_ADDRESS': rw['HHR_ADDRESS'], 'HHR_ZIP_CODE': rw['HHR_ZIP_CODE'],
             'HHR_PREFERRED': rw['HHR_PREFERRED'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_arch_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_FIRSTWORKING_DATE': rw['HHR_FIRSTWORKING_DATE'],
             'HHR_YEARSWORKING_DEDUCTION': rw['HHR_YEARSWORKING_DEDUCTION'],
             'HHR_RELIGION': rw['HHR_RELIGION'], 'HHR_ETHNIC_GROUP': rw['HHR_ETHNIC_GROUP'],
             'HHR_MARITAL_STATUS': rw['HHR_MARITAL_STATUS'],
             'HHR_POLITICS_STATUS': rw['HHR_POLITICS_STATUS'], 'HHR_NATIVE_PLACE': rw['HHR_NATIVE_PLACE'],
             'HHR_HUKOU_TYPE': rw['HHR_HUKOU_TYPE'],
             'HHR_PER_ARCHIVES_ATTR01': rw['HHR_PER_ARCHIVES_ATTR01'],
             'HHR_PER_ARCHIVES_ATTR02': rw['HHR_PER_ARCHIVES_ATTR02'],
             'HHR_PER_ARCHIVES_ATTR03': rw['HHR_PER_ARCHIVES_ATTR03'],
             'HHR_PER_ARCHIVES_ATTR04': rw['HHR_PER_ARCHIVES_ATTR04'],
             'HHR_PER_ARCHIVES_ATTR05': rw['HHR_PER_ARCHIVES_ATTR05'],
             'HHR_PER_ARCHIVES_ATTR06': rw['HHR_PER_ARCHIVES_ATTR06'],
             'HHR_PER_ARCHIVES_ATTR07': rw['HHR_PER_ARCHIVES_ATTR07'],
             'HHR_PER_ARCHIVES_ATTR08': rw['HHR_PER_ARCHIVES_ATTR08'],
             'HHR_PER_ARCHIVES_ATTR09': rw['HHR_PER_ARCHIVES_ATTR09'],
             'HHR_PER_ARCHIVES_ATTR10': rw['HHR_PER_ARCHIVES_ATTR10'],
             'HHR_PER_ARCHIVES_ATTR11': rw['HHR_PER_ARCHIVES_ATTR11'],
             'HHR_PER_ARCHIVES_ATTR12': rw['HHR_PER_ARCHIVES_ATTR12'],
             'HHR_PER_ARCHIVES_ATTR13': rw['HHR_PER_ARCHIVES_ATTR13'],
             'HHR_PER_ARCHIVES_ATTR14': rw['HHR_PER_ARCHIVES_ATTR14'],
             'HHR_PER_ARCHIVES_ATTR15': rw['HHR_PER_ARCHIVES_ATTR15'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '',
             'NUM': ''}
        param.item.append(d)
    return param


def build_cert_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_COUNTRY': rw['HHR_COUNTRY'],
             'HHR_CERTIFICATE_TYPE': rw['HHR_CERTIFICATE_TYPE'], 'HHR_CERTIFICATE_NUM': rw['HHR_CERTIFICATE_NUM'],
             'HHR_STATUS': rw['HHR_STATUS'], 'HHR_ISSUE_DATE': rw['HHR_ISSUE_DATE'],
             'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'],
             'HHR_END_DATE': rw['HHR_END_DATE'], 'HHR_EXPIRATION_REMIND': rw['HHR_EXPIRATION_REMIND'],
             'HHR_ISSUE_AUTHORITY': rw['HHR_ISSUE_AUTHORITY'],
             'HHR_PER_CERTIFICATE_ATTR01': rw['HHR_PER_CERTIFICATE_ATTR01'],
             'HHR_PER_CERTIFICATE_ATTR02': rw['HHR_PER_CERTIFICATE_ATTR02'],
             'HHR_PER_CERTIFICATE_ATTR03': rw['HHR_PER_CERTIFICATE_ATTR03'],
             'HHR_PER_CERTIFICATE_ATTR04': rw['HHR_PER_CERTIFICATE_ATTR04'],
             'HHR_PER_CERTIFICATE_ATTR05': rw['HHR_PER_CERTIFICATE_ATTR05'],
             'HHR_PER_CERTIFICATE_ATTR06': rw['HHR_PER_CERTIFICATE_ATTR06'],
             'HHR_PER_CERTIFICATE_ATTR07': rw['HHR_PER_CERTIFICATE_ATTR07'],
             'HHR_PER_CERTIFICATE_ATTR08': rw['HHR_PER_CERTIFICATE_ATTR08'],
             'HHR_PER_CERTIFICATE_ATTR09': rw['HHR_PER_CERTIFICATE_ATTR09'],
             'HHR_PER_CERTIFICATE_ATTR10': rw['HHR_PER_CERTIFICATE_ATTR10'],
             'HHR_PER_CERTIFICATE_ATTR11': rw['HHR_PER_CERTIFICATE_ATTR11'],
             'HHR_PER_CERTIFICATE_ATTR12': rw['HHR_PER_CERTIFICATE_ATTR12'],
             'HHR_PER_CERTIFICATE_ATTR13': rw['HHR_PER_CERTIFICATE_ATTR13'],
             'HHR_PER_CERTIFICATE_ATTR14': rw['HHR_PER_CERTIFICATE_ATTR14'],
             'HHR_PER_CERTIFICATE_ATTR15': rw['HHR_PER_CERTIFICATE_ATTR15'], 'REC_DATE': None, 'REC_TIME': None,
             'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_prob_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_PROBATION': rw['HHR_PROBATION'],
             'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'], 'HHR_END_DATE': rw['HHR_END_DATE'],
             'HHR_EXPIRATION_REMIND': rw['HHR_EXPIRATION_REMIND'],
             'HHR_PROBATION_INFO_ATTR01': rw['HHR_PROBATION_INFO_ATTR01'],
             'HHR_PROBATION_INFO_ATTR02': rw['HHR_PROBATION_INFO_ATTR02'],
             'HHR_PROBATION_INFO_ATTR03': rw['HHR_PROBATION_INFO_ATTR03'],
             'HHR_PROBATION_INFO_ATTR04': rw['HHR_PROBATION_INFO_ATTR04'],
             'HHR_PROBATION_INFO_ATTR05': rw['HHR_PROBATION_INFO_ATTR05'],
             'HHR_PROBATION_INFO_ATTR06': rw['HHR_PROBATION_INFO_ATTR06'],
             'HHR_PROBATION_INFO_ATTR07': rw['HHR_PROBATION_INFO_ATTR07'],
             'HHR_PROBATION_INFO_ATTR08': rw['HHR_PROBATION_INFO_ATTR08'],
             'HHR_PROBATION_INFO_ATTR09': rw['HHR_PROBATION_INFO_ATTR09'],
             'HHR_PROBATION_INFO_ATTR10': rw['HHR_PROBATION_INFO_ATTR10'],
             'HHR_PROBATION_INFO_ATTR11': rw['HHR_PROBATION_INFO_ATTR11'],
             'HHR_PROBATION_INFO_ATTR12': rw['HHR_PROBATION_INFO_ATTR12'],
             'HHR_PROBATION_INFO_ATTR13': rw['HHR_PROBATION_INFO_ATTR13'],
             'HHR_PROBATION_INFO_ATTR14': rw['HHR_PROBATION_INFO_ATTR14'],
             'HHR_PROBATION_INFO_ATTR15': rw['HHR_PROBATION_INFO_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_cont_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_CONTRACT_TYPE': rw['HHR_CONTRACT_TYPE'],
             'HHR_STATUS': rw['HHR_STATUS'], 'HHR_CONTRACT_NBR': rw['HHR_CONTRACT_NBR'],
             'HHR_CONTRACT_FIRST_PARTY': rw['HHR_CONTRACT_FIRST_PARTY'], 'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'],
             'HHR_ESTIMATED_END_DATE': rw['HHR_ESTIMATED_END_DATE'],
             'HHR_EXPIRATION_REMIND': rw['HHR_EXPIRATION_REMIND'],
             'HHR_END_DATE': rw['HHR_END_DATE'], 'HHR_COMENT': rw['HHR_COMENT'],
             'HHR_CONTRACT_INFO_ATTR01': rw['HHR_CONTRACT_INFO_ATTR01'],
             'HHR_CONTRACT_INFO_ATTR02': rw['HHR_CONTRACT_INFO_ATTR02'],
             'HHR_CONTRACT_INFO_ATTR03': rw['HHR_CONTRACT_INFO_ATTR03'],
             'HHR_CONTRACT_INFO_ATTR04': rw['HHR_CONTRACT_INFO_ATTR04'],
             'HHR_CONTRACT_INFO_ATTR05': rw['HHR_CONTRACT_INFO_ATTR05'],
             'HHR_CONTRACT_INFO_ATTR06': rw['HHR_CONTRACT_INFO_ATTR06'],
             'HHR_CONTRACT_INFO_ATTR07': rw['HHR_CONTRACT_INFO_ATTR07'],
             'HHR_CONTRACT_INFO_ATTR08': rw['HHR_CONTRACT_INFO_ATTR08'],
             'HHR_CONTRACT_INFO_ATTR09': rw['HHR_CONTRACT_INFO_ATTR09'],
             'HHR_CONTRACT_INFO_ATTR10': rw['HHR_CONTRACT_INFO_ATTR10'],
             'HHR_CONTRACT_INFO_ATTR11': rw['HHR_CONTRACT_INFO_ATTR11'],
             'HHR_CONTRACT_INFO_ATTR12': rw['HHR_CONTRACT_INFO_ATTR12'],
             'HHR_CONTRACT_INFO_ATTR13': rw['HHR_CONTRACT_INFO_ATTR13'],
             'HHR_CONTRACT_INFO_ATTR14': rw['HHR_CONTRACT_INFO_ATTR14'],
             'HHR_CONTRACT_INFO_ATTR15': rw['HHR_CONTRACT_INFO_ATTR15'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '',
             'NUM': ''}
        param.item.append(d)
    return param


def build_prot_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_PROTOCOL_TYPE': rw['HHR_PROTOCOL_TYPE'],
             'HHR_STATUS': rw['HHR_STATUS'], 'HHR_PROTOCOL_NBR': rw['HHR_PROTOCOL_NBR'],
             'HHR_PROTOCOL_FIRST_PARTY': rw['HHR_PROTOCOL_FIRST_PARTY'], 'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'],
             'HHR_ESTIMATED_END_DATE': rw['HHR_ESTIMATED_END_DATE'], 'HHR_END_DATE': rw['HHR_END_DATE'],
             'HHR_EXPIRATION_REMIND': rw['HHR_EXPIRATION_REMIND'], 'HHR_COMENT': rw['HHR_COMENT'],
             'HHR_PROTOCOL_INFO_ATTR01': rw['HHR_PROTOCOL_INFO_ATTR01'],
             'HHR_PROTOCOL_INFO_ATTR02': rw['HHR_PROTOCOL_INFO_ATTR02'],
             'HHR_PROTOCOL_INFO_ATTR03': rw['HHR_PROTOCOL_INFO_ATTR03'],
             'HHR_PROTOCOL_INFO_ATTR04': rw['HHR_PROTOCOL_INFO_ATTR04'],
             'HHR_PROTOCOL_INFO_ATTR05': rw['HHR_PROTOCOL_INFO_ATTR05'],
             'HHR_PROTOCOL_INFO_ATTR06': rw['HHR_PROTOCOL_INFO_ATTR06'],
             'HHR_PROTOCOL_INFO_ATTR07': rw['HHR_PROTOCOL_INFO_ATTR07'],
             'HHR_PROTOCOL_INFO_ATTR08': rw['HHR_PROTOCOL_INFO_ATTR08'],
             'HHR_PROTOCOL_INFO_ATTR09': rw['HHR_PROTOCOL_INFO_ATTR09'],
             'HHR_PROTOCOL_INFO_ATTR10': rw['HHR_PROTOCOL_INFO_ATTR10'],
             'HHR_PROTOCOL_INFO_ATTR11': rw['HHR_PROTOCOL_INFO_ATTR11'],
             'HHR_PROTOCOL_INFO_ATTR12': rw['HHR_PROTOCOL_INFO_ATTR12'],
             'HHR_PROTOCOL_INFO_ATTR13': rw['HHR_PROTOCOL_INFO_ATTR13'],
             'HHR_PROTOCOL_INFO_ATTR14': rw['HHR_PROTOCOL_INFO_ATTR14'],
             'HHR_PROTOCOL_INFO_ATTR15': rw['HHR_PROTOCOL_INFO_ATTR15'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '',
             'NUM': ''}
        param.item.append(d)
    return param


def build_cred_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'],
             'HHR_CREDENTIALS_TYPE': rw['HHR_CREDENTIALS_TYPE'],
             'HHR_CREDENTIALS_NAME': rw['HHR_CREDENTIALS_NAME'], 'HHR_CREDENTIALS_NBR': rw['HHR_CREDENTIALS_NBR'],
             'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'], 'HHR_END_DATE': rw['HHR_END_DATE'],
             'HHR_EXPIRATION_REMIND': rw['HHR_EXPIRATION_REMIND'], 'HHR_REGISTERED_UNIT': rw['HHR_REGISTERED_UNIT'],
             'HHR_PER_CREDENTIALS_ATTR01': rw['HHR_PER_CREDENTIALS_ATTR01'],
             'HHR_PER_CREDENTIALS_ATTR02': rw['HHR_PER_CREDENTIALS_ATTR02'],
             'HHR_PER_CREDENTIALS_ATTR03': rw['HHR_PER_CREDENTIALS_ATTR03'],
             'HHR_PER_CREDENTIALS_ATTR04': rw['HHR_PER_CREDENTIALS_ATTR04'],
             'HHR_PER_CREDENTIALS_ATTR05': rw['HHR_PER_CREDENTIALS_ATTR05'],
             'HHR_PER_CREDENTIALS_ATTR06': rw['HHR_PER_CREDENTIALS_ATTR06'],
             'HHR_PER_CREDENTIALS_ATTR07': rw['HHR_PER_CREDENTIALS_ATTR07'],
             'HHR_PER_CREDENTIALS_ATTR08': rw['HHR_PER_CREDENTIALS_ATTR08'],
             'HHR_PER_CREDENTIALS_ATTR09': rw['HHR_PER_CREDENTIALS_ATTR09'],
             'HHR_PER_CREDENTIALS_ATTR10': rw['HHR_PER_CREDENTIALS_ATTR10'],
             'HHR_PER_CREDENTIALS_ATTR11': rw['HHR_PER_CREDENTIALS_ATTR11'],
             'HHR_PER_CREDENTIALS_ATTR12': rw['HHR_PER_CREDENTIALS_ATTR12'],
             'HHR_PER_CREDENTIALS_ATTR13': rw['HHR_PER_CREDENTIALS_ATTR13'],
             'HHR_PER_CREDENTIALS_ATTR14': rw['HHR_PER_CREDENTIALS_ATTR14'],
             'HHR_PER_CREDENTIALS_ATTR15': rw['HHR_PER_CREDENTIALS_ATTR15'], 'REC_DATE': None, 'REC_TIME': None,
             'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_perf_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'],
             'HHR_ASSESSMENT_TYPE': rw['HHR_ASSESSMENT_TYPE'],
             'HHR_ASSESSMENT_RESULT': rw['HHR_ASSESSMENT_RESULT'], 'HHR_ASSESSMENT_GRADE': rw['HHR_ASSESSMENT_GRADE'],
             'HHR_REASON': rw['HHR_REASON'], 'HHR_AMT_NUM': rw['HHR_AMT_NUM'],
             'HHR_GENRE': rw['HHR_GENRE'], 'HHR_ASSESSMENT_REWARDS': rw['HHR_ASSESSMENT_REWARDS'],
             'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'],
             'HHR_END_DATE': rw['HHR_END_DATE'],
             'HHR_PER_ACHIEVEMENTS_ATTR01': rw['HHR_PER_ACHIEVEMENTS_ATTR01'],
             'HHR_PER_ACHIEVEMENTS_ATTR02': rw['HHR_PER_ACHIEVEMENTS_ATTR02'],
             'HHR_PER_ACHIEVEMENTS_ATTR03': rw['HHR_PER_ACHIEVEMENTS_ATTR03'],
             'HHR_PER_ACHIEVEMENTS_ATTR04': rw['HHR_PER_ACHIEVEMENTS_ATTR04'],
             'HHR_PER_ACHIEVEMENTS_ATTR05': rw['HHR_PER_ACHIEVEMENTS_ATTR05'],
             'HHR_PER_ACHIEVEMENTS_ATTR06': rw['HHR_PER_ACHIEVEMENTS_ATTR06'],
             'HHR_PER_ACHIEVEMENTS_ATTR07': rw['HHR_PER_ACHIEVEMENTS_ATTR07'],
             'HHR_PER_ACHIEVEMENTS_ATTR08': rw['HHR_PER_ACHIEVEMENTS_ATTR08'],
             'HHR_PER_ACHIEVEMENTS_ATTR09': rw['HHR_PER_ACHIEVEMENTS_ATTR09'],
             'HHR_PER_ACHIEVEMENTS_ATTR10': rw['HHR_PER_ACHIEVEMENTS_ATTR10'],
             'HHR_PER_ACHIEVEMENTS_ATTR11': rw['HHR_PER_ACHIEVEMENTS_ATTR11'],
             'HHR_PER_ACHIEVEMENTS_ATTR12': rw['HHR_PER_ACHIEVEMENTS_ATTR12'],
             'HHR_PER_ACHIEVEMENTS_ATTR13': rw['HHR_PER_ACHIEVEMENTS_ATTR13'],
             'HHR_PER_ACHIEVEMENTS_ATTR14': rw['HHR_PER_ACHIEVEMENTS_ATTR14'],
             'HHR_PER_ACHIEVEMENTS_ATTR15': rw['HHR_PER_ACHIEVEMENTS_ATTR15'], 'REC_DATE': None, 'REC_TIME': None,
             'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_tran_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'],
             'HHR_END_DATE': rw['HHR_END_DATE'], 'HHR_TRAIN_NAME': rw['HHR_TRAIN_NAME'],
             'HHR_TRAIN_LONGTH': rw['HHR_TRAIN_LONGTH'], 'HHR_TRAIN_ORG': rw['HHR_TRAIN_ORG'],
             'HHR_TRAIN_TEACHER': rw['HHR_TRAIN_TEACHER'], 'HHR_TRAIN_TYPE': rw['HHR_TRAIN_TYPE'],
             'HHR_TRAIN_SIGN': rw['HHR_TRAIN_SIGN'], 'HHR_TRAIN_SIGN_BGN_DT': rw['HHR_TRAIN_SIGN_BGN_DT'],
             'HHR_TRAIN_SIGN_END_DT': rw['HHR_TRAIN_SIGN_END_DT'], 'HHR_TRAIN_COST': rw['HHR_TRAIN_COST'],
             'HHR_SAT_DEGREE': rw['HHR_SAT_DEGREE'], 'HHR_BASE_MSG': rw['HHR_BASE_MSG'],
             'HHR_PER_TRAININFO_ATTR01': rw['HHR_PER_TRAININFO_ATTR01'],
             'HHR_PER_TRAININFO_ATTR02': rw['HHR_PER_TRAININFO_ATTR02'],
             'HHR_PER_TRAININFO_ATTR03': rw['HHR_PER_TRAININFO_ATTR03'],
             'HHR_PER_TRAININFO_ATTR04': rw['HHR_PER_TRAININFO_ATTR04'],
             'HHR_PER_TRAININFO_ATTR05': rw['HHR_PER_TRAININFO_ATTR05'],
             'HHR_PER_TRAININFO_ATTR06': rw['HHR_PER_TRAININFO_ATTR06'],
             'HHR_PER_TRAININFO_ATTR07': rw['HHR_PER_TRAININFO_ATTR07'],
             'HHR_PER_TRAININFO_ATTR08': rw['HHR_PER_TRAININFO_ATTR08'],
             'HHR_PER_TRAININFO_ATTR09': rw['HHR_PER_TRAININFO_ATTR09'],
             'HHR_PER_TRAININFO_ATTR10': rw['HHR_PER_TRAININFO_ATTR10'],
             'HHR_PER_TRAININFO_ATTR11': rw['HHR_PER_TRAININFO_ATTR11'],
             'HHR_PER_TRAININFO_ATTR12': rw['HHR_PER_TRAININFO_ATTR12'],
             'HHR_PER_TRAININFO_ATTR13': rw['HHR_PER_TRAININFO_ATTR13'],
             'HHR_PER_TRAININFO_ATTR14': rw['HHR_PER_TRAININFO_ATTR14'],
             'HHR_PER_TRAININFO_ATTR15': rw['HHR_PER_TRAININFO_ATTR15'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '',
             'NUM': ''}
        param.item.append(d)
    return param


def build_phone_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_PHONE_TYPE': rw['HHR_PHONE_TYPE'], 'HHR_PHONE_NUM': rw['HHR_PHONE_NUM'],
             'HHR_PREFERRED': rw['HHR_PREFERRED'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_mail_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_EMAIL_TYPE': rw['HHR_EMAIL_TYPE'],
             'HHR_EMAIL_ADDR': rw['HHR_EMAIL_ADDR'],
             'HHR_PREFERRED': rw['HHR_PREFERRED'], 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_edu_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'],
             'HHR_END_DATE': rw['HHR_END_DATE'], 'HHR_SCHOOL': rw['HHR_SCHOOL'], 'HHR_MAJOR': rw['HHR_MAJOR'],
             'HHR_EDUCATIONAL': rw['HHR_EDUCATIONAL'], 'HHR_HIGHEST_EDUCATIONAL': rw['HHR_HIGHEST_EDUCATIONAL'],
             'HHR_EDUCATIONA_TYPE': rw['HHR_EDUCATIONA_TYPE'], 'HHR_DEGREE': rw['HHR_DEGREE'],
             'HHR_STUDY_TYPE': rw['HHR_STUDY_TYPE'],
             'HHR_PER_EDUCATION_ATTR01': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR01'],
             'HHR_PER_EDUCATION_ATTR02': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR02'],
             'HHR_PER_EDUCATION_ATTR03': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR03'],
             'HHR_PER_EDUCATION_ATTR04': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR04'],
             'HHR_PER_EDUCATION_ATTR05': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR05'],
             'HHR_PER_EDUCATION_ATTR06': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR06'],
             'HHR_PER_EDUCATION_ATTR07': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR07'],
             'HHR_PER_EDUCATION_ATTR08': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR08'],
             'HHR_PER_EDUCATION_ATTR09': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR09'],
             'HHR_PER_EDUCATION_ATTR10': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR10'],
             'HHR_PER_EDUCATION_ATTR11': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR11'],
             'HHR_PER_EDUCATION_ATTR12': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR12'],
             'HHR_PER_EDUCATION_ATTR13': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR13'],
             'HHR_PER_EDUCATION_ATTR14': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR14'],
             'HHR_PER_EDUCATION_ATTR15': rw['HHR_PER_EDUCATION_EXPERIENCE_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_reward_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_RP_TYPE': rw['HHR_RP_TYPE'],
             'HHR_RP_DESC': rw['HHR_RP_DESC'], 'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'],
             'HHR_RP_SCORE': rw['HHR_RP_SCORE'],
             'HHR_RP_DEPARTMENT': rw['HHR_RP_DEPARTMENT'], 'HHR_RP_DETAIL': rw['HHR_RP_DETAIL'],
             'HHR_PER_REWARDS_ATTR01': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR01'],
             'HHR_PER_REWARDS_ATTR02': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR02'],
             'HHR_PER_REWARDS_ATTR03': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR03'],
             'HHR_PER_REWARDS_ATTR04': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR04'],
             'HHR_PER_REWARDS_ATTR05': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR05'],
             'HHR_PER_REWARDS_ATTR06': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR06'],
             'HHR_PER_REWARDS_ATTR07': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR07'],
             'HHR_PER_REWARDS_ATTR08': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR08'],
             'HHR_PER_REWARDS_ATTR09': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR09'],
             'HHR_PER_REWARDS_ATTR10': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR10'],
             'HHR_PER_REWARDS_ATTR11': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR11'],
             'HHR_PER_REWARDS_ATTR12': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR12'],
             'HHR_PER_REWARDS_ATTR13': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR13'],
             'HHR_PER_REWARDS_ATTR14': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR14'],
             'HHR_PER_REWARDS_ATTR15': rw['HHR_PER_REWARDS_PUNISHMENT_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_rela_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'],
             'HHR_RELATIONSHIP_TYPE': rw['HHR_RELATIONSHIP_TYPE'],
             'HHR_DESCRIPTION': rw['HHR_DESCRIPTION'], 'HHR_NAME': rw['HHR_NAME'], 'HHR_GENDER': rw['HHR_GENDER'],
             'HHR_EMERGENCY_CONTACT_PERSON': rw['HHR_EMERGENCY_CONTACT_PERSON'], 'HHR_PHONE': rw['HHR_PHONE'],
             'HHR_COUNTRY': rw['HHR_COUNTRY'], 'HHR_CERTIFICATE_TYPE': rw['HHR_CERTIFICATE_TYPE'],
             'HHR_IDENTITY_NUM': rw['HHR_IDENTITY_NUM'],
             'HHR_BIRTH_DATE': rw['HHR_BIRTH_DATE'],
             'HHR_RELATIONSHIP_ATTR01': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR01'],
             'HHR_RELATIONSHIP_ATTR02': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR02'],
             'HHR_RELATIONSHIP_ATTR03': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR03'],
             'HHR_RELATIONSHIP_ATTR04': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR04'],
             'HHR_RELATIONSHIP_ATTR05': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR05'],
             'HHR_RELATIONSHIP_ATTR06': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR06'],
             'HHR_RELATIONSHIP_ATTR07': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR07'],
             'HHR_RELATIONSHIP_ATTR08': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR08'],
             'HHR_RELATIONSHIP_ATTR09': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR09'],
             'HHR_RELATIONSHIP_ATTR10': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR10'],
             'HHR_RELATIONSHIP_ATTR11': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR11'],
             'HHR_RELATIONSHIP_ATTR12': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR12'],
             'HHR_RELATIONSHIP_ATTR13': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR13'],
             'HHR_RELATIONSHIP_ATTR14': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR14'],
             'HHR_RELATIONSHIP_ATTR15': rw['HHR_PER_SOCIALRE_RELATIONSHIP_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_work_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'],
             'HHR_EXPERIENCE_TYPE': rw['HHR_EXPERIENCE_TYPE'],
             'HHR_BEGIN_DATE': rw['HHR_BEGIN_DATE'], 'HHR_END_DATE': rw['HHR_END_DATE'],
             'HHR_EMPLOYER': rw['HHR_EMPLOYER'],
             'HHR_POSITION': rw['HHR_POSITION'], 'HHR_RELEVANT_WORK': rw['HHR_RELEVANT_WORK'],
             'HHR_PER_WORKEXPERIENCE_ATTR01': rw['HHR_PER_WORKEXPERIENCE_ATTR01'],
             'HHR_PER_WORKEXPERIENCE_ATTR02': rw['HHR_PER_WORKEXPERIENCE_ATTR02'],
             'HHR_PER_WORKEXPERIENCE_ATTR03': rw['HHR_PER_WORKEXPERIENCE_ATTR03'],
             'HHR_PER_WORKEXPERIENCE_ATTR04': rw['HHR_PER_WORKEXPERIENCE_ATTR04'],
             'HHR_PER_WORKEXPERIENCE_ATTR05': rw['HHR_PER_WORKEXPERIENCE_ATTR05'],
             'HHR_PER_WORKEXPERIENCE_ATTR06': rw['HHR_PER_WORKEXPERIENCE_ATTR06'],
             'HHR_PER_WORKEXPERIENCE_ATTR07': rw['HHR_PER_WORKEXPERIENCE_ATTR07'],
             'HHR_PER_WORKEXPERIENCE_ATTR08': rw['HHR_PER_WORKEXPERIENCE_ATTR08'],
             'HHR_PER_WORKEXPERIENCE_ATTR09': rw['HHR_PER_WORKEXPERIENCE_ATTR09'],
             'HHR_PER_WORKEXPERIENCE_ATTR10': rw['HHR_PER_WORKEXPERIENCE_ATTR10'],
             'HHR_PER_WORKEXPERIENCE_ATTR11': rw['HHR_PER_WORKEXPERIENCE_ATTR11'],
             'HHR_PER_WORKEXPERIENCE_ATTR12': rw['HHR_PER_WORKEXPERIENCE_ATTR12'],
             'HHR_PER_WORKEXPERIENCE_ATTR13': rw['HHR_PER_WORKEXPERIENCE_ATTR13'],
             'HHR_PER_WORKEXPERIENCE_ATTR14': rw['HHR_PER_WORKEXPERIENCE_ATTR14'],
             'HHR_PER_WORKEXPERIENCE_ATTR15': rw['HHR_PER_WORKEXPERIENCE_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_lang_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_LANGUAGE': rw['HHR_LANGUAGE'],
             'HHR_LEVEL': rw['HHR_LEVEL'],
             'HHR_PER_LANGUAGE_ATTR01': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR01'],
             'HHR_PER_LANGUAGE_ATTR02': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR02'],
             'HHR_PER_LANGUAGE_ATTR03': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR03'],
             'HHR_PER_LANGUAGE_ATTR04': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR04'],
             'HHR_PER_LANGUAGE_ATTR05': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR05'],
             'HHR_PER_LANGUAGE_ATTR06': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR06'],
             'HHR_PER_LANGUAGE_ATTR07': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR07'],
             'HHR_PER_LANGUAGE_ATTR08': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR08'],
             'HHR_PER_LANGUAGE_ATTR09': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR09'],
             'HHR_PER_LANGUAGE_ATTR10': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR10'],
             'HHR_PER_LANGUAGE_ATTR11': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR11'],
             'HHR_PER_LANGUAGE_ATTR12': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR12'],
             'HHR_PER_LANGUAGE_ATTR13': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR13'],
             'HHR_PER_LANGUAGE_ATTR14': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR14'],
             'HHR_PER_LANGUAGE_ATTR15': rw['HHR_PER_LANGUAGE_ABILITY_INFO_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def build_bank_method_param(param, rw):
    if rw is not None:
        d = {'HHR_EMPID': rw['HHR_EMPID'], 'HHR_SEQNUM': rw['HHR_SEQNUM'], 'HHR_CARD_TYPE': rw['HHR_CARD_TYPE'],
             'HHR_COUNTRY': rw['HHR_COUNTRY'], 'HHR_CURRENCY': rw['HHR_CURRENCY'],
             'HHR_ACCOUNT_NAME': rw['HHR_ACCOUNT_NAME'],
             'HHR_ACCOUNT_NUMBER': rw['HHR_ACCOUNT_NUMBER'], 'HHR_BANK_ID': rw['HHR_BANK_ID'],
             'HHR_BANK_BRANCH_ID': rw['HHR_BANK_BRANCH_ID'],
             'HHR_PER_BANK_CARD_ATTR01': rw['HHR_PER_BANK_CARD_ATTR01'],
             'HHR_PER_BANK_CARD_ATTR02': rw['HHR_PER_BANK_CARD_ATTR02'],
             'HHR_PER_BANK_CARD_ATTR03': rw['HHR_PER_BANK_CARD_ATTR03'],
             'HHR_PER_BANK_CARD_ATTR04': rw['HHR_PER_BANK_CARD_ATTR04'],
             'HHR_PER_BANK_CARD_ATTR05': rw['HHR_PER_BANK_CARD_ATTR05'],
             'HHR_PER_BANK_CARD_ATTR06': rw['HHR_PER_BANK_CARD_ATTR06'],
             'HHR_PER_BANK_CARD_ATTR07': rw['HHR_PER_BANK_CARD_ATTR07'],
             'HHR_PER_BANK_CARD_ATTR08': rw['HHR_PER_BANK_CARD_ATTR08'],
             'HHR_PER_BANK_CARD_ATTR09': rw['HHR_PER_BANK_CARD_ATTR09'],
             'HHR_PER_BANK_CARD_ATTR10': rw['HHR_PER_BANK_CARD_ATTR10'],
             'HHR_PER_BANK_CARD_ATTR11': rw['HHR_PER_BANK_CARD_ATTR11'],
             'HHR_PER_BANK_CARD_ATTR12': rw['HHR_PER_BANK_CARD_ATTR12'],
             'HHR_PER_BANK_CARD_ATTR13': rw['HHR_PER_BANK_CARD_ATTR13'],
             'HHR_PER_BANK_CARD_ATTR14': rw['HHR_PER_BANK_CARD_ATTR14'],
             'HHR_PER_BANK_CARD_ATTR15': rw['HHR_PER_BANK_CARD_ATTR15'],
             'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
        param.item.append(d)
    return param


def get_job_data(conn, sys_code, emp_id, emp_rcd, part_time):
    """
    获取任职信息
    :param conn: 数据库连接
    :param sys_code: 系统ID
    :param emp_id: 员工ID
    :param emp_rcd: 员工任职记录
    :param part_time: P-兼职；M-主职
    """

    job_data_sel = text(
        "select HHR_EMPID, HHR_EMP_RCD, HHR_EFFE_DATE, HHR_EFFDT_SEQ, HHR_STATUS, HHR_ACTION_CODE, HHR_REASON_CODE, "
        "HHR_RELATION, HHR_POSITION_CODE, HHR_DEPT_CODE, HHR_BU_CODE, HHR_COMPANY_CODE, HHR_LOCATION, HHR_COVER_LOC, "
        "HHR_DIRECT, HHR_COVER_DIRECT, HHR_INDIRECT, HHR_COVER_INDIRECT, HHR_EMP_CLASS, HHR_SUB_EMP_CLASS, HHR_POST_CODE, "
        "HHR_RANK_CODE, HHR_ORIG_HIRE_DATE, HHR_LAST_HIRE_DATE, HHR_LAST_DATE_WORKED, HHR_TERMINATION_DATE, HHR_HANDLE_LEAVE_DATE, "
        "HHR_PERSONAL_MAIN_REASON, HHR_COMPANY_MAIN_REASON, HHR_LEAVE_DETAIL, HHR_PER_JOBDATA_ATTR01, HHR_PER_JOBDATA_ATTR02, "
        "HHR_PER_JOBDATA_ATTR03, HHR_PER_JOBDATA_ATTR04, HHR_PER_JOBDATA_ATTR05, HHR_PER_JOBDATA_ATTR06, HHR_PER_JOBDATA_ATTR07,  "
        "HHR_PER_JOBDATA_ATTR08, HHR_PER_JOBDATA_ATTR09, HHR_PER_JOBDATA_ATTR10, HHR_PER_JOBDATA_ATTR11, HHR_PER_JOBDATA_ATTR12, "
        "HHR_PER_JOBDATA_ATTR13, HHR_PER_JOBDATA_ATTR14, HHR_PER_JOBDATA_ATTR15 "
        "from HHR_PER_JOBDATA_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 and ( HHR_EMP_RCD = :b3 or (HHR_EMP_RCD <> 1 and 'P' = :b4) ) "
        "order by HHR_EMPID, HHR_EMP_RCD, HHR_EFFE_DATE, HHR_EFFDT_SEQ ")
    job_rs = conn.execute(job_data_sel, b1=sys_code, b2=emp_id, b3=emp_rcd, b4=part_time).fetchall()
    return job_rs


def get_pos_title_data(conn, sys_code, emp_id):
    """获取职称信息"""
    data_sel = text("select HHR_EMPID, HHR_SEQNUM, HHR_QUALIFICATION_CLASS, HHR_QUALIFICATION_NAME, HHR_HIRE_THE_TITLE,"
                    "HHR_QUALIFICATION_LEVEL, HHR_QUALIFICATION_NBR, HHR_BEGIN_DATE, HHR_END_DATE, HHR_EXPIRATION_REMIND, "
                    "HHR_REGISTERED_UNIT, HHR_HIRE_BEGIN_DATE, HHR_HIRE_END_DATE, HHR_HIRE_EXPIRATION_REMIND, HHR_HIRE_QUALIFICATION_LEVEL,"
                    "HHR_PER_POSTTITLE_ATTR01, HHR_PER_POSTTITLE_ATTR02, HHR_PER_POSTTITLE_ATTR03, HHR_PER_POSTTITLE_ATTR04, "
                    "HHR_PER_POSTTITLE_ATTR05, HHR_PER_POSTTITLE_ATTR06, HHR_PER_POSTTITLE_ATTR07, "
                    "HHR_PER_POSTTITLE_ATTR08, HHR_PER_POSTTITLE_ATTR09, HHR_PER_POSTTITLE_ATTR10, "
                    "HHR_PER_POSTTITLE_ATTR11, HHR_PER_POSTTITLE_ATTR12, HHR_PER_POSTTITLE_ATTR13, "
                    "HHR_PER_POSTTITLE_ATTR14, HHR_PER_POSTTITLE_ATTR15 from HHR_PER_POSTTITLE_B "
                    "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_bio_data(conn, sys_code, emp_id):
    """获取基本信息"""
    data_sel = text("select HHR_EMPID, HHR_NAME, HHR_OTHER_NAME, HHR_GENDER, HHR_PER_BIOGRAPHICAL_ATTR01,"
                    "HHR_PER_BIOGRAPHICAL_ATTR02, HHR_PER_BIOGRAPHICAL_ATTR03, HHR_PER_BIOGRAPHICAL_ATTR04, "
                    "HHR_PER_BIOGRAPHICAL_ATTR05, HHR_PER_BIOGRAPHICAL_ATTR06, HHR_PER_BIOGRAPHICAL_ATTR07, "
                    "HHR_PER_BIOGRAPHICAL_ATTR08, HHR_PER_BIOGRAPHICAL_ATTR09, HHR_PER_BIOGRAPHICAL_ATTR10, "
                    "HHR_PER_BIOGRAPHICAL_ATTR11, HHR_PER_BIOGRAPHICAL_ATTR12, HHR_PER_BIOGRAPHICAL_ATTR13, "
                    "HHR_PER_BIOGRAPHICAL_ATTR14, HHR_PER_BIOGRAPHICAL_ATTR15 from HHR_PER_BIOGRAPHICAL_B "
                    "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_identity_data(conn, sys_code, emp_id):
    """获取身份信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_COUNTRY, HHR_CERTIFICATE_TYPE, HHR_IDENTITY_NUM, HHR_BEGIN_DATE, HHR_END_DATE, "
        "HHR_BIRTH_DATE, HHR_EXPIRATION_REMIND, HHR_PER_IDENTITY_ATTR01, HHR_PER_IDENTITY_ATTR02, "
        "HHR_PER_IDENTITY_ATTR03, HHR_PER_IDENTITY_ATTR04, HHR_PER_IDENTITY_ATTR05, HHR_PER_IDENTITY_ATTR06, "
        "HHR_PER_IDENTITY_ATTR07, HHR_PER_IDENTITY_ATTR08, HHR_PER_IDENTITY_ATTR09, HHR_PER_IDENTITY_ATTR10, "
        "HHR_PER_IDENTITY_ATTR11, HHR_PER_IDENTITY_ATTR12, HHR_PER_IDENTITY_ATTR13, HHR_PER_IDENTITY_ATTR14, "
        "HHR_PER_IDENTITY_ATTR15 from HHR_PER_IDENTITY_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_addr_data(conn, sys_code, emp_id):
    """获取地址信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_ADDRESS_TYPE, HHR_COUNTRY, HHR_PROVINCE, HHR_CITY, HHR_ADDRESS, HHR_ZIP_CODE, "
        "HHR_PREFERRED from HHR_PER_ADDRESS_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_arch_data(conn, sys_code, emp_id):
    """获取档案信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_FIRSTWORKING_DATE, HHR_YEARSWORKING_DEDUCTION, HHR_RELIGION, HHR_ETHNIC_GROUP, "
        "HHR_MARITAL_STATUS, HHR_POLITICS_STATUS, HHR_NATIVE_PLACE, HHR_HUKOU_TYPE, "
        "HHR_PER_ARCHIVES_ATTR01, HHR_PER_ARCHIVES_ATTR02, HHR_PER_ARCHIVES_ATTR03, HHR_PER_ARCHIVES_ATTR04, "
        "HHR_PER_ARCHIVES_ATTR05, HHR_PER_ARCHIVES_ATTR06, HHR_PER_ARCHIVES_ATTR07, HHR_PER_ARCHIVES_ATTR08, "
        "HHR_PER_ARCHIVES_ATTR09, HHR_PER_ARCHIVES_ATTR10, HHR_PER_ARCHIVES_ATTR11, HHR_PER_ARCHIVES_ATTR12, "
        "HHR_PER_ARCHIVES_ATTR13, HHR_PER_ARCHIVES_ATTR14, HHR_PER_ARCHIVES_ATTR15 "
        "from HHR_PER_ARCHIVES_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_cert_data(conn, sys_code, emp_id):
    """获取证件信息"""
    data_sel = text("select HHR_EMPID, HHR_SEQNUM, HHR_COUNTRY, HHR_CERTIFICATE_TYPE, HHR_CERTIFICATE_NUM, HHR_STATUS, "
                    "HHR_ISSUE_DATE, HHR_BEGIN_DATE, HHR_END_DATE, HHR_EXPIRATION_REMIND, HHR_ISSUE_AUTHORITY, "
                    "HHR_PER_CERTIFICATE_ATTR01, HHR_PER_CERTIFICATE_ATTR02, HHR_PER_CERTIFICATE_ATTR03, HHR_PER_CERTIFICATE_ATTR04, "
                    "HHR_PER_CERTIFICATE_ATTR05, HHR_PER_CERTIFICATE_ATTR06, HHR_PER_CERTIFICATE_ATTR07, HHR_PER_CERTIFICATE_ATTR08, "
                    "HHR_PER_CERTIFICATE_ATTR09, HHR_PER_CERTIFICATE_ATTR10, HHR_PER_CERTIFICATE_ATTR11, HHR_PER_CERTIFICATE_ATTR12, "
                    "HHR_PER_CERTIFICATE_ATTR13, HHR_PER_CERTIFICATE_ATTR14, HHR_PER_CERTIFICATE_ATTR15 "
                    "from HHR_PER_CERTIFICATE_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_prob_data(conn, sys_code, emp_id):
    """获取试用期信息"""
    data_sel = text("select HHR_EMPID, HHR_SEQNUM, HHR_PROBATION, HHR_BEGIN_DATE, HHR_END_DATE, HHR_EXPIRATION_REMIND, "
                    "HHR_PROBATION_INFO_ATTR01, HHR_PROBATION_INFO_ATTR02, HHR_PROBATION_INFO_ATTR03, HHR_PROBATION_INFO_ATTR04, "
                    "HHR_PROBATION_INFO_ATTR05, HHR_PROBATION_INFO_ATTR06, HHR_PROBATION_INFO_ATTR07, HHR_PROBATION_INFO_ATTR08, "
                    "HHR_PROBATION_INFO_ATTR09, HHR_PROBATION_INFO_ATTR10, HHR_PROBATION_INFO_ATTR11, HHR_PROBATION_INFO_ATTR12, "
                    "HHR_PROBATION_INFO_ATTR13, HHR_PROBATION_INFO_ATTR14, HHR_PROBATION_INFO_ATTR15 "
                    "from HHR_PER_PROBATION_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_cont_data(conn, sys_code, emp_id):
    """获取合同信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_CONTRACT_TYPE, HHR_STATUS, HHR_CONTRACT_NBR, HHR_CONTRACT_FIRST_PARTY, "
        "HHR_BEGIN_DATE, HHR_ESTIMATED_END_DATE, HHR_EXPIRATION_REMIND, HHR_END_DATE, HHR_COMENT, HHR_CONTRACT_INFO_ATTR01, "
        "HHR_CONTRACT_INFO_ATTR02, HHR_CONTRACT_INFO_ATTR03, HHR_CONTRACT_INFO_ATTR04, HHR_CONTRACT_INFO_ATTR05, "
        "HHR_CONTRACT_INFO_ATTR06, HHR_CONTRACT_INFO_ATTR07, HHR_CONTRACT_INFO_ATTR08, HHR_CONTRACT_INFO_ATTR09, "
        "HHR_CONTRACT_INFO_ATTR10, HHR_CONTRACT_INFO_ATTR11, HHR_CONTRACT_INFO_ATTR12, HHR_CONTRACT_INFO_ATTR13, "
        "HHR_CONTRACT_INFO_ATTR14, HHR_CONTRACT_INFO_ATTR15 "
        "from HHR_PER_CONTRACT_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_prot_data(conn, sys_code, emp_id):
    """获取协议信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_PROTOCOL_TYPE, HHR_STATUS, HHR_PROTOCOL_NBR, HHR_PROTOCOL_FIRST_PARTY, "
        "HHR_BEGIN_DATE, HHR_ESTIMATED_END_DATE, HHR_END_DATE, HHR_EXPIRATION_REMIND, HHR_COMENT, "
        "HHR_PROTOCOL_INFO_ATTR01, HHR_PROTOCOL_INFO_ATTR02, HHR_PROTOCOL_INFO_ATTR03, HHR_PROTOCOL_INFO_ATTR04, "
        "HHR_PROTOCOL_INFO_ATTR05, HHR_PROTOCOL_INFO_ATTR06, HHR_PROTOCOL_INFO_ATTR07, HHR_PROTOCOL_INFO_ATTR08, "
        "HHR_PROTOCOL_INFO_ATTR09, HHR_PROTOCOL_INFO_ATTR10, HHR_PROTOCOL_INFO_ATTR11, HHR_PROTOCOL_INFO_ATTR12, "
        "HHR_PROTOCOL_INFO_ATTR13, HHR_PROTOCOL_INFO_ATTR14, HHR_PROTOCOL_INFO_ATTR15 "
        "from HHR_PER_PROTOCOL_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_cred_data(conn, sys_code, emp_id):
    """获取证书信息"""
    data_sel = text("select HHR_EMPID, HHR_SEQNUM, HHR_CREDENTIALS_TYPE, HHR_CREDENTIALS_NAME, HHR_CREDENTIALS_NBR, "
                    "HHR_BEGIN_DATE, HHR_END_DATE, HHR_EXPIRATION_REMIND, HHR_REGISTERED_UNIT, HHR_PER_CREDENTIALS_ATTR01, "
                    "HHR_PER_CREDENTIALS_ATTR02, HHR_PER_CREDENTIALS_ATTR03, HHR_PER_CREDENTIALS_ATTR04, HHR_PER_CREDENTIALS_ATTR05, "
                    "HHR_PER_CREDENTIALS_ATTR06, HHR_PER_CREDENTIALS_ATTR07, HHR_PER_CREDENTIALS_ATTR08, HHR_PER_CREDENTIALS_ATTR09, "
                    "HHR_PER_CREDENTIALS_ATTR10, HHR_PER_CREDENTIALS_ATTR11, HHR_PER_CREDENTIALS_ATTR12, HHR_PER_CREDENTIALS_ATTR13, "
                    "HHR_PER_CREDENTIALS_ATTR14, HHR_PER_CREDENTIALS_ATTR15 "
                    "from HHR_PER_CREDENTIALS_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_perf_data(conn, sys_code, emp_id):
    """获取绩效信息"""
    data_sel = text("select HHR_EMPID, HHR_SEQNUM, HHR_ASSESSMENT_TYPE, HHR_ASSESSMENT_RESULT, HHR_ASSESSMENT_GRADE, "
                    "HHR_REASON, HHR_AMT_NUM, HHR_GENRE, HHR_ASSESSMENT_REWARDS, HHR_BEGIN_DATE, HHR_END_DATE, HHR_PER_ACHIEVEMENTS_ATTR01, "
                    "HHR_PER_ACHIEVEMENTS_ATTR02, HHR_PER_ACHIEVEMENTS_ATTR03, HHR_PER_ACHIEVEMENTS_ATTR04, HHR_PER_ACHIEVEMENTS_ATTR05, "
                    "HHR_PER_ACHIEVEMENTS_ATTR06, HHR_PER_ACHIEVEMENTS_ATTR07, HHR_PER_ACHIEVEMENTS_ATTR08, HHR_PER_ACHIEVEMENTS_ATTR09, "
                    "HHR_PER_ACHIEVEMENTS_ATTR10, HHR_PER_ACHIEVEMENTS_ATTR11, HHR_PER_ACHIEVEMENTS_ATTR12, HHR_PER_ACHIEVEMENTS_ATTR13, "
                    "HHR_PER_ACHIEVEMENTS_ATTR14, HHR_PER_ACHIEVEMENTS_ATTR15 "
                    "from HHR_PER_ACHIEVEMENTS_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_tran_data(conn, sys_code, emp_id):
    """获取培训信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_BEGIN_DATE, HHR_END_DATE, HHR_END_DATE, HHR_TRAIN_NAME, HHR_TRAIN_LONGTH, HHR_TRAIN_ORG, "
        "HHR_TRAIN_TEACHER, HHR_TRAIN_TYPE, HHR_TRAIN_SIGN, HHR_TRAIN_SIGN_BGN_DT, HHR_TRAIN_SIGN_END_DT, HHR_TRAIN_COST, HHR_SAT_DEGREE, "
        "HHR_BASE_MSG, HHR_PER_TRAININFO_ATTR01, HHR_PER_TRAININFO_ATTR02, HHR_PER_TRAININFO_ATTR03, HHR_PER_TRAININFO_ATTR04, "
        "HHR_PER_TRAININFO_ATTR05, HHR_PER_TRAININFO_ATTR06, HHR_PER_TRAININFO_ATTR07, HHR_PER_TRAININFO_ATTR08, HHR_PER_TRAININFO_ATTR09, "
        "HHR_PER_TRAININFO_ATTR10, HHR_PER_TRAININFO_ATTR11, HHR_PER_TRAININFO_ATTR12, HHR_PER_TRAININFO_ATTR13, HHR_PER_TRAININFO_ATTR14, "
        "HHR_PER_TRAININFO_ATTR15 from HHR_PER_TRAININFO_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_phone_data(conn, sys_code, emp_id):
    """获取电话信息"""
    data_sel = text("select HHR_EMPID, HHR_PHONE_TYPE, HHR_PHONE_NUM, HHR_PREFERRED from HHR_PER_PHONE_B "
                    "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_mail_data(conn, sys_code, emp_id):
    """获取邮箱信息"""
    data_sel = text("select HHR_EMPID, HHR_EMAIL_TYPE, HHR_EMAIL_ADDR, HHR_PREFERRED from HHR_PER_EMAIL_B "
                    "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_edu_data(conn, sys_code, emp_id):
    """获取教育信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_BEGIN_DATE, HHR_END_DATE, HHR_SCHOOL, HHR_MAJOR, HHR_EDUCATIONAL, HHR_HIGHEST_EDUCATIONAL, "
        "HHR_EDUCATIONA_TYPE, HHR_DEGREE, HHR_STUDY_TYPE, HHR_PER_EDUCATION_EXPERIENCE_ATTR01, HHR_PER_EDUCATION_EXPERIENCE_ATTR02, "
        "HHR_PER_EDUCATION_EXPERIENCE_ATTR03, HHR_PER_EDUCATION_EXPERIENCE_ATTR04, HHR_PER_EDUCATION_EXPERIENCE_ATTR05, "
        "HHR_PER_EDUCATION_EXPERIENCE_ATTR06, HHR_PER_EDUCATION_EXPERIENCE_ATTR07, HHR_PER_EDUCATION_EXPERIENCE_ATTR08, "
        "HHR_PER_EDUCATION_EXPERIENCE_ATTR09, HHR_PER_EDUCATION_EXPERIENCE_ATTR10, HHR_PER_EDUCATION_EXPERIENCE_ATTR11, "
        "HHR_PER_EDUCATION_EXPERIENCE_ATTR12, HHR_PER_EDUCATION_EXPERIENCE_ATTR13, HHR_PER_EDUCATION_EXPERIENCE_ATTR14, "
        "HHR_PER_EDUCATION_EXPERIENCE_ATTR15 "
        "from HHR_PER_EDUCATION_EXPERIENCE_B where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")

    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_reward_data(conn, sys_code, emp_id):
    """获取奖惩信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_RP_TYPE, HHR_RP_DESC, HHR_BEGIN_DATE, HHR_RP_SCORE, HHR_RP_DEPARTMENT, "
        "HHR_RP_DETAIL, HHR_PER_REWARDS_PUNISHMENT_ATTR01, HHR_PER_REWARDS_PUNISHMENT_ATTR02, HHR_PER_REWARDS_PUNISHMENT_ATTR03, HHR_PER_REWARDS_PUNISHMENT_ATTR04, "
        "HHR_PER_REWARDS_PUNISHMENT_ATTR05, HHR_PER_REWARDS_PUNISHMENT_ATTR06, HHR_PER_REWARDS_PUNISHMENT_ATTR07, HHR_PER_REWARDS_PUNISHMENT_ATTR08, HHR_PER_REWARDS_PUNISHMENT_ATTR09, "
        "HHR_PER_REWARDS_PUNISHMENT_ATTR10, HHR_PER_REWARDS_PUNISHMENT_ATTR11, HHR_PER_REWARDS_PUNISHMENT_ATTR12, HHR_PER_REWARDS_PUNISHMENT_ATTR13, HHR_PER_REWARDS_PUNISHMENT_ATTR14, "
        "HHR_PER_REWARDS_PUNISHMENT_ATTR15 from HHR_PER_REWARDS_PUNISHMENT_B "
        "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_rela_data(conn, sys_code, emp_id):
    """获取社会关系信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_RELATIONSHIP_TYPE, HHR_DESCRIPTION, HHR_NAME, HHR_GENDER, HHR_EMERGENCY_CONTACT_PERSON, "
        "HHR_PHONE, HHR_COUNTRY, HHR_CERTIFICATE_TYPE, HHR_IDENTITY_NUM, HHR_BIRTH_DATE, "
        "HHR_PER_SOCIALRE_RELATIONSHIP_ATTR01, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR02, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR03, "
        "HHR_PER_SOCIALRE_RELATIONSHIP_ATTR04, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR05, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR06, "
        "HHR_PER_SOCIALRE_RELATIONSHIP_ATTR07, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR08, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR09, "
        "HHR_PER_SOCIALRE_RELATIONSHIP_ATTR10, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR11, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR12, "
        "HHR_PER_SOCIALRE_RELATIONSHIP_ATTR13, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR14, HHR_PER_SOCIALRE_RELATIONSHIP_ATTR15 "
        "from HHR_PER_SOCIALRE_RELATIONSHIP_B "
        "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_work_data(conn, sys_code, emp_id):
    """获取工作经历信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_EXPERIENCE_TYPE, HHR_BEGIN_DATE, HHR_END_DATE, HHR_EMPLOYER, HHR_POSITION, "
        "HHR_RELEVANT_WORK, HHR_PER_WORKEXPERIENCE_ATTR01, HHR_PER_WORKEXPERIENCE_ATTR02, HHR_PER_WORKEXPERIENCE_ATTR03, "
        "HHR_PER_WORKEXPERIENCE_ATTR04, HHR_PER_WORKEXPERIENCE_ATTR05, HHR_PER_WORKEXPERIENCE_ATTR06, HHR_PER_WORKEXPERIENCE_ATTR07, "
        "HHR_PER_WORKEXPERIENCE_ATTR08, HHR_PER_WORKEXPERIENCE_ATTR09, HHR_PER_WORKEXPERIENCE_ATTR10, HHR_PER_WORKEXPERIENCE_ATTR11, "
        "HHR_PER_WORKEXPERIENCE_ATTR12, HHR_PER_WORKEXPERIENCE_ATTR13, HHR_PER_WORKEXPERIENCE_ATTR14, HHR_PER_WORKEXPERIENCE_ATTR15 "
        "from HHR_PER_WORKEXPERIENCE_B "
        "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_lang_data(conn, sys_code, emp_id):
    """获取语言信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_LANGUAGE, HHR_LEVEL, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR01, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR02, "
        "HHR_PER_LANGUAGE_ABILITY_INFO_ATTR03, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR04, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR05, "
        "HHR_PER_LANGUAGE_ABILITY_INFO_ATTR06, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR07, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR08, "
        "HHR_PER_LANGUAGE_ABILITY_INFO_ATTR09, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR10, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR11, "
        "HHR_PER_LANGUAGE_ABILITY_INFO_ATTR12, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR13, HHR_PER_LANGUAGE_ABILITY_INFO_ATTR14, "
        "HHR_PER_LANGUAGE_ABILITY_INFO_ATTR15 from HHR_PER_LANGUAGE_ABILITY_B "
        "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_bank_data(conn, sys_code, emp_id):
    """获取银行信息"""
    data_sel = text(
        "select HHR_EMPID, HHR_SEQNUM, HHR_CARD_TYPE, HHR_COUNTRY, HHR_CURRENCY, HHR_ACCOUNT_NAME, HHR_ACCOUNT_NUMBER, "
        "HHR_BANK_ID, HHR_BANK_BRANCH_ID, HHR_PER_BANK_CARD_ATTR01, HHR_PER_BANK_CARD_ATTR02, "
        "HHR_PER_BANK_CARD_ATTR03, HHR_PER_BANK_CARD_ATTR04, HHR_PER_BANK_CARD_ATTR05, "
        "HHR_PER_BANK_CARD_ATTR06, HHR_PER_BANK_CARD_ATTR07, HHR_PER_BANK_CARD_ATTR08, "
        "HHR_PER_BANK_CARD_ATTR09, HHR_PER_BANK_CARD_ATTR10, HHR_PER_BANK_CARD_ATTR11, "
        "HHR_PER_BANK_CARD_ATTR12, HHR_PER_BANK_CARD_ATTR13, HHR_PER_BANK_CARD_ATTR14, "
        "HHR_PER_BANK_CARD_ATTR15 from HHR_PER_BANK_CARD_INFO_B "
        "where HHR_SYS_CODE = :b1 and HHR_EMPID = :b2 order by HHR_EMPID, HHR_SEQNUM ")
    rs = conn.execute(data_sel, b1=sys_code, b2=emp_id).fetchall()
    return rs


def get_emp_lst_from_audit(conn, uuid, tgt_sys_id, ifc_type, sys_code, ifc_call_dtm, last_call_dttm, lang, emp_lst,
                           table_name):
    emp_audit_sel = text("select distinct HHR_EMPID from "
                         + table_name + " where HHR_SYS_CODE = :b1 and AUDIT_TIMESTAMP > :b2 and LANG = :b3 order by HHR_EMPID")
    audit_rs = conn.execute(emp_audit_sel, b1=sys_code, b2=last_call_dttm, b3=lang).fetchall()
    for rw in audit_rs:
        emp_id = rw['HHR_EMPID']
        val_dic = {'sys_code': sys_code, 'uuid': uuid, 'tgt_sys_id': tgt_sys_id, 'ifc_type': ifc_type,
                   'ifc_key1': emp_id,
                   'ifc_call_dtm': ifc_call_dtm, 'ifc_prc_stat': 'N', 'ifc_prc_dtm': None, 'msg_txt': None}
        insert_push_log(conn, **val_dic)
        if emp_id not in emp_lst:
            emp_lst.append(emp_id)
    return emp_lst


def get_emp_del_from_log(conn, uuid, tgt_sys_id, ifc_type, sys_code, ifc_call_dtm, last_call_dttm, emp_lst):
    emp_del_log_sel = text("select distinct HHR_EMPID from HHR_DELETE_EMP_LOG where HHR_SYS_CODE = :b1 "
                           "and CREATION_DATE > :b2 and HHR_ACTION_STATUS = 'SUCESS' and HHR_DELETE_TYPE = 'PER' order by HHR_EMPID ")
    emp_del_rs = conn.execute(emp_del_log_sel, b1=sys_code, b2=last_call_dttm).fetchall()
    for rw in emp_del_rs:
        emp_id = rw['HHR_EMPID']
        val_dic = {'sys_code': sys_code, 'uuid': uuid, 'tgt_sys_id': tgt_sys_id, 'ifc_type': ifc_type,
                   'ifc_key1': emp_id,
                   'ifc_call_dtm': ifc_call_dtm, 'ifc_prc_stat': 'N', 'ifc_prc_dtm': None, 'msg_txt': None}
        insert_push_log(conn, **val_dic)
        if emp_id not in emp_lst:
            emp_lst.append(emp_id)
    return emp_lst


def ins_emp_info_del_log(conn, uuid, tgt_sys_id, ifc_type, sys_code, ifc_call_dtm, del_lst):
    """记录员工某个信息类型的所有数据的删除日志"""
    for del_tup in del_lst:
        emp_id = del_tup[0]
        orig_ifc_type = del_tup[1]
        val_dic = {'sys_code': sys_code, 'uuid': uuid, 'tgt_sys_id': tgt_sys_id, 'ifc_type': ifc_type,
                   'ifc_key1': emp_id,
                   'ifc_key2': orig_ifc_type, 'ifc_call_dtm': ifc_call_dtm, 'ifc_prc_stat': 'N', 'ifc_prc_dtm': None,
                   'msg_txt': None}
        insert_push_log(conn, **val_dic)
    return del_lst


def get_emp_rcd_del_from_log(conn, sys_code, last_call_dttm):
    # 获取删除的兼职记录(主职记录不会记录到此日志表)
    emp_del_log_sel = text("select distinct HHR_EMPID, HHR_EMP_RCD from HHR_DELETE_EMP_LOG where HHR_SYS_CODE = :b1 "
                           "and CREATION_DATE > :b2 and HHR_ACTION_STATUS = 'SUCESS' and HHR_DELETE_TYPE = 'RCD' order by HHR_EMPID, HHR_EMP_RCD ")
    emp_rcd_del_rs = conn.execute(emp_del_log_sel, b1=sys_code, b2=last_call_dttm).fetchall()
    return emp_rcd_del_rs


def invoke_dept_sync_srv(conn, sys_code, ifc_call_dtm):
    """部门数据同步接口"""

    uuid = str(uuid4())
    tgt_sys_id = 'DHSAP'
    ifc_type = 'DEPT'
    lang = 'zh_CN'
    param_name = 'TABLE_OF_ZHR_DEPT_SYN'
    client = get_soap_client(dept_wsdl_url, dept_end_point)
    param = client.factory.create(param_name)

    # 获取接口最近一次调用时间
    last_call_dttm = get_last_call_dttm(conn, sys_code, tgt_sys_id, ifc_type)

    # 记录调用日志
    insert_call_log(conn, uuid, sys_code, tgt_sys_id, ifc_type, ifc_call_dtm)

    dept_audit_sel = "select AUDIT_ID, AUDIT_TIMESTAMP, AUDIT_TRANSACTION_TYPE, HHR_DEPT_CODE, HHR_EFFE_DATE, HHR_STATUS, " \
                     "HHR_DEPT_NAME, HHR_DEPT_SHORT_NAME, HHR_DEPT_DETAIL_DESC, HHR_DEPT_SORT, HHR_DEPT_TYPE, HHR_DEPT_LEVEL, " \
                     "HHR_DEPT_HIGHER_DEPT, HHR_DEPT_MANAGER_POSITION, HHR_ORG_DEPT_ATTR01, HHR_ORG_DEPT_ATTR02, HHR_ORG_DEPT_ATTR03, " \
                     "HHR_ORG_DEPT_ATTR04, HHR_ORG_DEPT_ATTR05, HHR_ORG_DEPT_ATTR06, HHR_ORG_DEPT_ATTR07, HHR_ORG_DEPT_ATTR08, " \
                     "HHR_ORG_DEPT_ATTR09, HHR_ORG_DEPT_ATTR10, HHR_ORG_DEPT_ATTR11, HHR_ORG_DEPT_ATTR12, HHR_ORG_DEPT_ATTR13, " \
                     "HHR_ORG_DEPT_ATTR14, HHR_ORG_DEPT_ATTR15 from HHR_ORG_DEPT_B_AUDIT "

    # 获取上次未处理成功的记录
    error_rs = get_last_error_rec(conn, sys_code, tgt_sys_id, ifc_type)
    err_dept_audit_where = "where HHR_SYS_CODE = :b1 and AUDIT_ID = :b2 and LANG = :b3 order by AUDIT_TIMESTAMP asc "
    err_dept_audit_sel = text(dept_audit_sel + err_dept_audit_where)
    error_uuid_lst = []
    for err_row in error_rs:
        err_uuid = err_row['HHR_INTFC_UUID']
        if err_uuid not in error_uuid_lst:
            error_uuid_lst.append(err_uuid)
        audit_id = err_row['HHR_INTFC_KEY1']
        rw = conn.execute(err_dept_audit_sel, b1=sys_code, b2=audit_id, b3=lang).fetchone()
        param = build_dept_method_param(param, rw)

    # 从部门审计表HHR_ORG_DEPT_B_AUDIT中获取AUDIT_TIMESTAMP > last_call_dttm，且语言=zh_CN的记录
    dept_audit_where = "where HHR_SYS_CODE = :b1 and AUDIT_TIMESTAMP > :b2 and LANG = :b3 order by AUDIT_TIMESTAMP asc "
    dept_audit_sel = text(dept_audit_sel + dept_audit_where)
    audit_rs = conn.execute(dept_audit_sel, b1=sys_code, b2=last_call_dttm, b3=lang).fetchall()
    for rw in audit_rs:
        audit_id = rw['AUDIT_ID']
        dept_cd = rw['HHR_DEPT_CODE']
        val_dic = {'sys_code': sys_code, 'uuid': uuid, 'tgt_sys_id': tgt_sys_id, 'ifc_type': ifc_type,
                   'ifc_key1': audit_id,
                   'ifc_key2': dept_cd,
                   'ifc_call_dtm': ifc_call_dtm, 'ifc_prc_stat': 'N', 'ifc_prc_dtm': None, 'msg_txt': None}
        insert_push_log(conn, **val_dic)
        param = build_dept_method_param(param, rw)

    resp = client.service.ZHR_DEPT_SYN(param)
    update_ifc_push_stat(conn, uuid, resp, error_uuid_lst)


def invoke_posn_sync_srv(conn, sys_code, ifc_call_dtm):
    """职位数据同步接口"""

    uuid = str(uuid4())
    tgt_sys_id = 'DHSAP'
    ifc_type = 'POSN'
    lang = 'zh_CN'
    param_name = 'TABLE_OF_ZHR_POSN_SYN'
    client = get_soap_client(posn_wsdl_url, posn_end_point)
    param = client.factory.create(param_name)

    # 获取接口最近一次调用时间
    last_call_dttm = get_last_call_dttm(conn, sys_code, tgt_sys_id, ifc_type)

    # 记录调用日志
    insert_call_log(conn, uuid, sys_code, tgt_sys_id, ifc_type, ifc_call_dtm)

    posn_audit_sel = "select AUDIT_ID, AUDIT_TIMESTAMP, AUDIT_TRANSACTION_TYPE, HHR_POSITION_CODE, HHR_EFFE_DATE, HHR_STATUS, HHR_POSITION_NAME, " \
                     "HHR_POSITION_SHORT_NAME, HHR_POSN_DESCRIPTION, HHR_POSN_REQUIRE, HHR_POSITION_SORT, HHR_DEPT_CODE, HHR_BU_CODE, HHR_POST_CODE, " \
                     "HHR_REPORT_POSITION, HHR_RPT_POSITION, HHR_LOCATION, HHR_ORG_POSN_ATTR01, HHR_ORG_POSN_ATTR02, HHR_ORG_POSN_ATTR03, HHR_ORG_POSN_ATTR04, " \
                     "HHR_ORG_POSN_ATTR05, HHR_ORG_POSN_ATTR06, HHR_ORG_POSN_ATTR07, HHR_ORG_POSN_ATTR08, HHR_ORG_POSN_ATTR09, HHR_ORG_POSN_ATTR10, " \
                     "HHR_ORG_POSN_ATTR11, HHR_ORG_POSN_ATTR12, HHR_ORG_POSN_ATTR13, HHR_ORG_POSN_ATTR14, HHR_ORG_POSN_ATTR15 " \
                     "from HHR_ORG_POSN_B_AUDIT "

    # 获取上次未处理成功的记录
    error_rs = get_last_error_rec(conn, sys_code, tgt_sys_id, ifc_type)
    err_posn_audit_where = "where HHR_SYS_CODE = :b1 and AUDIT_ID = :b2 and LANG = :b3 order by AUDIT_TIMESTAMP asc "
    err_posn_audit_sel = text(posn_audit_sel + err_posn_audit_where)
    error_uuid_lst = []
    for err_row in error_rs:
        err_uuid = err_row['HHR_INTFC_UUID']
        if err_uuid not in error_uuid_lst:
            error_uuid_lst.append(err_uuid)
        audit_id = err_row['HHR_INTFC_KEY1']
        rw = conn.execute(err_posn_audit_sel, b1=sys_code, b2=audit_id, b3=lang).fetchone()
        param = build_posn_method_param(param, rw)

    # 从职位审计表HHR_ORG_POSN_B_AUDIT中获取AUDIT_TIMESTAMP > last_call_dttm，且语言=zh_CN的记录
    posn_audit_where = "where HHR_SYS_CODE = :b1 and AUDIT_TIMESTAMP > :b2 and LANG = :b3 order by AUDIT_TIMESTAMP asc "
    posn_audit_sel = text(posn_audit_sel + posn_audit_where)
    audit_rs = conn.execute(posn_audit_sel, b1=sys_code, b2=last_call_dttm, b3=lang).fetchall()
    for rw in audit_rs:
        audit_id = rw['AUDIT_ID']
        posn_cd = rw['HHR_POSITION_CODE']
        val_dic = {'sys_code': sys_code, 'uuid': uuid, 'tgt_sys_id': tgt_sys_id, 'ifc_type': ifc_type,
                   'ifc_key1': audit_id,
                   'ifc_key2': posn_cd,
                   'ifc_call_dtm': ifc_call_dtm, 'ifc_prc_stat': 'N', 'ifc_prc_dtm': None, 'msg_txt': None}
        insert_push_log(conn, **val_dic)
        param = build_posn_method_param(param, rw)

    resp = client.service.ZHR_POSN_SYN(param)
    update_ifc_push_stat(conn, uuid, resp, error_uuid_lst)


def invoke_job_data_sync_srv(conn, sys_code, ifc_call_dtm, del_lst):
    """任职信息数据同步接口"""

    uuid = str(uuid4())
    tgt_sys_id = 'DHSAP'
    ifc_type = 'JOB'
    lang = 'zh_CN'
    param_name = 'TABLE_OF_ZHHR_JOBDATA_LINE'
    client = get_soap_client(job_wsld_url, job_end_point)
    param = client.factory.create(param_name)

    # 获取接口最近一次调用时间
    last_call_dttm = get_last_call_dttm(conn, sys_code, tgt_sys_id, ifc_type)

    # 记录调用日志
    insert_call_log(conn, uuid, sys_code, tgt_sys_id, ifc_type, ifc_call_dtm)

    emp_lst = []

    # 获取上次未处理成功的记录
    error_rs = get_last_error_rec(conn, sys_code, tgt_sys_id, ifc_type)
    error_uuid_lst = []
    for err_row in error_rs:
        err_uuid = err_row['HHR_INTFC_UUID']
        if err_uuid not in error_uuid_lst:
            error_uuid_lst.append(err_uuid)
        emp_id = err_row['HHR_INTFC_KEY1']
        emp_rcd = err_row['HHR_INTFC_KEY2']
        # 所有兼职记录只处理一次，统一设置为2
        if emp_rcd != 1:
            emp_rcd = 2
        emp_tup = (emp_id, emp_rcd)
        if emp_tup not in emp_lst:
            emp_lst.append(emp_tup)

    # 从审计表中获取人员
    job_audit_sel = text("select distinct HHR_EMPID, HHR_EMP_RCD from HHR_PER_JOBDATA_B_AUDIT "
                         "where HHR_SYS_CODE = :b1 and AUDIT_TIMESTAMP > :b2 and LANG = :b3 order by HHR_EMPID, HHR_EMP_RCD ")
    audit_rs = conn.execute(job_audit_sel, b1=sys_code, b2=last_call_dttm, b3=lang).fetchall()
    for rw in audit_rs:
        emp_id = rw['HHR_EMPID']
        emp_rcd = rw['HHR_EMP_RCD']
        val_dic = {'sys_code': sys_code, 'uuid': uuid, 'tgt_sys_id': tgt_sys_id, 'ifc_type': ifc_type,
                   'ifc_key1': emp_id,
                   'ifc_key2': emp_rcd,
                   'ifc_call_dtm': ifc_call_dtm, 'ifc_prc_stat': 'N', 'ifc_prc_dtm': None, 'msg_txt': None}
        insert_push_log(conn, **val_dic)
        if emp_rcd != 1:
            emp_rcd = 2
        emp_tup = (emp_id, emp_rcd)
        if emp_tup not in emp_lst:
            emp_lst.append(emp_tup)

    # 从删除兼职日志记录表中获取人员
    emp_rcd_del_rs = get_emp_rcd_del_from_log(conn, sys_code, last_call_dttm)
    for rcd_row in emp_rcd_del_rs:
        emp_id = rcd_row['HHR_EMPID']
        emp_rcd = rcd_row['HHR_EMP_RCD']
        if emp_rcd != 1:
            emp_rcd = 2
        emp_tup = (emp_id, emp_rcd)
        if emp_tup not in emp_lst:
            emp_lst.append(emp_tup)

    # 处理每一个满足条件的人员任职数据
    for emp_tup in emp_lst:
        emp_id = emp_tup[0]
        emp_rcd = emp_tup[1]
        # 对有主职更新（任职记录号=1）的人员编号，从hhr_org_per_jobdata中获取此员工的所有主职记录
        if emp_rcd == 1:
            job_rs = get_job_data(conn, sys_code, emp_id, emp_rcd, 'M')
        else:
            job_rs = get_job_data(conn, sys_code, emp_id, emp_rcd, 'P')
            # 如果该员工所有兼职数据都已经被删除了，则记录到删除列表中
            if len(job_rs) == 0 or job_rs is None:
                del_tup = (emp_id, ifc_type)
                if del_tup not in del_lst:
                    del_lst.append(del_tup)
        for job_rw in job_rs:
            param = build_job_method_param(param, job_rw)
    resp = client.service.ZHR_JOBDATA_SYN(param)
    update_ifc_push_stat(conn, uuid, resp, error_uuid_lst)


def invoke_pers_sync_srv(conn, sys_code, ifc_type, ifc_call_dtm, del_lst):
    """个人信息数据同步总接口"""

    uuid = str(uuid4())
    tgt_sys_id = 'DHSAP'
    lang = 'zh_CN'
    client = param_name = soap_method = build_param_handle = get_data_handle = audit_table_name = ''

    # 职称信息
    if ifc_type == 'TITL':
        param_name = 'TABLE_OF_ZHHR_POSTTITLE_TEXT'
        client = get_soap_client(pos_title_wsdl_url, pos_title_end_point)
        soap_method = client.service.ZHR_POSTTITLE_SYN
        build_param_handle = build_pos_title_method_param
        get_data_handle = get_pos_title_data
        audit_table_name = "HHR_PER_POSTTITLE_B_AUDIT"
    # 基本信息
    elif ifc_type == 'BIO':
        param_name = 'TABLE_OF_ZHHR_BIOGRAPHICAL_TEXT'
        client = get_soap_client(bio_wsdl_url, bio_end_point)
        soap_method = client.service.ZHR_BIOGRAPHICAL_SYN
        build_param_handle = build_bio_method_param
        get_data_handle = get_bio_data
        audit_table_name = "HHR_PER_BIOGRAPHICAL_B_AUDIT"
    # 身份信息
    elif ifc_type == 'IDEN':
        param_name = 'TABLE_OF_ZHHR_IDENTITY_TEXT'
        client = get_soap_client(iden_wsdl_url, iden_end_point)
        soap_method = client.service.ZHR_IDENTITY_SYN
        build_param_handle = build_identity_method_param
        get_data_handle = get_identity_data
        audit_table_name = "HHR_PER_IDENTITY_B_AUDIT"
    # 地址信息
    elif ifc_type == 'ADDR':
        param_name = 'TABLE_OF_ZHHR_ADDRESS_TEXT'
        client = get_soap_client(addr_wsdl_url, addr_end_point)
        soap_method = client.service.ZHR_ADDRESS_SYN
        build_param_handle = build_addr_method_param
        get_data_handle = get_addr_data
        audit_table_name = "HHR_PER_ADDRESS_B_AUDIT"
    # 档案信息
    elif ifc_type == 'ARCH':
        param_name = 'TABLE_OF_ZHHR_ARCHIVES_TEXT'
        client = get_soap_client(arch_wsdl_url, arch_end_point)
        soap_method = client.service.ZHR_ARCHIVES_SYN
        build_param_handle = build_arch_method_param
        get_data_handle = get_arch_data
        audit_table_name = "HHR_PER_ARCHIVES_B_AUDIT"
    # 证件信息
    elif ifc_type == 'CERT':
        param_name = 'TABLE_OF_ZHHR_CERTIFICATE_TEXT'
        client = get_soap_client(cert_wsdl_url, cert_end_point)
        soap_method = client.service.ZHR_CERTIFICATE_SYN
        build_param_handle = build_cert_method_param
        get_data_handle = get_cert_data
        audit_table_name = "HHR_PER_CERTIFICATE_B_AUDIT"
    # 试用期信息
    elif ifc_type == 'PROB':
        param_name = 'TABLE_OF_ZHHR_PROBATION_TEXT'
        client = get_soap_client(prob_wsdl_url, prob_end_point)
        soap_method = client.service.ZHR_PROBATION_SYN
        build_param_handle = build_prob_method_param
        get_data_handle = get_prob_data
        audit_table_name = "HHR_PER_PROBATION_B_AUDIT"
    # 合同信息
    elif ifc_type == 'CONT':
        param_name = 'TABLE_OF_ZHHR_CONTRACT_TEXT'
        client = get_soap_client(cont_wsdl_url, cont_end_point)
        soap_method = client.service.ZHR_CONTRACT_SYN
        build_param_handle = build_cont_method_param
        get_data_handle = get_cont_data
        audit_table_name = "HHR_PER_CONTRACT_B_AUDIT"
    # 协议信息
    # elif ifc_type == 'PROT':
    #     param_name = 'TABLE_OF_ZHHR_PROTOCOL_TEXT'
    #     client = get_soap_client(prot_wsdl_url, prot_end_point)
    #     soap_method = client.service.ZHR_PROTOCOL_SYN
    #     build_param_handle = build_prot_method_param
    #     get_data_handle = get_prot_data
    #     audit_table_name = "HHR_PER_PROTOCOL_B_AUDIT"
    # 证书信息
    elif ifc_type == 'CRED':
        param_name = 'TABLE_OF_ZHHR_CREDENTIALS_TEXT'
        client = get_soap_client(cred_wsdl_url, cred_end_point)
        soap_method = client.service.ZHR_CREDENTIALS_SYN
        build_param_handle = build_cred_method_param
        get_data_handle = get_cred_data
        audit_table_name = "HHR_PER_CREDENTIALS_B_AUDIT"
    # 绩效信息
    elif ifc_type == 'PERF':
        param_name = 'TABLE_OF_ZHHR_ACHIEVEMENTS'
        client = get_soap_client(perf_wsdl_url, perf_end_point)
        soap_method = client.service.ZHR_PER_ACHIEVEMENTS_SYN
        build_param_handle = build_perf_method_param
        get_data_handle = get_perf_data
        audit_table_name = "HHR_PER_ACHIEVEMENTS_B_AUDIT"
    # 培训信息
    elif ifc_type == 'TRAN':
        param_name = 'TABLE_OF_ZHHR_TRAININFO'
        client = get_soap_client(tran_wsdl_url, tran_end_point)
        soap_method = client.service.ZHR_PER_TRAININFO_SYN
        build_param_handle = build_tran_method_param
        get_data_handle = get_tran_data
        audit_table_name = "HHR_PER_TRAININFO_B_AUDIT"
    # 电话信息
    elif ifc_type == 'PHON':
        param_name = 'TABLE_OF_ZHHR_PHONE_TEXT'
        client = get_soap_client(phone_wsdl_url, phone_end_point)
        soap_method = client.service.ZHR_PHONE_SYN
        build_param_handle = build_phone_method_param
        get_data_handle = get_phone_data
        audit_table_name = "HHR_PER_PHONE_B_AUDIT"
    # 邮箱信息
    elif ifc_type == 'MAIL':
        param_name = 'TABLE_OF_ZHHR_MAIL_TEXT'
        client = get_soap_client(mail_wsdl_url, mail_end_point)
        soap_method = client.service.ZHR_MAIL_SYN
        build_param_handle = build_mail_method_param
        get_data_handle = get_mail_data
        audit_table_name = "HHR_PER_EMAIL_B_AUDIT"
    # 教育信息
    elif ifc_type == 'EDU':
        param_name = 'TABLE_OF_ZHHR_EDUCATION_EXPERIENCE_TEXT'
        client = get_soap_client(edu_wsdl_url, edu_end_point)
        soap_method = client.service.ZHR_EDUCATION_EXPERIENCE_SYN
        build_param_handle = build_edu_method_param
        get_data_handle = get_edu_data
        audit_table_name = "HHR_PER_EDUCATION_EXPERIENCE_B_AUDIT"
    # 奖惩信息
    elif ifc_type == 'RWRD':
        param_name = 'TABLE_OF_ZHHR_REWARDS_PUNISHMENT_TEXT'
        client = get_soap_client(reward_wsdl_url, reward_end_point)
        soap_method = client.service.ZHR_REWARDS_PUNISHMENT_SYN
        build_param_handle = build_reward_method_param
        get_data_handle = get_reward_data
        audit_table_name = "HHR_PER_REWARDS_PUNISHMENT_B_AUDIT"
    # 社会关系信息
    elif ifc_type == 'RELA':
        param_name = 'TABLE_OF_ZHHR_RELATIONSHIP_TEXT'
        client = get_soap_client(rela_wsdl_url, rela_end_point)
        soap_method = client.service.ZHR_SOCIALRE_RELATIONSHIP_SYN
        build_param_handle = build_rela_method_param
        get_data_handle = get_rela_data
        audit_table_name = "HHR_PER_SOCIALRE_RELATIONSHIP_B_AUDIT"
    # 工作经历信息
    elif ifc_type == 'WORK':
        param_name = 'TABLE_OF_ZHHR_WORKEXPERIENCE_TEXT'
        client = get_soap_client(work_wsdl_url, work_end_point)
        soap_method = client.service.ZHR_WORKEXPERIENCE_SYN
        build_param_handle = build_work_method_param
        get_data_handle = get_work_data
        audit_table_name = "HHR_PER_WORKEXPERIENCE_B_AUDIT"
    # 语言信息
    elif ifc_type == 'LANG':
        param_name = 'TABLE_OF_ZHHR_PER_LANGUAGE_ABILITY_B'
        client = get_soap_client(lang_wsdl_url, lang_end_point)
        soap_method = client.service.ZHR_LANGUAGE_ABILITY_SYN
        build_param_handle = build_lang_method_param
        get_data_handle = get_lang_data
        audit_table_name = "HHR_PER_LANGUAGE_ABILITY_B_AUDIT"
    # 银行信息
    elif ifc_type == 'BANK':
        param_name = 'TABLE_OF_ZHHR_PER_BANK_CARD_INFO_B'
        client = get_soap_client(bank_wsdl_url, bank_end_point)
        soap_method = client.service.ZHHR_BANK_CARD_INFO_SYN
        build_param_handle = build_bank_method_param
        get_data_handle = get_bank_data
        audit_table_name = "HHR_PER_BANK_CARD_INFO_B_AUDIT"
    # 删除人员所有数据
    elif ifc_type == 'DEL':
        param_name = 'TABLE_OF_ZHHR_PERSON_DELETE_S'
        client = get_soap_client(del_wsdl_url, del_end_point)
        soap_method = client.service.ZHHR_PERSON_DELETE_SYN
    # 删除单个人员某个信息类型
    elif ifc_type == 'IFDL':
        param_name = 'TABLE_OF_ZHHR_INFO_DELETE_S'
        client = get_soap_client(info_del_wsdl_url, info_del_end_point)
        soap_method = client.service.ZHR_INFO_DELETE_SYN

    param = client.factory.create(param_name)

    # 获取接口最近一次调用时间
    last_call_dttm = get_last_call_dttm(conn, sys_code, tgt_sys_id, ifc_type)

    # 记录调用日志
    insert_call_log(conn, uuid, sys_code, tgt_sys_id, ifc_type, ifc_call_dtm)

    emp_lst = []

    # 获取上次未处理成功的记录
    error_rs = get_last_error_rec(conn, sys_code, tgt_sys_id, ifc_type)
    error_uuid_lst = []
    for err_row in error_rs:
        err_uuid = err_row['HHR_INTFC_UUID']
        if err_uuid not in error_uuid_lst:
            error_uuid_lst.append(err_uuid)
        emp_id = err_row['HHR_INTFC_KEY1']
        if emp_id not in emp_lst:
            emp_lst.append(emp_id)

    # 从审计表中获取人员
    if ifc_type not in ['DEL', 'IFDL']:
        emp_lst = get_emp_lst_from_audit(conn, uuid, tgt_sys_id, ifc_type, sys_code, ifc_call_dtm, last_call_dttm, lang,
                                         emp_lst, audit_table_name)
        for emp_id in emp_lst:
            rs = get_data_handle(conn, sys_code, emp_id)
            # 如果该员工的此类型数据都已经被删除了，则记录到删除列表中
            if len(rs) == 0 or rs is None:
                del_tup = (emp_id, ifc_type)
                if del_tup not in del_lst:
                    del_lst.append(del_tup)
            for rw in rs:
                param = build_param_handle(param, rw)
    # 从删除日志表中获取人员
    elif ifc_type == 'DEL':
        emp_lst = get_emp_del_from_log(conn, uuid, tgt_sys_id, ifc_type, sys_code, ifc_call_dtm, last_call_dttm,
                                       emp_lst)
        for emp_id in emp_lst:
            d = {'HHR_EMPID': emp_id, 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '', 'NUM': ''}
            param.item.append(d)
    elif ifc_type == 'IFDL':
        ins_emp_info_del_log(conn, uuid, tgt_sys_id, ifc_type, sys_code, ifc_call_dtm, del_lst)
        for del_tup in del_lst:
            emp_id = del_tup[0]
            orig_ifc_type = del_tup[1]
            d = {'HHR_EMPID': emp_id, 'HHR_INFO': orig_ifc_type, 'REC_DATE': None, 'REC_TIME': None, 'TYPE': '',
                 'NUM': ''}
            param.item.append(d)
    resp = soap_method(param)
    update_ifc_push_stat(conn, uuid, resp, error_uuid_lst)
