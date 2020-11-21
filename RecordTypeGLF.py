#!/usr/bin/env python
# funzioni per leggere record file GLF
import Logger
import MainframeFieldsUtility

logger = Logger.getLogger()


def map_record_type_06(stringa):
    current_date = stringa[42:50].strip().rstrip('\n').rstrip('\r\n')
    data_file = current_date[4:8] + current_date[2:4] + current_date[0:2]

    fields = {
        "SRT-RECORD-TYPE": stringa[0:2].encode('utf-8').strip(),
        "SPS-AC-CO-NR": stringa[2:7].encode('utf-8').strip(),
        #        "SPS-OWNR1-CD" : stringa[7:12].encode('utf-8').strip(),
        #        "SPS-OWNR2-CD" : stringa[12:17].encode('utf-8').strip(),
        #        "SPS-OWNR3-CD" : stringa[17:22].encode('utf-8').strip(),
        "LYL-PGM-ID": stringa[22:27].encode('utf-8').strip(),
        #        "LYL-MEMB-ID" : stringa[27:42].encode('utf-8').strip(),
        "CURRENT-DATE": current_date,
        "data_file": data_file,
        "internallog_ts": data_file + '0000000'
    }
    return fields


def map_record_type_18(stringa):
    lyl_added = stringa[226:241]
    lyl_added_handler = MainframeFieldsUtility.sign_Handler(lyl_added)
    lyl_added_segno = lyl_added_handler['segno']
    lyl_added_valore = lyl_added_handler['valore'][:-2].lstrip('0')
    if lyl_added_valore == '':
        lyl_added_valore = '0'

    fields = {
        "SRT-RECORD-TYPE": stringa[0:2].encode('utf-8').strip(),
        "SPS-AC-CO-NR": stringa[2:7].encode('utf-8').strip(),
        #        "SPS-OWNR1-CD" : stringa[7:12].encode('utf-8').strip(),
        #        "SPS-OWNR2-CD" : stringa[12:17].encode('utf-8').strip(),
        #        "SPS-OWNR3-CD" : stringa[17:22].encode('utf-8').strip(),
        "LYL-PGM-ID": stringa[22:27].encode('utf-8').strip(),
        #        "LYL-MEMB-ID" : stringa[27:42].encode('utf-8').strip(),
        "SRT-AC-CD": stringa[42:65].encode('utf-8').strip(),
        "LY-EFF-DT": stringa[65:72].strip(),
        "LY-EXP-DT": stringa[72:79].strip(),
        #        "PRI-CUST-NAME" : stringa[79:119].encode('utf-8').strip(),
        #        "STREET-ADDRESS-0" : stringa[119:159].encode('utf-8').strip(),
        #        "CITY" : stringa[159:184].encode('utf-8').strip(),
        #        "STATE" : stringa[184:186].encode('utf-8').strip(),
        #        "ZIP-CODE" : stringa[186:195].encode('utf-8').strip(),
        #        "COUNTRY-CODE-2" : stringa[195:197].encode('utf-8').strip(),
        "SN-CYC-END-DT": stringa[197:204].strip(),
        "sn_cyc_end_dt_format_yyyyMMdd": MainframeFieldsUtility.read_CYYMMDD_date(stringa[197:204].strip()),
        "AWD-CYC-DT": stringa[204:211].strip(),
        "awd-cyc-dt_format_yyyyMMdd": MainframeFieldsUtility.read_CYYMMDD_date(stringa[204:211].strip()),

        #        "LYL-BEG-BAL-AM" : stringa[211:226].strip(),
        "LYL-BEG-BAL-AM": stringa[211:226].strip(),
        "LYL-ADDED": lyl_added,
        "lyl_added_handler": lyl_added_handler,
        "virtual_credit_regular": eval(lyl_added_segno + lyl_added_valore),
        #        "LYL-REDEEMED" : stringa[241:256].encode('utf-8').strip(),
        #        "LYL-DR-ADJ-AM" : stringa[256:271].encode('utf-8').strip(),
        #        "LYL-CR-ADJ-AM" : stringa[271:286].encode('utf-8').strip(),
        "LYL-END-BAL-AM": stringa[286:301].strip(),
        "LH-CYC-REG-AM": stringa[301:316].strip(),
        #        "LH-CYC-BNS-AM" : stringa[316:331].encode('utf-8').strip(),
        "LH-EXP-IN": stringa[331:332].strip(),
        #        "LYL-PER-REG-AM" : stringa[332:347].encode('utf-8').strip(),
        #        "LY-PR-END-BAL-AM" : stringa[347:362].encode('utf-8').strip(),
        #        "LYL-CUR-EXP-AM" : stringa[362:377].encode('utf-8').strip().rstrip('\n').rstrip('\r\n')
        "FILLER": ''

    }
    return fields


def map_record_type_26(stringa):
    txn_dt = stringa[88:95].strip()
    txn_curr_cd = stringa[112:115].encode('utf-8').strip()

    jo_reg_pnt_am = stringa[130:145]
    jo_reg_pnt_am_handler = MainframeFieldsUtility.sign_Handler(jo_reg_pnt_am)
    jo_reg_pnt_am_segno = jo_reg_pnt_am_handler['segno']
    jo_reg_pnt_am_valore = jo_reg_pnt_am_handler['valore'][:-2].lstrip('0')
    if jo_reg_pnt_am_valore == '':
        jo_reg_pnt_am_valore = '0'

    srt_tran_pstg_dt = stringa[161:168].strip().rstrip('\n').rstrip('\r\n')

    fields = {
        "SRT-RECORD-TYPE": stringa[0:2].encode('utf-8').strip(),
        "SPS-AC-CO-NR": stringa[2:7].encode('utf-8').strip(),
        #        "SPS-OWNR1-CD" : stringa[7:12].encode('utf-8').strip(), # nome del PGM, come RECUCARD, non interessante?
        #        "SPS-OWNR2-CD" : stringa[12:17].encode('utf-8').strip(), # valore P o N
        #        "SPS-OWNR3-CD" : stringa[17:22].encode('utf-8').strip(),
        "LYL-PGM-ID": stringa[22:27].encode('utf-8').strip(),
        #        "LYL-MEMB-ID" : stringa[27:42].encode('utf-8').strip(),
        "SRT-AC-CD": stringa[42:65].encode('utf-8').strip(),
        "JO-SYS-GEN-REF-NR": stringa[65:88].encode('utf-8').strip(),
        "TXN-DT": txn_dt,
        "transaction_date": MainframeFieldsUtility.read_CYYMMDD_date(txn_dt),
        "JO-PST-AM": stringa[95:110].strip(),
        "CATG-CD": stringa[110:112].encode('utf-8').strip(),
        "TXN-CURR-CD": txn_curr_cd,  # necessaria trascodifica 978 = EUR ?
        "currency_cod": MainframeFieldsUtility.translate_currency(txn_curr_cd),
        "FGN-TR-AM": stringa[115:130].strip(),
        "JO-REG-PNT-AM": jo_reg_pnt_am,
        "jo_reg_pnt_am_handler": jo_reg_pnt_am_handler,
        "jo_reg_pnt_am_segno": jo_reg_pnt_am_segno,
        "jo_reg_pnt_am_valore": jo_reg_pnt_am_valore,
        "virtual_credit_regular": eval(jo_reg_pnt_am_segno + jo_reg_pnt_am_valore),

        #        "JO-BNS-PNT-AM" : stringa[145:160].encode('utf-8').strip(),
        #        "JO-LYL-DELQ-IN" : stringa[160:161].encode('utf-8').strip(),
        "SRT-TRAN-PSTG-DT": srt_tran_pstg_dt,
        "srt_tran_pstg_dt_format_yyyyMMdd": MainframeFieldsUtility.read_CYYMMDD_date(srt_tran_pstg_dt)

    }
    return fields
