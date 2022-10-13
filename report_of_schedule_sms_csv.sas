/*last calculation*/





/*creating temporary table*/



%include "/sas/ma/env/autoexec.sas" / ENCODING='UTF-8';

%let mvTempValue= %mGetUniqueSuffix;

data ma_temp.t4xs_&mvTempValue.;
set mareport.T_SK_DAILY_WF_SMS_XS;
run;

PROC SQL;
&ConnectToOra.;
execute ( grant select on ma_temp.t4xs_&mvTempValue. to ROLE_BUSINESS_USERS ) by ora;
disconnect from ora;
quit;



/*Formulating the schedule of sms*/


%include "/sas/ma/env/autoexec.sas" / ENCODING='UTF-8';

PROC SQL;
    &ConnectToOra.;
    create table work.t_zz_report_sms as 
        select *       
            from connection to ora          
                (

with steps as
 (select cl.id_cuid,
         cl.skp_client,
         ptb.ptb_decile_cash as num_dec_ptb,
         pta.code_risk_grade as code_risk_grade,
         case
           when ptb.ptb_decile_cash in (1, 2, 3) then
            '1,2,3'
           when ptb.ptb_decile_cash in (4, 5, 6) then
            '4,5,6'
           when ptb.ptb_decile_cash in (7, 8, 9, 10) then
            '7,8,9,10'
           else
            ''
         end as code_dec_ptb,
         case
           when pta.code_risk_grade in ('A', 'B', 'C', 'A+') then
            'A,B,C'
           when pta.code_risk_grade in ('D', 'E', 'F') then
            'D,E,F'
           when pta.code_risk_grade in ('G', 'H') then
            'G,H'
           else
            ''
         end as code_risk_group,
         trunc(h.dtime_last_xs_sms) as date_last_xs_sms,
         greatest(trunc(nvl(h.dtime_last_xs_sms + 14, sysdate + 0)),
                  trunc(sysdate + 0)) as date_4next_xs_sms,
         case
           when cl.date_last_event between trunc(sysdate) - 60 and sysdate then
            'Y'
           else
            'N'
         end as flag_has_mb,
         t.SEGMENT_NAME
    from ma_temp.t4xs_&mvTempValue. /*ma_temp.t_tmp_sms_step3*/ t
    join cmdm.ft_client_ad cl
      on cl.skp_client = t.skp_client
    left join maimport.t_mo_ptb_decile3 ptb
      on ptb.skp_client = t.skp_client
  /*left*/
    join maimport.t_sk_pta_decile pta
      on pta.party_id = cl.id_cuid
    left join maimport.t_sas_dm_cl_comm_dm h
      on h.skp_client = t.skp_client),
m as
 (select ID_CUID,
         max(SKP_CLIENT) as SKP_CLIENT,
         max(NUM_DEC_PTB) as NUM_DEC_PTB,
         max(CODE_RISK_GRADE) as CODE_RISK_GRADE,
         max(CODE_DEC_PTB) as CODE_DEC_PTB,
         max(CODE_RISK_GROUP) as CODE_RISK_GROUP,
         max(DATE_LAST_XS_SMS) as DATE_LAST_XS_SMS,
         max(DATE_4NEXT_XS_SMS) as DATE_4NEXT_XS_SMS,
         max(FLAG_HAS_MB) as FLAG_HAS_MB,
         max(SEGMENT_NAME) as PROD_SEGMENT_NAME
    from steps
   group by ID_CUID)

select NUM_DEC_PTB,
       CODE_RISK_GROUP,
       code_risk_grade,
       code_dec_ptb,
       date_4next_xs_sms,
       flag_has_mb,
       PROD_SEGMENT_NAME,
       PROD_SEGMENT_NAME || '_' || CODE_RISK_GROUP || '_' || code_dec_ptb as name_segment,
       CODE_RISK_GROUP || '_' || code_dec_ptb as name_segment_ptbrg,
       count(1) as kk
  from m
 group by code_risk_grade,
          code_dec_ptb,
          NUM_DEC_PTB,
          CODE_RISK_GROUP,
          date_4next_xs_sms,
          flag_has_mb,
          PROD_SEGMENT_NAME
 order by kk desc




);
    disconnect from ora;
QUIT;



filename fileout "/sas/sas/data/import/share/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_sms_schedule.CSV" encoding="utf-8";

proc export data=work.t_zz_report_sms dbms=csv replace outfile=fileout;
run;


/* --------------------------------------------------*/
filename fileout "/sas/ma/data/models/reports/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_sms_schedule.CSV" encoding="utf-8";

proc export data=work.t_zz_report_sms dbms=csv replace outfile=fileout;
run;

ods listing close;

%let v_email_address_from = 'Zhalgas.Zhienbekov@homecredit.kz';

/* -------------------------------------------------------------------------------------- */
/* Sent e-mail */
filename outbox email 
/*to=("Zhalgas.Zhienbekov@homecredit.kz", "sergey.korniyenko@homecredit.kz","viktoriya.polonskaya@homecredit.kz","assyl.junussova@homecredit.kz")*/
to=("Zhalgas.Zhienbekov@homecredit.kz")

from= 'zhalgas.zhienbekov@homecredit.kz'
subject="Daily schedule for SMS on %sysfunc(intnx(day,%sysfunc(date()), 0),ddmmyyp10.)"
encoding="utf-8"
attach=("/sas/ma/data/models/reports/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_sms_schedule.CSV" content_type="application/vnd.ms-excel")
type="text/html"
;

ods html body=outbox;
ods path work.tmp(update) sasuser.templat(update) sashelp.tmplmst(read);
ods html close;