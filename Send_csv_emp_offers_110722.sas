%include "/sas/ma/env/autoexec.sas" / ENCODING='UTF-8';

PROC SQL;
    &ConnectToOra.;
    create table work.t_emp_offers_915 as 
        select *       
            from connection to ora          
                (
select Distinct

    sysdate date_export,
    cl.id_cuid
    ,cl.name_full
     ,''''||cl.text_identity_card_iin iin
       ,mp.product_group
       ,o.offer_type_code
       ,o.offer_status
       ,clp.date1                 date_registration
       ,(case when cl.skp_client in (select cca.skp_client from cdm.ma_card_params cp 
       join cmdm.ft_contract_card_ad cca on cp.skp_credit_case = cca.skp_credit_case and cca.code_card_status in ('A','I','N') 
       and cp.char10 = 'Y') then 'Y' else 'N' end)                flag_salary_card
       ,cl.flag_employee
       ,(case when cl.id_cuid in (select l.party_id from ofs.limits l where l.limit_type_code in ('Cash_online','Cash_offline')) then 'Y'
       else 'N' end) as flag_cash_xs_limits 
       ,(case when cl.id_cuid in (select l.party_id from ofs.limits l where l.limit_type_code in ('CE_Cash_online')) then 'Y'
       else 'N' end) as flag_ce_cash_xs_limits 
       , (case when cl.id_cuid in (select l.party_id from ofs.limits l where l.limit_type_code in ('Refinance_online','Refinance_offline')) then 'Y'
       else 'N' end) as flag_ref_xs_limits 
       , (case when cl.id_cuid in (select l.party_id from ofs.limits l where l.limit_type_code in ('acl_cash_mapp_online')) then 'Y'
       else 'N' end) as flag_cash_mapp_wi_limits 
       , (case when cl.id_cuid in (select l.party_id from ofs.limits l where l.limit_type_code in ('acl_ref_online')) then 'Y'
       else 'N' end) as flag_ref_wi_limits 
       , (case when cl.id_cuid in (select l.party_id from ofs.limits l where l.limit_type_code in ('Cash_online','Cash_offline','CE_Cash_online','Refinance_online','Refinance_offline','acl_cash_mapp_online','acl_ref_online')) then 'Y'
       else 'N' end) as flag_any_limits 
      
  from cmdm.ft_contract_card_ad cca
  join cmdm.ft_client_ad cl
    on cca.skp_client = cl.skp_client and cca.code_card_status in ('A','I','N')
   and cl.flag_employee = 'Y'
  left join cdm.ma_client_params clp
    on cl.id_cuid = clp.id_cuid
  and clp.date1 < trunc(sysdate) - 90
  left join cdm.ma_card_params ccp
    on ccp.skp_credit_case = cca.skp_credit_case and ccp.char10 = 'Y'
  left join ofs.offer o on cl.id_cuid = o.party_id and o.offer_status = 'ACTIVE' and not (lower(o.offer_type_code) like ('%ard%') or lower(o.offer_type_code) like ('virtual%'))
  left join cdm.ma_participant mp on mp.participant_id = o.offer_id_sas and mp.deactivation_dttm > trunc(sysdate) + 1 and mp.product_group in ('WI_CB_ON_EMP', 'XSELL_CB_EMP','WI_CB_REF_EMP', 'XSELL_CB_REF_EMP')




);
    disconnect from ora;
QUIT;

filename fileout "/sas/sas/data/import/share/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_offers.CSV" encoding="utf-8";

proc export data=WORK.t_emp_offers_915 dbms=csv replace outfile=fileout;
run;


/* --------------------------------------------------*/
filename fileout "/sas/ma/data/models/reports/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_offers.CSV" encoding="utf-8";

proc export data=work.t_emp_offers_915 dbms=csv replace outfile=fileout;
run;

ods listing close;

%let v_email_address_from = 'Zhalgas.Zhienbekov@homecredit.kz';

/* -------------------------------------------------------------------------------------- */
/* Sent e-mail */
filename outbox email 
/*to=("Zhalgas.Zhienbekov@homecredit.kz", "sergey.korniyenko@homecredit.kz","viktoriya.polonskaya@homecredit.kz","assyl.junussova@homecredit.kz")*/
to=("Zhalgas.Zhienbekov@homecredit.kz")

from= 'zhalgas.zhienbekov@homecredit.kz'
subject="List of eligible employees on %sysfunc(intnx(day,%sysfunc(date()), 0),ddmmyyp10.)"
encoding="utf-8"
attach=("/sas/ma/data/models/reports/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_offers.CSV" content_type="application/vnd.ms-excel")
type="text/html"
;

ods html body=outbox;
ods path work.tmp(update) sasuser.templat(update) sashelp.tmplmst(read);
ods html close;