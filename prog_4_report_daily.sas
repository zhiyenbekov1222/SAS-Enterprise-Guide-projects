ods _ALL_ close;
%let v_email_address_from = 'zhalgas.zhienbekov@homecredit.kz';


%include "/sas/ma/env/autoexec.sas" / ENCODING='UTF-8';
%include "/sas/ma/env/scripts/logging/ProcessLogging.sas";
options nofmterr;
%let CURRENT_GPATH = %sysfunc(pathname(work));
ods listing GPATH="&CURRENT_GPATH";
%let HCIKZ_LIB=hcikz;
%let HCIKZ_SCHEMA=hcikz;
%let HCIKZ_LIBNAME = libname &HCIKZ_LIB ORACLE SCHEMA=&HCIKZ_SCHEMA PATH=&CMDM_PATH AUTHDOMAIN=OraAuth dbserver_max_bytes=2 dbclient_max_bytes=1 dbclient_encoding_fixed=yes  INSERTBUFF=10000  READBUFF=10000;
&HCIKZ_LIBNAME.;
&MATEMP_LIBNAME.;

%macro mReportForOffers;

%local mvAction_id mvActionType CAMPCODE;
%let mvActionType = DAILY_REPORT_FOR_OFFERS_JOB;
%let CAMPCODE = stp_proc;

%mInitProcessAction(mvlOutActionID = mvAction_id,mvlActionType = &mvActionType, mvlisTest = N);
%if %MA_NOT_OK %then %GOTO ERROR_EXIT;


/*DAILY SENT SMS*/

proc sql;

	&ConnectToOra.;

	create table work.sent_daily_sms as
	select * from connection to ora

( 
with t1 as
 (select distinct mo.id_cuid,
                  mo.channel_cd,
                  max(mo.create_dttm) over() create_dttm,
                  mo.sms_type,
                  mo.push_type,
                  mo.sms_category,
                  mo.push_category,
                  mo.tpl_cd
    from cdm.ma_offer mo
   where mo.create_dttm > trunc(sysdate)
     and mo.channel_cd = 'NTF' and mo.push_category in ('X-SELL','WALKIN')) ,
     
t2 as
 (select (create_dttm), count(*) cnt_all from t1 group by create_dttm),

t3 as
 (select create_dttm,
         channel_cd,
         sms_type,
         sms_category,
         push_type,
         push_category,
         tpl_cd,
         count(*) cnt_by_segment
    from t1
   group by create_dttm,sms_category, sms_type, push_category, push_type,  channel_cd, tpl_cd)


select t3.create_dttm,
       t3.channel_cd,
       t3.push_category,
       t3.sms_category,
       t3.tpl_cd,
       t3.cnt_by_segment,
       t2.cnt_all
  from t2
  join t3
    on t2.create_dttm = t3.create_dttm

	);

disconnect from ora;
quit;




/*REPORT OF UPLOAD FOR TLM*/

proc sql;

	&ConnectToOra.;

	create table work.upload_tlm as
	select * from connection to ora

( 
with t1 as 
(select max(o.create_dttm) create_dttm
  , o.channel_cd
  ,o.tlm_id_wf
  ,o.tlm_name_wf
  ,count(*) cnt_4_flow
  from cdm.ma_offer o
  join monitor.ma_log_action la
    on o.create_action_id = la.action_id
 where o.create_dttm > trunc(sysdate)
   and o.channel_cd in ('TLM_OUT')

 group by 
    o.channel_cd
  ,o.tlm_id_wf
  ,o.tlm_name_wf),
  

  t2 as
  (select channel_cd, sum(cnt_4_flow) cnt_all from t1
  group by channel_cd)
 
  select t1.create_dttm
  , t1.channel_cd
  ,t1.tlm_id_wf
  ,t1.tlm_name_wf
  ,t1.cnt_4_flow 
  ,t2.cnt_all from t1 
  join t2 on t1.channel_cd = t2.channel_cd
   order by cnt_4_flow desc

	);

disconnect from ora;
quit;





/*REPORT OF UPLOAD FOR TLO*/

proc sql;

	&ConnectToOra.;

	create table work.upload_tlo as
	select * from connection to ora

( 
select max(o.create_dttm) create_dttm
  , o.channel_cd
  ,o.tlm_id_wf
  ,o.tlm_name_wf
  ,count(*) cnt_id_flow
  from cdm.ma_offer o
  join monitor.ma_log_action la
    on o.create_action_id = la.action_id

 where o.create_dttm > trunc(sysdate)
   and o.channel_cd in ('TLO_OUT')

 group by
   o.channel_cd
  ,o.tlm_id_wf
  ,o.tlm_name_wf
  
  


	);

disconnect from ora;
quit;





/*REPORT OF UPLOAD FOR VOICE BOT*/

proc sql;

	&ConnectToOra.;

	create table work.upload_vb as
	select * from connection to ora

( 

WITH t1 as 
(select max(o.create_dttm) create_dttm
  , o.channel_cd
  ,o.tlm_id_wf
  ,o.tlm_name_wf
  ,count(*) cnt_4_flow
  from cdm.ma_offer o
  join monitor.ma_log_action la
    on o.create_action_id = la.action_id
 where o.create_dttm > trunc(sysdate)
   and o.channel_cd in ('VB_OUT')

 group by
   o.channel_cd
  ,o.tlm_id_wf
  ,o.tlm_name_wf),
  

  t2 as 
  (select channel_cd, sum(cnt_4_flow) cnt_all from t1
  group by channel_cd)
  

  select t1.create_dttm
  ,t1.channel_cd
  ,t1.tlm_id_wf
  ,t1.tlm_name_wf
  ,t1.cnt_4_flow
  ,t2.cnt_all from t1
  join t2 on t1.channel_cd = t2.channel_cd

);

disconnect from ora;
quit;




/* -------------------------------------------------------------------------- */
ods listing close;
filename outbox email
	to=("Zhalgas.Zhienbekov@homecredit.kz")
	from=&v_email_address_from
	subject="Daily Monitoring 4 Call List"
	encoding="utf-8"
	type="text/html";
ods html body=outbox options(pagebreak="no") style=sasweb rs=none;
ods path work.tmp(update) sasuser.templat(update) sashelp.tmplmst(read);
title;
ods escapechar="^";




proc report data=work.sent_daily_sms nowd
	style(report)={bordercolor=black borderstyle=solid borderwidth=1pt cellpadding=5pt cellspacing=0pt}
	style(header)={background=gray foreground=black font_face=Arial}
	style(column)={background=white foreground=black };
	title "DAILY SENT SMS";
	column 
	   create_dttm
       channel_cd
       push_category
       sms_category
       tpl_cd
       cnt_by_segment
       cnt_all;
	define cnt_offers / order order=internal format=commax10.0 descending;
	define cnt_pasticipants / order order=internal format=commax10.0 descending;
run;





proc report data=work.upload_tlm nowd
	style(report)={bordercolor=black borderstyle=solid borderwidth=1pt cellpadding=5pt cellspacing=0pt}
	style(header)={background=gray foreground=black font_face=Arial}
	style(column)={background=white foreground=black };
	title "REPORT OF UPLOAD FOR TLM";
	column 
  		create_dttm
  		channel_cd
  		tlm_id_wf
  		tlm_name_wf
  		cnt_4_flow 
  		cnt_all;
	define cnt_offers / order order=internal format=commax10.0 descending;
	define cnt_pasticipants / order order=internal format=commax10.0 descending;
run;




proc report data=work.upload_tlo nowd
	style(report)={bordercolor=black borderstyle=solid borderwidth=1pt cellpadding=5pt cellspacing=0pt}
	style(header)={background=gray foreground=black font_face=Arial}
	style(column)={background=white foreground=black };
	title "REPORT OF UPLOAD FOR TLO";
	column 
  		create_dttm
  		channel_cd
  		tlm_id_wf
  		tlm_name_wf
  		cnt_id_flow;
	define cnt_offers / order order=internal format=commax10.0 descending;
	define cnt_pasticipants / order order=internal format=commax10.0 descending;
run;




proc report data=work.upload_vb nowd
	style(report)={bordercolor=black borderstyle=solid borderwidth=1pt cellpadding=5pt cellspacing=0pt}
	style(header)={background=gray foreground=black font_face=Arial}
	style(column)={background=white foreground=black };
	title "REPORT OF UPLOAD FOR VOICE BOT";
	column 
  		create_dttm
  		channel_cd
  		tlm_id_wf
  		tlm_name_wf
  		cnt_4_flow
  		cnt_all;
	define cnt_offers / order order=internal format=commax10.0 descending;
	define cnt_pasticipants / order order=internal format=commax10.0 descending;
run;




ods html close;

%if %MA_NOT_OK %then %GOTO ERROR_EXIT;

%GOTO EXIT;

%ERROR_EXIT:
			%if %MA_NOT_OK %then
                   %MA_RAISE_ERROR(%str(Error in DAILY_REPORT_FOR_OFFERS macro, check the log for details));
			%GOTO EXIT;
%EXIT:
	%local mvCount;
	%if %sysfunc(exist(work.t_cnt_offers)) %then %do;
	proc sql noprint;
		select count(*) 
		into :mvCount
		from work.t_cnt_offers;
	quit;
	%end;
	%else %do;
	%let mvCount = 0;
	%end;

	%mCompleteProcessAction (mvlActionID = mvAction_id
							,mvlRecordsCount=&mvCount.);

%mend mReportForOffers;
%mReportForOffers;
