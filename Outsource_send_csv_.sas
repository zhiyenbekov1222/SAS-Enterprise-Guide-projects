ods _ALL_ close;
%include "/sas/ma/env/autoexec.sas" / ENCODING='UTF-8';
&MATEMP_LIBNAME;
%let MAREPORT_LIB = MAREPORT;
LIBNAME &MAREPORT_LIB BASE "/temp/ma/MA_REPORT";

proc sql;

	&ConnectToOra.;
	execute(
		merge into ma_temp.t_xs_call_tlm_outsourcing hh
	using (
		with oo as
	(select o.offer_id,
		o.id_cuid,
		o.participant_id,
		o.deactivation_reason_nm,
		o.deactivation_dttm,
		o.phone_number,
		o.create_dttm,
		o.tlm_id_wf,
		o.tlm_name_wf,
		o.tlm_id_cs,
		o.tlm_name_cs
	from cdm.ma_offer o
		left join ma_temp.t_xs_call_tlm_outsourcing h
			on h.offer_id = o.offer_id
		where o.channel_cd in ('TLO_OUT')
			and o.create_dttm > greatest(trunc(sysdate) - 150, date '2021-03-01')
		group by o.offer_id,
			o.id_cuid,
			o.participant_id,
			o.deactivation_reason_nm,
			o.deactivation_dttm,
			o.phone_number,
			o.create_dttm,
			o.tlm_id_wf,
			o.tlm_name_wf,
			o.tlm_id_cs,
			o.tlm_name_cs
		having(max
			(case
				when o.create_dttm > trunc(sysdate) and
				o.deactivation_dttm > sysdate and
				o.deactivation_reason_nm is null then 1 
				else
				0 
			end)
			= 1 
			and max
		(case 
			when h.offer_id is not null then 1 
			else 0 
		end)
		= 0) 
		OR    
		(max
	(case
		when o.deactivation_reason_nm is not null and
		o.deactivation_dttm between date
		'2021-03-01' and sysdate then 1 
		else 0 
	end)
	= 1 and 
	max(decode(h.ACTION_TYPE, 'Deactivation', 1, 0)) = 0    
	and max
(case 
	when h.offer_id is not null then 1 
	else 0 
end)
= 1    
			)),
			ct as
		(select ph.offer_id,
			ph.phone_number,
			ph.phone_type,
			row_number() over(partition by ph.offer_id order by decode(ph.phone_type, 'PRIMARY_MOBILE', 1, 2), ph.score_1 desc) as r_
		from oo
			join integration.call_list_phones ph
				on ph.offer_id = oo.offer_id
				and oo.deactivation_reason_nm is null),
				phones as
			(select ct.offer_id,
				max(decode(r_, 1, phone_number)) as phone1,
				max(decode(r_, 1, phone_type)) as phone_type1,
				max(decode(r_, 2, phone_number)) as phone2,
				max(decode(r_, 2, phone_type)) as phone_type2
			from ct
				group by ct.offer_id),
					offers as
				(select oo.offer_id,
					nvl(max 
				(case
					when p.participant_id = oo.participant_id then
					p.product_group
				end), max 
				(case
					when p.offer_type_code = 'Cash_online1' then 
					p.product_group
				end))
			as CAMPAIGN_NAME,
				max 
			(case
				when p.participant_id != oo.participant_id and
                    p.offer_type_code in
                    ('acl_card_online', 'Card_offline', 'Card_online') then
				p.product_group
			end)
		as CAMPAIGN_NAME_2 
			from oo 
				join cdm.v_participant p 
					on p.id_cuid = oo.id_cuid 
					and p.deactivation_dttm > sysdate 
					and p.offer_status = 'ACTIVE' 
					and (p.offer_type_code in 
					('acl_card_online',
					/*'acl_cash_exist_online',*/
					'acl_cash_lp_online',
					'acl_cash_online',
					/*'acl_ref_online',*/
					'Card_offline',
					'Card_online',
					'Cash_offline',
					'Cash_online' /*,'Refinance_offline'*/) or 
					p.participant_id = oo.participant_id)
					and oo.deactivation_reason_nm is null 
				group by oo.offer_id) 
					select oo.offer_id,
						oo.id_cuid,
						decode(cl.name_first, 'XNA', '', cl.name_first) as name,
						decode(cl.name_middle, 'XNA', '', cl.name_middle) as name_middle,
						cl.num_age as age,
						decode(cl.name_region_contact,
						'XNA',
						cl.name_region_report,
						cl.name_region_contact) as region,
					case 
						when oo.deactivation_reason_nm is not null and oo.tlm_id_wf in (372,373)  then 
						'Deactivation2'
						when oo.deactivation_reason_nm is not null then 
						'Deactivation'
						when oo.tlm_id_wf = 329 then 
						'Created'
						when oo.tlm_id_wf = 372 then 
						'Created2'
						when oo.tlm_id_wf = 330 then 
						'Follow UP'
						when oo.tlm_id_wf = 373 then 
						'Follow UP2'
					end 
				as action_type,
					phones.PHONE1,
					phones.PHONE2,
					offers.CAMPAIGN_NAME,
					offers.CAMPAIGN_NAME_2,
					cl.skp_client 
				from oo 
					join cmdm.ft_client_ad cl 
						on cl.id_cuid = oo.id_cuid 
					left join offers 
						on offers.offer_id = oo.offer_id 
					left join phones 
						on phones.offer_id = oo.offer_id 
				where nvl(offers.CAMPAIGN_NAME, offers.CAMPAIGN_NAME_2) is not null or oo.deactivation_reason_nm is not null
				) s 
				on (hh.offer_id = s.offer_id and hh.action_type = s.action_type)
				/**/
				when matched then update set hh.campaign_name_2 = s.campaign_name_2
				/**/
	when not matched then 
	insert 
		(hh.offer_id,
		hh.id_cuid,
		hh.name,
		hh.name_middle,
		hh.age,
		hh.region,
		hh.phone1,
		hh.phone2,
		hh.campaign_name,
		hh.action_type,
		hh.skp_client,
		hh.date_export,
		hh.campaign_name_2)
		values
			(s.offer_id,
			s.id_cuid,
			s.name,
			s.name_middle,
			s.age,
			s.region,
			s.phone1,
			s.phone2,
			s.campaign_name,
			s.action_type,
			s.skp_client,
			sysdate,
			s.campaign_name_2)





		) by ora;
	disconnect from ora;
quit;

PROC SQL;
	CREATE TABLE WORK.T_TLM_OUT_EXPORT AS 
		SELECT distinct t1.OFFER_ID, 
			t1.ID_CUID, 
		case t1.name 
			when 'XNA' then '' 
			else t1.name 
		end 
	as name, 
		case t1.NAME_MIDDLE 
			when 'XNA' then '' 
			else t1.NAME_MIDDLE 
		end 
	as NAME_MIDDLE, 
		t1.age as age, 
	case t1.REGION 
		when 'XNA' then '' 
		else t1.REGION 
	end 
as REGION, 
	t1.phone1 as phone1, 
case 
	when t1.phone2=t1.phone1 then '' 
	else t1.phone2 
end 
as phone2, 
t1.CAMPAIGN_NAME as campaign_name, 
t1.CAMPAIGN_NAME_2 as campaign_name_2, 
t1.action_type as action_type
FROM ma_temp.t_xs_call_tlm_outsourcing  t1
INNER JOIN CDM.MA_OFFER MO ON T1.ID_CUID = MO.ID_CUID AND MO.CREATE_DTTM > date()-1
AND MO.CHANNEL_CD = 'TLO_OUT'
INNER JOIN monitor.ma_log_action la on mo.create_action_id = la.action_id 
and la.campaign_cd = 'CAMP12422'
WHERE t1.DATE_EXPORT between date()-1 and date()+1 

	and t1.action_type in ('Deactivation','Created','Follow UP');
QUIT;




filename fileout "/sas/sas/data/import/share_TLM/out/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_out.CSV" encoding="utf-8";

/*filename fileout "/sas/sas/data/import/share/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_out.CSV" encoding="utf-8";*/
proc export data=WORK.T_TLM_OUT_EXPORT dbms=csv replace outfile=fileout;
run;

/* --------------------------------------------------*/
filename fileout "/sas/ma/data/models/reports/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_out.CSV" encoding="utf-8";

proc export data=WORK.T_TLM_OUT_EXPORT dbms=csv replace outfile=fileout;
run;

ods listing close;

%let v_email_address_from = 'skorniyenko@homecredit.kz';

/* -------------------------------------------------------------------------------------- */
/* Sent e-mail */
filename outbox email 
	to=("skorniyenko@Homecredit.kz" "Zhalgas.Zhienbekov@homecredit.kz" "EPORGEN@Homecredit.kz" "mgolub@Homecredit.kz" "bota.yerdebayeva@homecredit.kz" "anton.rudin@Homecredit.kz" "ulan.gabdushev@homecredit.kz" &v_email_address_from)
/*to=("Zhalgas.Zhienbekov@homecredit.kz")*/
/*to=("skorniyenko@homecredit.kz" )*/

/*to= ("mussa.osser@Homecredit.kz" )*/
from= 'zhalgas.zhienbekov@homecredit.kz'
subject="File for outsourcing on %sysfunc(intnx(day,%sysfunc(date()), 0),ddmmyyp10.)"
encoding="utf-8"
attach=("/sas/ma/data/models/reports/%sysfunc(intnx(day,%sysfunc(date()), 0),YYMMDD10.)_out.CSV" content_type="application/vnd.ms-excel")
type="text/html"

;
ods html body=outbox;
ods path work.tmp(update) sasuser.templat(update) sashelp.tmplmst(read);
ods html close;