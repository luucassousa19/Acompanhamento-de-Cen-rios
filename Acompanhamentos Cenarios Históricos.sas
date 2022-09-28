libname rmbarslt "/install/SASConfig/Lev1/AppData/SASIRM/pa/data/1660958844/rmbarslt";
libname rmbastg "/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.sbrlus/input_area/07312019";
libname rmbantmp "/install/SASConfig/Lev1/AppData/SASIRM/pa/data/1660958844/rmbantmp";

proc sql;
create table scendiv_filter as
	select distinct AnalysisNumber, ResultName, ResultVersion
	from rmbarslt.scen_div
;quit;

/**************INICIO RELs 2.2.5.2 - 2.2.5.4****************/
proc sql;
create table curveid as
	select
		a.RISK_FACTOR_ID,
		a.RISK_FACTOR_CATEGORY_NM,
		b.CURVE_ID
	from rmbastg.RISK_FACTOR as a
	left join rmbantmp.risk_factor_x_risk_fctr_curve as b
	on a.RISK_FACTOR_ID = b.RISK_FACTOR_ID
;quit;

data states_div;
set rmbarslt.states_div;
run;

proc transpose data=states_div out=STATES_V1 (drop=_label_ rename=(col1=VALUE));
	by  statenumber analysisnumber analysispart analysisname _date_;
run;

data STATES_V2;
set STATES_V1;
VERTICE = input(compress(scan(tranwrd(_NAME_,"_"," "),-1),'','kd'),8.);
rename _date_ = BaseDate;
run;


proc sql;
	create table STATES_V3 as
	select *
	from STATES_V2 as a
	left join curveid as b
	on a._name_= b.RISK_FACTOR_ID
where _NAME_ CONTAINS '_R1_'
;quit;

proc sql;
	create table STATES_FIM as
	select *
	from STATES_V3 as a
	left join scendiv_filter as b
	on a.AnalysisNumber = b.AnalysisNumber
;quit;


data basecase notbasecase;
set STATES_FIM;
	where RISK_FACTOR_CATEGORY_NM not is missing;
	if AnalysisName = 'BASECASE' then output basecase;
	else output notbasecase;
run;

data stress_option1;
	set rmbastg.x_br_stress_testing_option(where=(CONFIG_NAME = 'RED_TYPE'));
	call symputx('RED_TYPE',CONFIG_VALUE);
run;

data stress_option2;
	set rmbastg.x_br_stress_testing_option(where=(CONFIG_NAME = 'NO_REPORT_RF'));
	call symputx('NO_REPORT_RF',CONFIG_VALUE);
run;

proc sql;
	create table tabela_fim as
select 
	a.BaseDate,
	a.AnalysisName,
	a._NAME_,
	a.VALUE as Value_statesdiv,
	a.VERTICE,
	a.RISK_FACTOR_ID,
	a.RISK_FACTOR_CATEGORY_NM,
	a.CURVE_ID,
	a.ResultVersion,
	b.VALUE as Value_statesdiv_bc
from notbasecase as a
left join basecase as b
on a.BaseDate = b.BaseDate
and a.VERTICE = b.VERTICE
and a._NAME_ = b._NAME_
and	a.RISK_FACTOR_ID = b.RISK_FACTOR_ID
and a.CURVE_ID = b.CURVE_ID
where prxmatch("/^.+_R&RED_TYPE._[0-9]+$/i", trim(a.risk_factor_id)) and
a.CURVE_ID not in ("&NO_REPORT_RF.")
;quit;
