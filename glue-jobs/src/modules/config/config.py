env = "local"
# ------------------------------FILENAME CONFIGURATIONS-----------------
FILE_DATE_FORMAT = "%Y%m%d%H%M%S"


# -----------------------------------------AWS CONFIGURATIONS------------
REGION = "us-east-1"


# ----------------------------TR WEATHER TRANSFORM CONFIGURATIONS--------
TR_WEATHER_REFINED_TEMP_VIEW = "weather_refined_source"
TR_WEATHER_WAREHOUSE_TEMP_VIEW = "weather_warehouse_source"
TR_WEATHER_CHANGED_RECORDS_TEMP_VIEW = "weather_changed_records"

# Weather File
WEATHER_FILE_MODE = "append"


# chnaged record query
chnaged_records_query = """select a.* from {refined_temp_view} as a left outer join {warehouse_temp_view} as b
on
trim(b.location) = trim(a.location) and a.dt = b.dt
where
b.dt is null or (b.dt is not null and a.sas_process_dt >   b.sas_process_dt)""".format(
    refined_temp_view=TR_WEATHER_REFINED_TEMP_VIEW,
    warehouse_temp_view=TR_WEATHER_WAREHOUSE_TEMP_VIEW,
)


retained_records_query = """select warehouse.* from  {warehouse_temp_view} as warehouse
left outer join {changed_records_temp_view} as change
on
trim(warehouse.location)=trim(change.location)
and
warehouse.dt = change.dt
where
change.dt is null""".format(
    warehouse_temp_view=TR_WEATHER_WAREHOUSE_TEMP_VIEW,
    changed_records_temp_view=TR_WEATHER_CHANGED_RECORDS_TEMP_VIEW,
)

# ---------------------------CUSTOM_GENERIC_MAP------------------
MAP_SRC_TEMP_VIEW = "source"
MAP_TEMP_VIEW = "map"
# coalesce(map_tbl.customer_id,src_tbl.customer_id) as customer_id_updated
# MAP_GENERIC_QUERY = (
#    "select src_tbl.*, {} from "
#    + MAP_SRC_TEMP_VIEW
#    + " src_tbl left join "
#    + MAP_TEMP_VIEW
#    + " map_tbl {}"
# )
MAP_DROP_QUERY1 = "drop table if exists {1}.{0}_map_stage"
MAP_CREATE_QUERY = "create table {4}.{1}_map_stage DISTSTYLE EVEN as select src_tbl.*, {0} from {4}.{1} src_tbl left join {4}.{2} map_tbl {3}"
MAP_UPDATE_QUERY = "update {1}.{0}_map_stage set process_dtm=CURRENT_TIMESTAMP where customer_id<>customer_id_updated"
MAP_ALTER_QUERY1 = "alter table {1}.{0}_map_stage drop column customer_id"
MAP_ALTER_QUERY2 = (
    "alter table {1}.{0}_map_stage rename column customer_id_updated to customer_id"
)
MAP_TRUNCATE_QUERY = "truncate table {1}.{0}"
MAP_APPEND_QUERY = "ALTER TABLE {1}.{0} APPEND FROM {1}.{0}_map_stage"
MAP_DROP_QUERY2 = "DROP TABLE {1}.{0}_map_stage"
# ----------------------------TR MAP_CUSTOM_SHOES_TRANSFORM CONFIGURATIONS----
TR_MAP_CUSTOM_REFINED_TEMP_VIEW = "custom_map_refined_source"
TR_MAP_CUSTOM_WAREHOUSE_TEMP_VIEW = "custom_map_warehouse_source"
TR_MAP_CUSTOM_CHANGED_RECORDS_TEMP_VIEW = "custom_map_changed_records"


MAP_CUSTOM_FILE_MODE = "append"


# custom_map query
map_custom_query = """select
src_tbl.*,
coalesce(map_tbl.customer_id,src_tbl.customer_id) as customer_id_updated
from {custom_map_refined_source} src_tbl left join {custom_map_warehouse_source} map_tbl """.format(
    custom_map_refined_source=TR_MAP_CUSTOM_REFINED_TEMP_VIEW,
    custom_map_warehouse_source=TR_MAP_CUSTOM_WAREHOUSE_TEMP_VIEW,
)

# ---------------------------------DYNAMODB_CONFIGURATIONS------------------------------------
FILE_STATUS_PRIMARY_KEY_ATTRIBUTE = "file_name"
EVENT_STATUS_AFTER_TRANSFORM = "SEL"
EVENT_AFTER_TRANSFORM = "TR"

# FILE BROKER TABLE
FILE_BROKER_PARTITION_KEY = "feed_name"
FILE_BROKER_SORT_KEY_ATTRIBUTE = "feed_name"
FILE_BROKER_ATTRIBUTES_TO_BE_FETCHED = [
    "brand",
    "tgt_dstn_folder_name",
    "tr_class_to_call",
    "data_source",
    "dq_class_to_call",
    "dq_params",
    "feed_name",
    "file_frequency",
    "hasHeaders",
    "isActive",
    "isRequired",
    "raw_source_file_delimiter",
    "raw_source_filename",
    "raw_source_filetype",
    "raw_source_folder_name",
    "rf_dstn_file_delimiter",
    "rf_dstn_filename",
    "rf_dstn_filetype",
    "rf_dstn_folder_name",
    "rf_source_dir",
    "sas_brand_id",
    "schema",
    "tgt_destination_filename",
    "tgt_dstn_filetype",
    "tgt_dstn_folder_name",
    "tr_class_to_call",
    "tr_params",
    "tgt_dstn_tbl_name",
]
FB_RF_DESTINATION_FOLDER = "current"
FB_TR_FOLDER = "external"

# ---------------------------------CM_SESSION_XREF_CONFIGURATIONS-----------------------------------------------------------
MAP_CM_REGISTRATION_VIEW = "cm_registration"
MAP_EMAIL_XREF_VIEW = "email_xref"

CM_SESSION_XREF_QUERY = """select
distinct session_id,customer_id,ex.sas_brand_id
from
{cm_registration} as reg
inner join
{email_xref} ex
on
ex.email_address = ltrim(rtrim(lower(reg.email_address)))
and ex.sas_brand_id = reg.sas_brand_id """.format(
    cm_registration=MAP_CM_REGISTRATION_VIEW, email_xref=MAP_EMAIL_XREF_VIEW
)

# ---------------------------------LOYALTY_XREF_CONFIGURATIONS-----------------------------------------------------------
CUST_ATTRIBUTE_VIEW = "cust_attribute"

LOYALTY_XREF_QUERY_OLD = """select customer_id,attribute_comment as LOYA_ID from
( select customer_id,attribute_comment,row_number() over (partition by customer_id order by attribute_date desc ) as row_number
from {cust_attribute} attr where attribute_grouping_code = 'VNBR' and sas_brand_id = 4
)
where row_number = 1 """.format(
    cust_attribute=CUST_ATTRIBUTE_VIEW
)

LOYALTY_XREF_QUERY = """create table {0}.LOYALTY_XREF
AS
SELECT CUSTOMER_ID,ATTRIBUTE_COMMENT AS LOYA_ID
FROM
(SELECT CUSTOMER_ID,ATTRIBUTE_COMMENT,
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY ATTRIBUTE_DATE DESC) AS row_number
FROM {0}.CUST_ATTRIBUTE attr
WHERE ATTRIBUTE_GROUPING_CODE = 'VNBR' AND SAS_BRAND_ID = 4
)
WHERE row_number = 1"""
# --------------------------WEBS_XREF-------------------------------------------------------------------
MAP_ADOBE_TEMP_VIEW = "cust_alt_key"
WEBS_XREF_QUERY = """select sas_brand_id,webs_customer_id,max(customer_id) as customer_id
  from (
      select distinct sas_brand_id,customer_id,
          case 
              when substring(rtrim(ltrim(alternate_key)),1,5) = '01101' then substring(rtrim(ltrim(alternate_key)),6)
          end as webs_customer_id
      from cust_alt_key alt_key
          where ALTERNATE_KEY_CODE='WEBS' and sas_brand_id=4 
      and substring(rtrim(ltrim(alternate_key)),1,5) = '01101'

  )
  group by SAS_BRAND_ID,WEBS_CUSTOMER_ID
  having count(customer_id)=1"""
# ---------------------------------TNF_RESPONSYS_XREF_CONFIGURATIONS-----------------------------------------------------------
TNF_EMAIL_SENT_VIEW = "tnf_email_sent_view"

# TNF_RESPONSYS_XREF_QUERY="""select LIST_ID,RIID,CAMPAIGN_ID,LAUNCH_ID,max(customer_id) as customer_id
#                                 from {}.{tnf_email_sent_view} vw  where
#                                 customer_id is not null
#                                 group by
#                                 LIST_ID,RIID,CAMPAIGN_ID,LAUNCH_ID""".format(tnf_email_sent_view=TNF_EMAIL_SENT_VIEW)
TNF_RESPONSYS_XREF_QUERY = """create table {0}.tnf_responsys_xref as select LIST_ID,RIID,CAMPAIGN_ID,LAUNCH_ID,max(customer_id) as customer_id 
                                from {0}.{1} vw  where
                                customer_id is not null
                                group by 
                                LIST_ID,RIID,CAMPAIGN_ID,LAUNCH_ID"""

TNF_RESPONSYS_XREF_DROP_QUERY = "DROP table if exists {0}.tnf_responsys_xref"
# --------------------MAP ADOBE CONFIGURATION------------------------------------------------------------
MAP_TNF_ADOBE_PRODUCTVIEW_ABANDON_TEMP_VIEW = "tnf_adobe_productview_abandon"
MAP_TNF_ADOBE_CART_AND_PRODVIEW_XREF_FINAL_QUERY = """
select VISITOR_ID,max(CUSTOMER_ID) as CUSTOMER_ID ,sas_brand_id
from TNF_ADOBE_PRODUCTVIEW_ABANDON tnf where customer_id is not null group by
 visitor_id,SAS_brand_id"""
# ----------------------SCHEMA_CONVERSION_CONFIGURATIONS--------------------------------------------------
data_type_dict = {
    "int": "integer",
    "bigint": "long",
    "string": "string",
    "date": "date",
    "timestamp": "timestamp",
    "float": "float",
    "long": "long",
    "double": "double",
    "varchar": "string",
    "number": "integer",
    "numeric": "integer",
    "character": "string",
    "char": "string",
    "integer": "integer",
    "bool": "boolean",
}
# ---------------------------STAGE_STATUS_CONFIGURATIONS----------------------------------------------------
STAGE_COMPLETED_STATUS = "completed"
STAGE_NOT_STARTED_STATUS = "not_started"
STAGE_FAILED_STATUS = "failed"
STAGE_NOT_APPLICABLE_STATUS = "not_applicable"
STAGE_ERROR_NULL = "Null"
STAGE_REFINED_TO_TRANSFORM_INITIAL_STATUS_PARAMS = {
    "error_info": STAGE_ERROR_NULL,
    "status": STAGE_NOT_STARTED_STATUS,
    "update_dttm": "Null",
}

STAGE_DQ_CHECK_INITIAL_STATUS_PARAMS = {
    "error_info": STAGE_ERROR_NULL,
    "status": STAGE_NOT_STARTED_STATUS,
    "update_dttm": "Null",
}
STAGE_SKIPPED = "skipped"
GLUE_JOB_STATUS_PARAMS = {"job_status": "null", "log_file_path": "null"}

# -----------------------------------------------------------------------------------------------------------

LOG_DIR = "glue_process/"
LOG_FILE = "./tmp/process.log"

# --------------------------------------------COPY_OPTIONS_TO_TRUNCATE_STRING----------------------------------
copy_options = "TRUNCATECOLUMNS"

# --------------------------CUST_XREF CONFIGURATIONS -------------------------

cust_xref_stored_procedure = """CREATE OR REPLACE PROCEDURE {0}.stp_find_new_customer(sas_brand_id IN integer)
AS '
DECLARE 

unique_customers_to_find_qry varchar(max);
only_1_new_customer_qry varchar(max);
old_customers_to_find_new_customer_qry varchar(max);
main_customer_base_tofind_new_customers_qry varchar(max);
find_cust_itr_qry varchar(max);
loopvar integer := 2;
x integer;
cust_xref_M_qry varchar(max);
filter_brand_id_qry varchar(max);
create_cust_tbl_qry varchar(max);
create_cust_xref_final_qry varchar(max);
append_brand_id_qry varchar(max);


BEGIN

filter_brand_id_qry = ''create temp table customer_nos as
		select distinct old_customer_no ,  new_customer_no
			from {0}.cust_xref
				where sas_brand_id = '' || sas_brand_id;
EXECUTE filter_brand_id_qry;
Raise info'' Filter_brand_id_qry executed successfully!! '' ;       
		

unique_customers_to_find_qry = ''create temp table unique_customers_to_find as 
(
select *,
case when old_customer_no in (select new_customer_no from customer_nos)  then old_customer_no else NULL  end as old_cust,
case when new_customer_no in (select old_customer_no from customer_nos)  then new_customer_no else NULL  end as new_cust
from customer_nos 
)'';
EXECUTE unique_customers_to_find_qry;
Raise info ''unique_customers_to_find_qry executed successfully'';

only_1_new_customer_qry = ''create temp table only_1_new_customer as
select old_customer_no,new_customer_no
from unique_customers_to_find 
where old_cust  is null  and new_cust is null'';

EXECUTE only_1_new_customer_qry;
Raise info ''only_1_new_customer_qry executed successfully'';

old_customers_to_find_new_customer_qry = ''create temp table old_customers_to_find_new_customer_itr1 as
select old_customer_no,new_customer_no
from unique_customers_to_find 
where old_customer_no not in ( select distinct old_customer_no from only_1_new_customer)'';


EXECUTE old_customers_to_find_new_customer_qry;
Raise info ''old_customers_to_find_new_customer_qry executed successfully'';

main_customer_base_tofind_new_customers_qry = ''create temp table main_customer_base_tofind_new_customers as
select old_customer_no,new_customer_no
from unique_customers_to_find 
where old_cust  is not null'';

EXECUTE main_customer_base_tofind_new_customers_qry;
Raise info ''main_customer_base_tofind_new_customers_qry executed successfully'';

while loopvar  <= 10

loop

x = loopvar - 1;
Raise info ''x:%'',x;

find_cust_itr_qry = ''create temp table old_customers_to_find_new_customer_itr''||loopvar||'' as 
select 
old_customer_no,
case when otpt.new_cust is null then new_customer_no else new_cust end as new_customer_no
from
(
select a.*, b.new_customer_no as new_cust 
from old_customers_to_find_new_customer_itr''||x||''  a
left join 
main_customer_base_tofind_new_customers b
on a.new_customer_no = b.old_customer_no
) otpt'';

EXECUTE find_cust_itr_qry;
Raise info ''loopvar : %'',loopvar;
Raise info ''find_cust_itr_qry : %'',find_cust_itr_qry;
Raise info ''find_cust_itr_qry executed successfully : %'',loopvar;
loopvar = loopvar+1;
end loop;

EXECUTE ''drop table if exists {0}.CUST_XREF_M'';
cust_xref_M_qry = ''CREATE TABLE {0}.CUST_XREF_M AS select * from 
old_customers_to_find_new_customer_itr10
union ALL 
select * from only_1_new_customer'';

EXECUTE cust_xref_M_qry;
Raise info ''cust_xref_M_qry executed successfully'';

create_cust_tbl_qry = ''create temp table cust as 
	select customer_no, customer_id from {0}.cust 
	where sas_brand_id = '' || sas_brand_id;
EXECUTE create_cust_tbl_qry;
Raise info '' Cust table created successfully'';

create_cust_xref_final_qry = ''create temp table cust_xref_final as 
select xf.old_customer_no as customer_no, cust.customer_id , '' || sas_brand_id || ''  as sas_brand_id, current_timestamp as process_dtm from cust cust
inner join {0}.CUST_XREF_M xf
on cust.customer_no = xf.new_customer_no
union
select cust.customer_no, cust.customer_id , '' || sas_brand_id || '' as sas_brand_id , current_timestamp as process_dtm from cust cust '';
EXECUTE create_cust_xref_final_qry;
Raise info '' cust_xref_final table has created successfully!!'';

Raise info ''Applying tr_append_brand_id logic here'';

Execute ''Delete from {0}.cust_xref_final where sas_brand_id =''||sas_brand_id;
Raise info ''Deleted data for given sas_brand_id :%'',sas_brand_id;

append_brand_id_qry = ''insert into {0}.cust_xref_final
select sas_brand_id,customer_no,customer_id,process_dtm from cust_xref_final'';
EXECUTE append_brand_id_qry;
Raise info ''Append_brand_id_qry executed successfully!!'';

END;
' LANGUAGE plpgsql;
"""

# --------------------------REPORTING JOB CONFIGURATIONS -------------------------

transpose_stored_procedure = """CREATE OR REPLACE PROCEDURE {0}.create_transpose_tables(query_out INOUT VARCHAR(max) ,query_sum_out INOUT varchar(max) ,category IN VARCHAR , var_list IN VARCHAR )
AS '
 
DECLARE 
 customer_rec record;
 full_query varchar(max);
 dist_qry varchar;
 tmp_tbl_qry varchar;
 drop_tbl_qry varchar;
 cat varchar;
 var_nm varchar;
 
BEGIN
 
dist_qry = ''create temp table distinct_'' + category + '' as select distinct '' + category + '' from {0}.temp_tnf_''+ category +''_metrics''; 
Raise info ''dist_qry : %'',dist_qry;
Execute dist_qry;
tmp_tbl_qry = ''select ''+ category +'' as category from distinct_'' + category;
Raise info ''tmp_tbl_qry : %'',tmp_tbl_qry;
 
for customer_rec in Execute tmp_tbl_qry loop
cat = customer_rec.category;
var_nm = category + ''_'' + var_list+ ''_'' + cat;
 
query_out := query_out||'',case when ''+ category + ''= ''||quote_literal(cat)||'' then '' + var_list + '' else null end as ''||var_nm;
Raise info ''query_out : %'',query_out;
 
query_sum_out := query_sum_out||'',sum(''||var_nm||'') as '' + var_nm ;
Raise info ''query_sum_out : %'',query_sum_out;
end loop; 
 
drop_tbl_qry = ''drop table if exists {0}.''+ category + ''_csv_''+var_list;
EXECUTE drop_tbl_qry;
full_query = ''create table {0}.''+ category + ''_csv_''+var_list+'' DISTKEY(customer_id_'' + category + ''_'' + var_list+'') as ( with ''+ category + ''_csv_''+var_list+'' as ( 
 select customer_id as customer_id_'' + category + ''_'' + var_list || query_out || '' from {0}.temp_tnf_''+ category +''_metrics ) select customer_id_''+category + ''_'' + var_list||query_sum_out||'' from ''
 + category + ''_csv_''+var_list + '' group by customer_id_'' +category + ''_'' + var_list+'' )'';
Raise info ''Full Query : %'',full_query;
EXECUTE full_query ;
Raise info ''Query Executed successfully'';
 
 
END;
' LANGUAGE plpgsql;"""

dedup_summary_non_wcs = """CREATE OR REPLACE PROCEDURE {0}.dedup_summary_non_wcs(sas_brand_id IN integer,input_table varchar(max), key_var varchar(max))
AS'
DECLARE 
create_dup_for_each_table_qry varchar(max);
create_unq_for_each_table_qry varchar(max);
drop_dup_summary_qry varchar(max);
create_dup_summary_qry varchar(max);
insert_into_final_table_qry varchar(max);
 
BEGIN
drop_dup_summary_qry = ''drop table if exists {0}.dup_summary'';
create_dup_summary_qry = ''create table if not exists {0}.dup_summary
  (Observations integer, 
 Output_Table_Name varchar(300),
 Created_Date timestamp, 
 Keys varchar(1000), 
 Brand varchar(5), 
 Dup varchar(5)
 )'';
 
 Raise info ''create_dup_summary_qry : %'',create_dup_summary_qry;
 --Execute drop_dup_summary_qry;
 Execute create_dup_summary_qry;
 
create_dup_for_each_table_qry = ''create temp table '' + input_table+''_DUP''+SAS_BRAND_ID+'' as
select *,
case
 
when '' + SAS_BRAND_ID + ''=4 then ''''tnf'''' when '' + SAS_BRAND_ID + ''=7 then ''''vans'''' else ''''ERROR'''' end as Brand,
case
when Observations > 0 then ''''Y''''
when Observations = 0 then ''''N''''
else ''''ERROR''''
end as Dup from (
SELECT count(*) as Observations, 
upper('''''' + input_table+''_DUP''+SAS_BRAND_ID+'''''') as Output_Table_Name,
current_timestamp as Created_Date, 
upper('''''' + key_var + '''''') as keys
FROM
(SELECT 
row_number() over (partition BY '' + key_var + '' order by ''+ key_var + '') rn
FROM {0}.'' + input_table + ''
where
sas_brand_id = '' + sas_brand_id + ''
)
WHERE rn <> 1
)'';
 
create_unq_for_each_table_qry = ''create temp table '' + input_table+''_UNQ''+SAS_BRAND_ID+'' as
select *,
case
when '' + SAS_BRAND_ID + ''=4 then ''''tnf'''' when '' + SAS_BRAND_ID + ''=7 then ''''vans'''' else ''''ERROR'''' end as Brand,
case
when Observations > 0 then ''''N''''
when Observations = 0 then ''''Y''''
else ''''ERROR''''
end as Dup from (
SELECT count(*) as Observations, 
upper('''''' + input_table+''_UNQ''+SAS_BRAND_ID+'''''') as Output_Table_Name,
current_timestamp as Created_Date, 
upper('''''' + key_var + '''''') as keys
FROM
(SELECT 
row_number() over (partition BY '' + key_var + '' order by ''+ key_var + '') rn
FROM {0}.'' + input_table + ''
where
sas_brand_id = '' + sas_brand_id + ''
)
WHERE rn = 1
)'';
 
insert_into_final_table_qry = ''Insert into {0}.dup_summary (select * from ''+ input_table+''_UNQ''+SAS_BRAND_ID + '' 
union all select * from ''+ input_table+''_DUP''+SAS_BRAND_ID+'')'';
 

 
EXECUTE create_dup_for_each_table_qry;
EXECUTE create_unq_for_each_table_qry;
Execute insert_into_final_table_qry;
Raise info ''create_dup_for_each_table : %'',create_dup_for_each_table_qry;
Raise info ''create_unq_for_each_table : %'',create_unq_for_each_table_qry;
 
END;
'LANGUAGE plpgsql;"""


dedup_summary_wcs = """CREATE OR REPLACE PROCEDURE {0}.dedup_summary_wcs(sas_brand_id IN integer,input_table varchar(max), key_var varchar(max), email varchar(max))
AS'
DECLARE 
create_dup_for_each_table_qry varchar(max);
create_unq_for_each_table_qry varchar(max);
drop_dup_summary_qry varchar(max);
create_dup_summary_qry varchar(max);
insert_into_final_table_qry varchar(max);
 
BEGIN
drop_dup_summary_qry = ''drop table if exists {0}.dup_summary'';
create_dup_summary_qry = ''create table if not exists {0}.dup_summary
  (Observations integer, 
 Output_Table_Name varchar(300),
 Created_Date timestamp, 
 Keys varchar(1000), 
 Brand varchar(5), 
 Dup varchar(5)
 )'';
 
 Raise info ''create_dup_summary_qry : %'',create_dup_summary_qry;
 --Execute drop_dup_summary_qry;
 Execute create_dup_summary_qry;
 
create_dup_for_each_table_qry = ''create temp table '' + input_table+''_DUP''+SAS_BRAND_ID+'' as
select *,
case
 
when '' + SAS_BRAND_ID + ''=4 then ''''tnf'''' when '' + SAS_BRAND_ID + ''=7 then ''''vans'''' else ''''ERROR'''' end as Brand,
case
when Observations > 0 then ''''Y''''
when Observations = 0 then ''''N''''
else ''''ERROR''''
end as Dup from (
SELECT count(*) as Observations, 
upper('''''' + input_table+''_DUP''+SAS_BRAND_ID+'''''') as Output_Table_Name,
current_timestamp as Created_Date, 
upper('''''' + key_var + '''''') as keys
FROM
(SELECT 
row_number() over (partition BY '' + key_var + '' order by ''+ key_var + '') rn
FROM {0}.'' + input_table + ''
where
sas_brand_id = '' + sas_brand_id + '' and '' + email + '' is not null
)
WHERE rn <> 1
)'';
 
create_unq_for_each_table_qry = ''create temp table '' + input_table+''_UNQ''+SAS_BRAND_ID+'' as
select *,
case
when '' + SAS_BRAND_ID + ''=4 then ''''tnf'''' when '' + SAS_BRAND_ID + ''=7 then ''''vans'''' else ''''ERROR'''' end as Brand,
case
when Observations > 0 then ''''N''''
when Observations = 0 then ''''Y''''
else ''''ERROR''''
end as Dup from (
SELECT count(*) as Observations, 
upper('''''' + input_table+''_UNQ''+SAS_BRAND_ID+'''''') as Output_Table_Name,
current_timestamp as Created_Date, 
upper('''''' + key_var + '''''') as keys
FROM
(SELECT 
row_number() over (partition BY '' + key_var + '' order by ''+ key_var + '') rn
FROM {0}.'' + input_table + ''
where
sas_brand_id = '' + sas_brand_id + '' and '' + email + '' is not null
)
WHERE rn = 1
)'';
 
insert_into_final_table_qry = ''Insert into {0}.dup_summary (select * from ''+ input_table+''_UNQ''+SAS_BRAND_ID + '' 
union all select * from ''+ input_table+''_DUP''+SAS_BRAND_ID+'')'';
 

 
EXECUTE create_dup_for_each_table_qry;
EXECUTE create_unq_for_each_table_qry;
Execute insert_into_final_table_qry;
Raise info ''create_dup_for_each_table : %'',create_dup_for_each_table_qry;
Raise info ''create_unq_for_each_table : %'',create_unq_for_each_table_qry;
 
END;
'LANGUAGE plpgsql;"""


execute_stp_wcs_non_wcs = """CREATE OR REPLACE PROCEDURE {0}.execute_stp_wcs_non_wcs()
AS'

BEGIN
Execute ''drop table if exists {0}.dup_summary'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''ADDRESS'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''ADDRESS'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CLASS'''',''''class_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CLASS'''',''''class_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''COLOR'''',''''color_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''COLOR'''',''''color_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CUST'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CUST'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CUST_ALT_KEY'''',''''customer_id, alternate_key, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CUST_ALT_KEY'''',''''customer_id, alternate_key, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CUST_ATTRIBUTE'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CUST_ATTRIBUTE'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CUST_XREF'''',''''old_customer_no, new_customer_no, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CUST_XREF'''',''''old_customer_no, new_customer_no, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''DEPT'''',''''department_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''DEPT'''',''''department_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''PRODUCTXREF'''',''''product_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''PRODUCTXREF'''',''''product_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''REGION'''',''''region_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''REGION'''',''''region_code, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''STORE'''',''''store_no, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''STORE'''',''''store_no, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''STYLE'''',''''style_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''STYLE'''',''''style_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''TRANS_CATEGORY'''',''''transaction_category, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''TRANS_CATEGORY'''',''''transaction_category, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''TRANS_DETAIL'''',''''transaction_id, transaction_line_no, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''TRANS_DETAIL'''',''''transaction_id, transaction_line_no, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''TRANS_HEADER'''',''''transaction_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''TRANS_HEADER'''',''''transaction_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''VANS_COUPON'''',''''detail_line_number, pos_trans_no, store, register, transaction_date, deal_number, event_number, promotion_amount, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''EXPERIAN_BRONZE'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''EXPERIAN_BRONZE'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''EXPERIAN_GOLD'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''EXPERIAN_GOLD'''',''''customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CM_ABANDON'''',''''session_id, cookie_id, ts, product_id, abandonment_attribute_1, abandonment_attribute_2, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CM_ABANDON'''',''''session_id, cookie_id, ts, product_id, abandonment_attribute_1, abandonment_attribute_2, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CM_CONVERSION'''',''''session_id, cookie_id, ts, event_action_type, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CM_PAGEVIEW'''',''''session_id, cookie_id, ts, page, page_id, page_url, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CM_PRODUCTVIEW'''',''''session_id, cookie_id, ts, product_name, product_id, page_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CM_PRODUCTVIEW'''',''''session_id, cookie_id, ts, product_name, product_id, page_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CM_REGISTRATION'''',''''session_id, cookie_id, registration_id, email_address, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CM_REGISTRATION'''',''''session_id, cookie_id, registration_id, email_address, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CM_SESSION_FIRST_PAGE_VIEW'''',''''session_id, cookie_id, first_ts, ip_address, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''CM_SESSION_XREF'''',''''session_id, customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(7,''''CM_SESSION_XREF'''',''''session_id, customer_id, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_non_wcs(4,''''TNF_TIBCO'''',''''loyaltylabrewardid, firstname, lastname, processdatetime, pointbalanceextractdate, SAS_BRAND_ID'''')'';
Execute ''call {0}.dedup_summary_wcs(4,''''TNF_WCS_ABANDONED_WISH_CART'''',''''customer_id, email, versioncode, firstname, lastname, product_id, cart_itm_dt, SAS_BRAND_ID'''',''''email'''')'';
Execute ''call {0}.dedup_summary_wcs(4,''''TNF_WCS_RETURNS_US'''',''''customer_id, productid, orderid, userid, price, dt, rmaid, emailid, SAS_BRAND_ID'''',''''emailid'''')'';
Execute ''call {0}.dedup_summary_wcs(7,''''VANS_WCS_ABANDONED_CART'''',''''email, product_id, cartlisturl, cart_itm_dt, SAS_BRAND_ID'''',''''email'''')'';
Execute ''call {0}.dedup_summary_wcs(7,''''VANS_WCS_RETURNS_US'''',''''customer_id, productid, orderid, userid, rmaid, emailid, price, dt, SAS_BRAND_ID'''',''''emailid'''')'';
Execute ''call {0}.dedup_summary_wcs(7,''''VANS_WCS_WISHLIST'''',''''customer_id, firstname, lastname, product_id,
cart_itm_dt, cartlisturl, email, versioncode, SAS_BRAND_ID'''',''''email'''')''; 

END;

'LANGUAGE plpgsql;"""
