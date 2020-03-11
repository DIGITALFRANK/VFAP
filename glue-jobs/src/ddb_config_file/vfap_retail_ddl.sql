DROP table vfap_retail.cm_pageview;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_pageview(
session_id VARCHAR(34),
cookie_id VARCHAR(41),
ts TIMESTAMP WITHOUT TIME ZONE,
page VARCHAR(600),
page_id VARCHAR(600),
content_category VARCHAR(600),
content_category_id VARCHAR(600),
content_category_top VARCHAR(600),
content_category_bottom VARCHAR(600),
on_site_search_term VARCHAR(600),
page_url VARCHAR(3000),
page_referral_url VARCHAR(3000),
site_id VARCHAR(250),
page_view_attribute_1 VARCHAR(600),
page_view_attribute_2 VARCHAR(600),
page_view_attribute_3 VARCHAR(600),
page_view_attribute_4 VARCHAR(600),
page_view_attribute_5 VARCHAR(600),
page_view_attribute_6 VARCHAR(600),
page_view_attribute_7 VARCHAR(600),
page_view_attribute_8 VARCHAR(600),
page_view_attribute_9 VARCHAR(600),
page_view_attribute_10 VARCHAR(600),
page_view_attribute_11 VARCHAR(600),
page_view_attribute_12 VARCHAR(600),
page_view_attribute_13 VARCHAR(600),
page_view_attribute_14 VARCHAR(600),
page_view_attribute_15 VARCHAR(600),
page_view_attribute_16 VARCHAR(600),
page_view_attribute_17 VARCHAR(600),
page_view_attribute_18 VARCHAR(600),
page_view_attribute_19 VARCHAR(600),
page_view_attribute_20 VARCHAR(600),
page_view_attribute_21 VARCHAR(600),
page_view_attribute_22 VARCHAR(600),
page_view_attribute_23 VARCHAR(600),
page_view_attribute_24 VARCHAR(600),
page_view_attribute_25 VARCHAR(600),
page_view_attribute_26 VARCHAR(600),
page_view_attribute_27 VARCHAR(600),
page_view_attribute_28 VARCHAR(600),
page_view_attribute_29 VARCHAR(600),
page_view_attribute_30 VARCHAR(600),
page_view_attribute_31 VARCHAR(600),
page_view_attribute_32 VARCHAR(600),
page_view_attribute_33 VARCHAR(600),
page_view_attribute_34 VARCHAR(600),
page_view_attribute_35 VARCHAR(600),
page_view_attribute_36 VARCHAR(600),
page_view_attribute_37 VARCHAR(600),
page_view_attribute_38 VARCHAR(600),
page_view_attribute_39 VARCHAR(600),
page_view_attribute_40 VARCHAR(600),
page_view_attribute_41 VARCHAR(600),
page_view_attribute_42 VARCHAR(600),
page_view_attribute_43 VARCHAR(600),
page_view_attribute_44 VARCHAR(600),
page_view_attribute_45 VARCHAR(600),
page_view_attribute_46 VARCHAR(600),
page_view_attribute_47 VARCHAR(600),
page_view_attribute_48 VARCHAR(600),
page_view_attribute_49 VARCHAR(600),
page_view_attribute_50 VARCHAR(600),
search_results_count INTEGER,
customer_id INTEGER,
sas_brand_id INTEGER,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200),
fs_sk INTEGER
)
DISTSTYLE EVEN;


DROP table vfap_retail.tnf_responsys_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_responsys_xref(
list_id INTEGER,
riid BIGINT,
campaign_id INTEGER ,
launch_id INTEGER,
customer_id INTEGER,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_email_sent;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_sent(
sas_brand_id INTEGER,
customer_id BIGINT ,
event_typ_id INTEGER,
account_id INTEGER ,
list_id INTEGER,
riid BIGINT,
customer_no BIGINT,
event_captured_dt TIMESTAMP WITHOUT TIME ZONE,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE,
campaign_id INTEGER,
launch_id INTEGER,
email VARCHAR(46),
email_isp VARCHAR(32),
email_format VARCHAR(1),
offer_signature INTEGER,
dynamic_content_sig_id INTEGER,
message_size INTEGER,
segment_info VARCHAR(10),
contact_info VARCHAR(46),
store_id VARCHAR(1),
fs_sk INTEGER,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cm_productview;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_productview(
session_id VARCHAR(34),
cookie_id VARCHAR(41),
ts TIMESTAMP WITHOUT TIME ZONE,
product_name VARCHAR(435),
product_id VARCHAR(435),
page_id VARCHAR(530),
product_category_id VARCHAR(435),
product_category VARCHAR(435),
product_category_top VARCHAR(435),
product_category_bottom VARCHAR(435),
site_id VARCHAR(170),
product_view_attribute_1 VARCHAR(256),
product_view_attribute_2 VARCHAR(256),
product_view_attribute_3 VARCHAR(256),
product_view_attribute_4 VARCHAR(256),
product_view_attribute_5 VARCHAR(256),
product_view_attribute_6 VARCHAR(256),
product_view_attribute_7 VARCHAR(256),
product_view_attribute_8 VARCHAR(256),
product_view_attribute_9 VARCHAR(256),
product_view_attribute_10 VARCHAR(256),
product_view_attribute_11 VARCHAR(256),
product_view_attribute_12 VARCHAR(256),
product_view_attribute_13 VARCHAR(256),
product_view_attribute_14 VARCHAR(256),
product_view_attribute_15 VARCHAR(256),
product_view_attribute_16 VARCHAR(256),
product_view_attribute_17 VARCHAR(256),
product_view_attribute_18 VARCHAR(256),
product_view_attribute_19 VARCHAR(256),
product_view_attribute_20 VARCHAR(256),
product_view_attribute_21 VARCHAR(256),
product_view_attribute_22 VARCHAR(256),
product_view_attribute_23 VARCHAR(256),
product_view_attribute_24 VARCHAR(256),
product_view_attribute_25 VARCHAR(256),
product_view_attribute_26 VARCHAR(256),
product_view_attribute_27 VARCHAR(256),
product_view_attribute_28 VARCHAR(256),
product_view_attribute_29 VARCHAR(256),
product_view_attribute_30 VARCHAR(256),
product_view_attribute_31 VARCHAR(256),
product_view_attribute_32 VARCHAR(256),
product_view_attribute_33 VARCHAR(256),
product_view_attribute_34 VARCHAR(256),
product_view_attribute_35 VARCHAR(256),
product_view_attribute_36 VARCHAR(256),
product_view_attribute_37 VARCHAR(256),
product_view_attribute_38 VARCHAR(256),
product_view_attribute_39 VARCHAR(256),
product_view_attribute_40 VARCHAR(256),
product_view_attribute_41 VARCHAR(256),
product_view_attribute_42 VARCHAR(256),
product_view_attribute_43 VARCHAR(256),
product_view_attribute_44 VARCHAR(256),
product_view_attribute_45 VARCHAR(256),
product_view_attribute_46 VARCHAR(256),
product_view_attribute_47 VARCHAR(256),
product_view_attribute_48 VARCHAR(256),
product_view_attribute_49 VARCHAR(256),
product_view_attribute_50 VARCHAR(256),
sas_brand_id INTEGER,
fs_sk INTEGER,
customer_id INTEGER,
sas_product_id VARCHAR(20),
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;
COMMIT;

DROP table vfap_retail.vans_zeta_sent;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_zeta_sent(
campaign_id INTEGER,
campaign_name VARCHAR(340),
mailing_id INTEGER,
mailing_name VARCHAR(340),
cell_id INTEGER,
cell_name VARCHAR(109),
mailing_udf_1 VARCHAR(109),
mailing_udf_2 VARCHAR(109),
mailing_udf_3 VARCHAR(109),
email_subject VARCHAR(434),
mailed_date TIMESTAMP WITHOUT TIME ZONE,
mailed_count INTEGER,
customer_no BIGINT,
email_address VARCHAR(435),
sas_brand_id INTEGER,
fs_sk INTEGER,
customer_id INTEGER,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cm_session_first_page_view;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_session_first_page_view(
session_id VARCHAR(34) ,
cookie_id VARCHAR(41) ,
first_ts TIMESTAMP WITHOUT TIME ZONE ,
first_referral_url VARCHAR(1741) ,
first_destination_url VARCHAR(1741) ,
first_referral_type VARCHAR(2) ,
ip_address VARCHAR(435) ,
ts TIMESTAMP WITHOUT TIME ZONE ,
referral_name VARCHAR(170) ,
referral_url VARCHAR(1741) ,
referral_type VARCHAR(2) ,
natural_search_term VARCHAR(435) , --value too long error changing to 800 
destination_url VARCHAR(1741) ,
user_agent VARCHAR(870) ,
search_engine VARCHAR(170) ,
marketing_vendor VARCHAR(435) ,
marketing_category VARCHAR(435) ,
marketing_placement VARCHAR(435) ,
marketing_item VARCHAR(435) ,
visitor_ad_impression_id VARCHAR(435) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id INTEGER,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_email_sent_hist;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_sent_hist(
id INTEGER,
event_uuid VARCHAR(3),
event_type_id INTEGER,
account_id INTEGER,
list_id INTEGER,
riid BIGINT,
custom_properties VARCHAR(2),
campaign_id INTEGER,
launch_id INTEGER,
email VARCHAR(48),
email_isp VARCHAR(32),
email_format VARCHAR(3),
offer_signature_id INTEGER,
dynamic_content_signature_id INTEGER,
message_size INTEGER,
segment_info VARCHAR(2),
contact_info VARCHAR(3),
event_captured_dt TIMESTAMP WITHOUT TIME ZONE,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE,
customer_no BIGINT,
customer_id BIGINT,
sas_brand_id INTEGER DEFAULT 4,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_email_open;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_open(
sas_brand_id INTEGER ,
customer_id BIGINT ,
event_type_id INTEGER ,
account_id INTEGER ,
list_id INTEGER ,
riid BIGINT ,
customer_no BIGINT ,
event_captured_dt TIMESTAMP WITHOUT TIME ZONE ,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE ,
campaign_id INTEGER ,
launch_id INTEGER ,
email_format VARCHAR(1) ,
store_id VARCHAR(10) ,
user_agent_string VARCHAR(100) ,
operating_system VARCHAR(3) ,
browser_type VARCHAR(2) ,
browser VARCHAR(2) ,
fs_sk INTEGER,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.trans_detail;
CREATE TABLE IF NOT EXISTS vfap_retail.trans_detail(
sas_brand_id INTEGER ,
transaction_id INTEGER ,
transaction_line_no INTEGER ,
sale_or_return_indicator VARCHAR(1) ,
sales_associate_no INTEGER ,
style_id INTEGER ,
color_code INTEGER ,
size_description VARCHAR(60) ,
quantity INTEGER ,
net_retail DOUBLE PRECISION ,
cost DOUBLE PRECISION ,
markdown_percent INTEGER ,
coupon_code VARCHAR(10) ,
class_code INTEGER ,
net_retail_central DOUBLE PRECISION ,
product_code BIGINT ,
fs_sk INTEGER,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_adobe_productview_abandon;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_adobe_productview_abandon(
dt TIMESTAMP WITHOUT TIME ZONE ,
visitor_id VARCHAR(41) ,
webs_customer_id VARCHAR(11) ,
products VARCHAR(13) ,
product_views INTEGER ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id VARCHAR(11) ,
sas_product_id VARCHAR(20) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_zeta_response;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_zeta_response(
campaign_id INTEGER ,
campaign_name VARCHAR(340) ,
mailing_id INTEGER ,
mailing_name VARCHAR(340) ,
cell_id INTEGER ,
cell_name VARCHAR(109) ,
mailing_udf1 VARCHAR(51) ,
mailing_udf2 VARCHAR(51) ,
mailing_udf3 VARCHAR(51) ,
email_subject VARCHAR(435) ,
customer_no BIGINT ,
email_address VARCHAR(435) ,
event_date TIMESTAMP WITHOUT TIME ZONE ,
event_type VARCHAR(17) ,
conv_amount INTEGER ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cust_attribute;
CREATE TABLE IF NOT EXISTS vfap_retail.cust_attribute(
sas_brand_id INTEGER ,
customer_id INTEGER ,
attribute_grouping_code VARCHAR(4) ,
attribute_code VARCHAR(5) ,
attribute_comment VARCHAR(61) ,
attribute_value INTEGER ,
attribute_date TIMESTAMP WITHOUT TIME ZONE ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.trans_header;
CREATE TABLE IF NOT EXISTS vfap_retail.trans_header(
sas_brand_id INTEGER ,
customer_id INTEGER ,
customer_no BIGINT ,
transaction_id INTEGER ,
sales_module_transaction_id INTEGER ,
tender_type VARCHAR(4) ,
store_no INTEGER ,
sales_associate_no INTEGER ,
transaction_type VARCHAR(1) ,
transaction_date TIMESTAMP WITHOUT TIME ZONE ,
pos_transaction_no INTEGER ,
register_no INTEGER ,
segmentation_flag_a VARCHAR(5) ,
total_net_retail DOUBLE PRECISION ,
no_transaction_lines INTEGER ,
total_net_retail_central DOUBLE PRECISION ,
posted_date TIMESTAMP WITHOUT TIME ZONE ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;
COMMIT;

DROP table vfap_retail.tnf_email_open_hist;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_open_hist(
account_id INTEGER ,
list_id INTEGER ,
riid BIGINT ,
custom_properties VARCHAR(40) ,
campaign_id INTEGER ,
launch_id INTEGER ,
email_format VARCHAR(3) ,
id INTEGER ,
event_uuid VARCHAR(38) ,
event_type_id INTEGER ,
event_captured_dt TIMESTAMP WITHOUT TIME ZONE ,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE ,
customer_no BIGINT ,
customer_id BIGINT ,
sas_brand_id INTEGER  DEFAULT 4,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.weather_hist;
CREATE TABLE IF NOT EXISTS vfap_retail.weather_hist(
location VARCHAR(10) ,
municipality VARCHAR(30) ,
state VARCHAR(2) ,
county VARCHAR(60) ,
dt TIMESTAMP WITHOUT TIME ZONE ,
mintempdegf double precision ,
maxtempdegf double precision ,
prcpin double precision ,
snowin double precision ,
wspdmph double precision ,
wdirdeg double precision ,
gustmph double precision ,
rhpct INTEGER ,
skycpct INTEGER ,
presmb double precision ,
poppct double precision ,
uvindex double precision ,
wxicon double precision ,
mintemplydegf double precision ,
maxtemplydegf double precision ,
mintempnormdegf double precision ,
maxtempnormdegf double precision ,
fs_sk INTEGER ,
sas_brand_id INTEGER ,
sas_process_dt TIMESTAMP WITHOUT TIME ZONE ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cust_alt_key;
CREATE TABLE IF NOT EXISTS vfap_retail.cust_alt_key(
sas_brand_id INTEGER ,
customer_id INTEGER ,
alternate_key VARCHAR(27) ,
alternate_key_code VARCHAR(4) ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cm_abandon;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_abandon( -- not done table empty in vfap_retail
session_id VARCHAR(20) ,
cookie_id VARCHAR(24) ,
ts TIMESTAMP WITHOUT TIME ZONE ,
product_id VARCHAR(256) ,
product_name VARCHAR(256) ,
product_category VARCHAR(256) ,
product_category_id VARCHAR(256) ,
product_category_top VARCHAR(256) ,
product_category_bottom VARCHAR(256) ,
base_price DOUBLE PRECISION ,
qy INTEGER ,
order_id VARCHAR(64) ,
site_id VARCHAR(100) ,
abandonment_attribute_1 VARCHAR(256) ,
abandonment_attribute_2 VARCHAR(256) ,
abandonment_attribute_3 VARCHAR(256) ,
abandonment_attribute_4 VARCHAR(256) ,
abandonment_attribute_5 VARCHAR(256) ,
abandonment_attribute_6 VARCHAR(256) ,
abandonment_attribute_7 VARCHAR(256) ,
abandonment_attribute_8 VARCHAR(256) ,
abandonment_attribute_9 VARCHAR(256) ,
abandonment_attribute_10 VARCHAR(256) ,
abandonment_attribute_11 VARCHAR(256) ,
abandonment_attribute_12 VARCHAR(256) ,
abandonment_attribute_13 VARCHAR(256) ,
abandonment_attribute_14 VARCHAR(256) ,
abandonment_attribute_15 VARCHAR(256) ,
abandonment_attribute_16 VARCHAR(256) ,
abandonment_attribute_17 VARCHAR(256) ,
abandonment_attribute_18 VARCHAR(256) ,
abandonment_attribute_19 VARCHAR(256) ,
abandonment_attribute_20 VARCHAR(256) ,
abandonment_attribute_21 VARCHAR(256) ,
abandonment_attribute_22 VARCHAR(256) ,
abandonment_attribute_23 VARCHAR(256) ,
abandonment_attribute_24 VARCHAR(256) ,
abandonment_attribute_25 VARCHAR(256) ,
abandonment_attribute_26 VARCHAR(256) ,
abandonment_attribute_27 VARCHAR(256) ,
abandonment_attribute_28 VARCHAR(256) ,
abandonment_attribute_29 VARCHAR(256) ,
abandonment_attribute_30 VARCHAR(256) ,
abandonment_attribute_31 VARCHAR(256) ,
abandonment_attribute_32 VARCHAR(256) ,
abandonment_attribute_33 VARCHAR(256) ,
abandonment_attribute_34 VARCHAR(256) ,
abandonment_attribute_35 VARCHAR(256) ,
abandonment_attribute_36 VARCHAR(256) ,
abandonment_attribute_37 VARCHAR(256) ,
abandonment_attribute_38 VARCHAR(256) ,
abandonment_attribute_39 VARCHAR(256) ,
abandonment_attribute_40 VARCHAR(256) ,
abandonment_attribute_41 VARCHAR(256) ,
abandonment_attribute_42 VARCHAR(256) ,
abandonment_attribute_43 VARCHAR(256) ,
abandonment_attribute_44 VARCHAR(256) ,
abandonment_attribute_45 VARCHAR(256) ,
abandonment_attribute_46 VARCHAR(256) ,
abandonment_attribute_47 VARCHAR(256) ,
abandonment_attribute_48 VARCHAR(256) ,
abandonment_attribute_49 VARCHAR(256) ,
abandonment_attribute_50 VARCHAR(256) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id INTEGER ,
sas_product_id VARCHAR(20) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cm_registration;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_registration(
session_id VARCHAR(34) ,
cookie_id VARCHAR(41) ,
registration_id VARCHAR(435) ,
new_repeat_buyer VARCHAR(2) ,
new_repeat_visitor VARCHAR(2) ,
visitor_customer_flag VARCHAR(2) ,
registered_city VARCHAR(435) ,
registered_state VARCHAR(435) ,
registered_country VARCHAR(435) ,
registered_zip_code VARCHAR(435) ,
gndr VARCHAR(2) ,
email_address VARCHAR(435) ,
age INTEGER ,
registration_attribute_1 VARCHAR(435) ,
registration_attribute_2 VARCHAR(435) ,
registration_attribute_3 VARCHAR(435) ,
registration_attribute_4 VARCHAR(435) ,
registration_attribute_5 VARCHAR(435) ,
registration_attribute_6 VARCHAR(435) ,
registration_attribute_7 VARCHAR(435) ,
registration_attribute_8 VARCHAR(435) ,
registration_attribute_9 VARCHAR(435) ,
registration_attribute_10 VARCHAR(435) ,
registration_attribute_11 VARCHAR(435) ,
registration_attribute_12 VARCHAR(435) ,
registration_attribute_13 VARCHAR(435) ,
registration_attribute_14 VARCHAR(435) ,
registration_attribute_15 VARCHAR(435) ,
registration_attribute_16 VARCHAR(435) ,
registration_attribute_17 VARCHAR(435) ,
registration_attribute_18 VARCHAR(435) ,
registration_attribute_19 VARCHAR(435) ,
registration_attribute_20 VARCHAR(435) ,
registration_attribute_21 VARCHAR(435) ,
registration_attribute_22 VARCHAR(435) ,
registration_attribute_23 VARCHAR(435) ,
registration_attribute_24 VARCHAR(435) ,
registration_attribute_25 VARCHAR(435) ,
registration_attribute_26 VARCHAR(435) ,
registration_attribute_27 VARCHAR(435) ,
registration_attribute_28 VARCHAR(435) ,
registration_attribute_29 VARCHAR(435) ,
registration_attribute_30 VARCHAR(435) ,
registration_attribute_31 VARCHAR(435) ,
registration_attribute_32 VARCHAR(435) ,
registration_attribute_33 VARCHAR(435) ,
registration_attribute_34 VARCHAR(435) ,
registration_attribute_35 VARCHAR(435) ,
registration_attribute_36 VARCHAR(435) ,
registration_attribute_37 VARCHAR(435) ,
registration_attribute_38 VARCHAR(435) ,
registration_attribute_39 VARCHAR(435) ,
registration_attribute_40 VARCHAR(435) ,
registration_attribute_41 VARCHAR(435) ,
registration_attribute_42 VARCHAR(435) ,
registration_attribute_43 VARCHAR(435) ,
registration_attribute_44 VARCHAR(435) ,
registration_attribute_45 VARCHAR(435) ,
registration_attribute_46 VARCHAR(435) ,
registration_attribute_47 VARCHAR(435) ,
registration_attribute_48 VARCHAR(435) ,
registration_attribute_49 VARCHAR(435) ,
registration_attribute_50 VARCHAR(435) ,
last_update_date TIMESTAMP WITHOUT TIME ZONE ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;


DROP table vfap_retail.cust_xref_final;
CREATE TABLE IF NOT EXISTS vfap_retail.cust_xref_final(
sas_brand_id INTEGER ,
customer_no BIGINT ,
customer_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.sl_message_resp;
CREATE TABLE IF NOT EXISTS vfap_retail.sl_message_resp(
sl_member_id VARCHAR(11) ,
message_id INTEGER ,
label VARCHAR(67) ,
message_action VARCHAR(7) ,
sl_activity_ts TIMESTAMP WITHOUT TIME ZONE ,
sl_type VARCHAR(8) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cm_session_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_session_xref(
session_id VARCHAR(34) ,
customer_id INTEGER ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.sl_orders;
CREATE TABLE IF NOT EXISTS vfap_retail.sl_orders(
sl_activity_ts TIMESTAMP WITHOUT TIME ZONE ,
sl_type VARCHAR(11) ,
sl_member_id VARCHAR(11) ,
sl_context VARCHAR(6) ,
sl_subtype VARCHAR(4) ,
sl_parent_id INTEGER ,
sl_metric VARCHAR(5) ,
sl_value DOUBLE PRECISION  ,
sl_integration_id VARCHAR(24) ,
sl_location INTEGER , --varchar() in vfapdsmigration
earn_type VARCHAR(4) ,
sl_tz VARCHAR(19) ,
sl_order_status VARCHAR(9) ,
sl_transaction_id BIGINT ,
sl_employee_id INTEGER ,
sl_subtotal DOUBLE PRECISION  ,
sl_overall_discount INTEGER ,
sl_item_count INTEGER ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cust;
CREATE TABLE IF NOT EXISTS vfap_retail.cust(
sas_brand_id INTEGER ,
customer_id INTEGER ,
customer_no BIGINT ,
first_name VARCHAR(34) ,
last_name VARCHAR(51) ,
email_address VARCHAR(111) ,
opt_in_flag INTEGER ,
store_no BIGINT ,
language_code VARCHAR(5) ,
alpha_key VARCHAR(24) ,
gender VARCHAR(2) ,
create_source VARCHAR(17) ,
landmark_date_a TIMESTAMP WITHOUT TIME ZONE ,
create_date TIMESTAMP WITHOUT TIME ZONE ,
last_update_date TIMESTAMP WITHOUT TIME ZONE ,
sales_associate_no INTEGER ,
head_of_household_flag VARCHAR(9) ,
opt_in_date TIMESTAMP WITHOUT TIME ZONE ,
segmentation_flag_a VARCHAR(9) ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;
COMMIT;

DROP table vfap_retail.address;
CREATE TABLE IF NOT EXISTS vfap_retail.address(
customer_id INTEGER ,
customer_no BIGINT ,
address_id INTEGER ,
country_code VARCHAR(5) ,
address_match_key VARCHAR(40) ,
telephone_no VARCHAR(24) ,
address_type_code VARCHAR(7) ,
address_1 VARCHAR(58) ,
address_2 VARCHAR(41) ,
address_3 VARCHAR(44) ,
address_4 VARCHAR(4) ,
post_code VARCHAR(57) ,
date_last_modified TIMESTAMP WITHOUT TIME ZONE ,
mail_opt_in_flag INTEGER ,
mail_opt_in_date TIMESTAMP WITHOUT TIME ZONE ,
phone_opt_in_flag INTEGER ,
phone_opt_in_date TIMESTAMP WITHOUT TIME ZONE ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.email_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.email_xref(
email_address VARCHAR(111) ,
customer_id INTEGER ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP 
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_coupon;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_coupon(
sas_brand_id INTEGER ,
detail_line_number INTEGER ,
pos_trans_no INTEGER ,
store INTEGER ,
register INTEGER ,
transaction_date TIMESTAMP WITHOUT TIME ZONE ,
deal_number VARCHAR(7) ,
event_number INTEGER ,
promotion_amount DOUBLE PRECISION ,
coupon_number VARCHAR(26) ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_email_click;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_click(
sas_brand_id INTEGER ,
customer_id INTEGER ,
event_type_id INTEGER ,
account_id INTEGER ,
list_id INTEGER ,
riid BIGINT ,
customer_no BIGINT ,
event_captured_dt TIMESTAMP WITHOUT TIME ZONE ,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE ,
campaign_id INTEGER ,
launch_id INTEGER ,
email_format VARCHAR(1) ,
offer_name VARCHAR(24) ,
offer_number INTEGER ,
offer_category VARCHAR(28) ,
offer_url VARCHAR(216) ,
store_id VARCHAR(10) ,
operating_system VARCHAR(3) ,
browser VARCHAR(2) ,
browser_type VARCHAR(2) ,
user_agent_string VARCHAR(100) ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.sl_challenge_resp;
CREATE TABLE IF NOT EXISTS vfap_retail.sl_challenge_resp(
challenge_id VARCHAR(20) ,-- value too long error changing it to 30
member_id VARCHAR(19) ,
challenge_name VARCHAR(88) ,
challenge_label VARCHAR(92) ,
response VARCHAR(85) , -- value too long error changing it to 150
challenge_status VARCHAR(27) ,
sl_activity_ts TIMESTAMP WITHOUT TIME ZONE ,
sl_type VARCHAR(15) ,
sl_subtype VARCHAR(24) ,
sl_context VARCHAR(20) , -- value too long error changing it to 30
sl_metric VARCHAR(15) ,
sl_value VARCHAR(8) ,
sl_client VARCHAR(17) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.sl_reward_resp;
CREATE TABLE IF NOT EXISTS vfap_retail.sl_reward_resp(
sl_activity_ts TIMESTAMP WITHOUT TIME ZONE ,
sl_type VARCHAR(9) ,
sl_member_id VARCHAR(11) ,
sl_context VARCHAR(9) ,
sl_subtype VARCHAR(7) ,
sl_label VARCHAR(57) ,
sl_parent_id INTEGER ,
sl_parent_name VARCHAR(54) ,
sl_metric VARCHAR(5) ,
sl_value INTEGER ,
sl_creator_type VARCHAR(6) ,
sl_client VARCHAR(10) ,
sl_status VARCHAR(9) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_tibco;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_tibco(
loyaltylabrewardid BIGINT ,
rewardcompensation VARCHAR(1) ,
rewardtext VARCHAR(15) ,
rewardvalue INTEGER ,
retailershopperid VARCHAR(17) ,
instorecardshopperid BIGINT ,
firstname VARCHAR(26) , --value too long error changing it to 50
middlename VARCHAR(2) ,
lastname VARCHAR(27) ,
addressline1 VARCHAR(29) ,
addressline2 VARCHAR(5) ,
city VARCHAR(19) ,
state VARCHAR(2) ,
postalcode VARCHAR(10) ,
phonenumber VARCHAR(18) ,
email VARCHAR(33) ,
processdatetime TIMESTAMP WITHOUT TIME ZONE ,
pointbalance INTEGER ,
pointbalanceextractdate TIMESTAMP WITHOUT TIME ZONE ,
offerid BIGINT ,
offername VARCHAR(32) ,
loyaltyprogramid BIGINT ,
sku VARCHAR(1) ,
actualvalue VARCHAR(1) ,
valuetypecode VARCHAR(1) ,
code VARCHAR(1) ,
issuedate VARCHAR(19) ,
expirationtext VARCHAR(1) ,
expirationdate VARCHAR(19) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id INTEGER ,
pointsawarded INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.experian_behavior_stage_1;
CREATE TABLE IF NOT EXISTS vfap_retail.experian_behavior_stage_1(
customer_no BIGINT ,
first_name VARCHAR(19) ,
last_name VARCHAR(23) ,
email_address VARCHAR(48) ,
mailresponder_indiv VARCHAR(1) ,
multicodmresp_hh VARCHAR(1) ,
multicategorybuyer VARCHAR(1) ,
dmcat_giftgadget INTEGER ,
dmcat_collspcfood INTEGER ,
dmcat_books INTEGER ,
dmcat_gardenfarm INTEGER ,
dmcat_craftshobbies INTEGER ,
dmcat_femaleorient INTEGER ,
dmcat_maleorient INTEGER ,
dmcat_upscale INTEGER ,
dmcat_general INTEGER ,
magbyrcat_healthfit INTEGER ,
magbyrcat_culinary INTEGER ,
magbyrcat_gardenfarm INTEGER ,
magbyrcat_religious INTEGER ,
magbyrcat_malesport INTEGER ,
magbyrcat_female INTEGER ,
magbyrcat_familygen INTEGER ,
contribcat_general INTEGER ,
contribcat_health INTEGER ,
contribcat_political INTEGER ,
contribcat_religious INTEGER ,
sweepstakes INTEGER ,
do_it_yourselfers INTEGER ,
news_financial INTEGER ,
photography INTEGER ,
mailrespond_oddsends INTEGER ,
mailrespond_misc INTEGER ,
political_affiliation VARCHAR(2) ,
firsttimehmebuyr_enh INTEGER ,
interest_in_golf VARCHAR(1) ,
contrib_to_charities VARCHAR(1) ,
pet_enthusiast VARCHAR(1) ,
z_culturalarts VARCHAR(1) ,
mail_order_buyer VARCHAR(1) ,
z_int_in_fitness VARCHAR(1) ,
z_int_in_outdoors VARCHAR(1) ,
z_int_in_travel VARCHAR(1) ,
z_investors VARCHAR(1) ,
presence_of_auto VARCHAR(1) ,
presence_creditcard VARCHAR(1) ,
z_int_gardening VARCHAR(1) ,
z_int_crafts VARCHAR(1) ,
z_collectors VARCHAR(1) ,
z_cruise_enthusiasts VARCHAR(1) ,
z_int_sports VARCHAR(1) ,
z_int_gourmet_cook VARCHAR(1) ,
z_sweepstakes_gambling VARCHAR(1) ,
z_int_politics VARCHAR(1) ,
z_int_music VARCHAR(1) ,
z_int_reading VARCHAR(1) ,
z_childparentingprod VARCHAR(1) ,
z_do_it_yourselfer VARCHAR(1) ,
z_self_improve VARCHAR(1) ,
z_int_religion VARCHAR(1) ,
z_grandparent VARCHAR(1) ,
z_int_clothing VARCHAR(1) ,
z_donate_environment VARCHAR(1) ,
z_investmutualfunds VARCHAR(1) ,
z_weight_conscious VARCHAR(1) ,
z_mailordermultibuyer VARCHAR(1) ,
z_pres_premiumcc VARCHAR(1) ,
z_dog_enthusiasts VARCHAR(1) ,
z_cat_enthusiasts VARCHAR(1) ,
z_healthy_living VARCHAR(1) ,
z_int_automobile VARCHAR(1) ,
z_int_skiing VARCHAR(1) ,
z_int_boating VARCHAR(1) ,
z_present_cellphone VARCHAR(1) ,
z_comm_connectivity VARCHAR(1) ,
z_comp_peripherals VARCHAR(1) ,
z_hi_tech_owner VARCHAR(1) ,
z_homedecor_furnish VARCHAR(1) ,
z_homeenter_tv_video VARCHAR(1) ,
z_kitchaidsmapplianc VARCHAR(1) ,
z_mailorder_musicvideo VARCHAR(1) ,
z_mailorder_booksmags VARCHAR(1) ,
z_mailorderclothshoe VARCHAR(1) ,
z_mailorder_insfin VARCHAR(1) ,
z_mailorder_gifts VARCHAR(1) ,
z_mailorder_garden VARCHAR(1) ,
z_mailorder_jewelcos VARCHAR(1) ,
z_music_classopera VARCHAR(1) ,
z_music_country VARCHAR(1) ,
z_music_christian VARCHAR(1) ,
z_music_rock VARCHAR(1) ,
z_interntonlne_subsc VARCHAR(1) ,
z_photography VARCHAR(1) ,
z_pur_via_online VARCHAR(1) ,
z_int_afflulifestyle VARCHAR(1) ,
z_domestic_traveler VARCHAR(1) ,
z_foreign_traveler VARCHAR(1) ,
z_contactlenswearer VARCHAR(1) ,
z_prescrip_druguser VARCHAR(1) ,
z_int_videodvd VARCHAR(1) ,
z_active_military VARCHAR(1) ,
z_inactive_military VARCHAR(1) ,
morbank_dedcathit INTEGER ,
morbank_nondedcathit INTEGER ,
political_affil_2 VARCHAR(2) ,
political_affil_3 VARCHAR(2) ,
political_affil_4 VARCHAR(2) ,
political_affil_5 VARCHAR(2) ,
political_affil_6 VARCHAR(2) ,
political_affil_7 VARCHAR(2) ,
political_affil_8 VARCHAR(2) ,
through_the_mail VARCHAR(1) ,
other_collectibles VARCHAR(1) ,
art_antiques VARCHAR(1) ,
stamps_coins VARCHAR(1) ,
dolls VARCHAR(1) ,
figurines VARCHAR(1) ,
sports_memorabilia VARCHAR(1) ,
cooking VARCHAR(1) ,
baking VARCHAR(1) ,
cooking_weightcons VARCHAR(1) ,
wine_appreciation VARCHAR(1) ,
cooking_gourmet VARCHAR(1) ,
crafts VARCHAR(1) ,
knitting_needlework VARCHAR(1) ,
quilting VARCHAR(1) ,
sewing VARCHAR(1) ,
woodworking VARCHAR(1) ,
losing_weight VARCHAR(1) ,
vitamin_supplements VARCHAR(1) ,
health_naturalfoods VARCHAR(1) ,
hobby_photography VARCHAR(1) ,
hobby_gardening VARCHAR(1) ,
home_workshop_diy VARCHAR(1) ,
cars_auto_repair VARCHAR(1) ,
bird_watching VARCHAR(1) ,
self_improvement VARCHAR(1) ,
christian_gospel VARCHAR(1) ,
r_and_b VARCHAR(1) ,
jazz_newage VARCHAR(1) ,
classical VARCHAR(1) ,
rock_n_roll VARCHAR(1) ,
country VARCHAR(1) ,
music_general VARCHAR(1) ,
other_music VARCHAR(1) ,
best_selling_fiction VARCHAR(1) ,
bible_religious VARCHAR(1) ,
childrensreading VARCHAR(1) ,
computer VARCHAR(1) ,
cooking_culinary VARCHAR(1) ,
country_lifestyle VARCHAR(1) ,
entertainment_people VARCHAR(1) ,
fashion VARCHAR(1) ,
history VARCHAR(1) ,
interior_decorating VARCHAR(1) ,
medical_health VARCHAR(1) ,
military VARCHAR(1) ,
mystery VARCHAR(1) ,
naturalhlth_remedies VARCHAR(1) ,
romance VARCHAR(1) ,
science_fiction VARCHAR(1) ,
science_technology VARCHAR(1) ,
sports_reading VARCHAR(1) ,
worldnews_politics VARCHAR(1) ,
book_reader VARCHAR(1) ,
animal_welfare_socl VARCHAR(1) ,
environment_wildlife VARCHAR(1) ,
political_conserve VARCHAR(1) ,
political_liberal VARCHAR(1) ,
religious_socialconc VARCHAR(1) ,
children VARCHAR(1) ,
veterans VARCHAR(1) ,
health_social_conc VARCHAR(1) ,
other_social_conc VARCHAR(1) ,
sportsrec_baseball VARCHAR(1) ,
sportsrec_basketball VARCHAR(1) ,
sportsrec_hockey VARCHAR(1) ,
sportsrec_camp_hike VARCHAR(1) ,
sportsrec_hunting VARCHAR(1) ,
sportsrec_fishing VARCHAR(1) ,
sportsrec_nascar VARCHAR(1) ,
sportsrec_per_fitexr VARCHAR(1) ,
sportsrec_scuba_dive VARCHAR(1) ,
sportsrec_football VARCHAR(1) ,
sportsrec_golf VARCHAR(1) ,
sportsrec_snowskibrd VARCHAR(1) ,
sportsrec_boat_sail VARCHAR(1) ,
sportsrec_walking VARCHAR(1) ,
sportsrec_cycling VARCHAR(1) ,
sportsrec_motorcycle VARCHAR(1) ,
sportsrec_run_jog VARCHAR(1) ,
sportsrec_soccer VARCHAR(1) ,
sportsrec_swim VARCHAR(1) ,
playsports_general VARCHAR(1) ,
sweepstakes2 VARCHAR(1) ,
lotteries VARCHAR(1) ,
casino_gambling VARCHAR(1) ,
cruise VARCHAR(1) ,
international VARCHAR(1) ,
domestic VARCHAR(1) ,
time_share VARCHAR(1) ,
recreational_vehicle VARCHAR(1) ,
enjoy_rv_travel VARCHAR(1) ,
business_travel VARCHAR(1) ,
personal_travel VARCHAR(1) ,
own_computer VARCHAR(1) ,
use_internet_service VARCHAR(1) ,
useinternetdsl_hspd VARCHAR(1) ,
plan_to_buy_computer VARCHAR(1) ,
cell_phone VARCHAR(1) ,
compact_disc_player VARCHAR(1) ,
dvd_player VARCHAR(1) ,
satellite_dish VARCHAR(1) ,
digital_camera VARCHAR(1) ,
hdtv VARCHAR(1) ,
int_electronics VARCHAR(1) ,
personal_data_asst VARCHAR(1) ,
video_camera VARCHAR(1) ,
video_game_system VARCHAR(1) ,
arts_int_culturlarts VARCHAR(1) ,
business_finance VARCHAR(1) ,
childrens_magazines VARCHAR(1) ,
computer_electronics VARCHAR(1) ,
crafts_games_hobbies VARCHAR(1) ,
fitness VARCHAR(1) ,
food_wine_cooking VARCHAR(1) ,
gardening_magazines VARCHAR(1) ,
hunting_fishing VARCHAR(1) ,
mens VARCHAR(1) ,
music VARCHAR(1) ,
parenting_babies VARCHAR(1) ,
sports_magazines VARCHAR(1) ,
subscription VARCHAR(1) ,
travel VARCHAR(1) ,
womens VARCHAR(1) ,
currentevents_news VARCHAR(1) ,
have_leased_vehicle VARCHAR(1) ,
havepurchasedvehicle VARCHAR(1) ,
book_club VARCHAR(1) ,
music_club VARCHAR(1) ,
amer_express_premium VARCHAR(1) ,
amer_express_regular VARCHAR(1) ,
discover_premium VARCHAR(1) ,
discover_regular VARCHAR(1) ,
othercard_premium VARCHAR(1) ,
othercard_regular VARCHAR(1) ,
store_retail_regular VARCHAR(1) ,
visa_regular VARCHAR(1) ,
mastercard_regular VARCHAR(1) ,
humanitarian VARCHAR(1) ,
buyprerecrded_videos VARCHAR(1) ,
watch_cable_tv VARCHAR(1) ,
watch_videos VARCHAR(1) ,
cds_moneymkt_current VARCHAR(1) ,
iras_current VARCHAR(1) ,
iras_future_interest VARCHAR(1) ,
life_insur_current VARCHAR(1) ,
mutual_funds_current VARCHAR(1) ,
mutualfunds_future VARCHAR(1) ,
otherinvestmt_curr VARCHAR(1) ,
otherinvestmt_future VARCHAR(1) ,
real_estate_current VARCHAR(1) ,
real_estate_future VARCHAR(1) ,
stocks_bonds_current VARCHAR(1) ,
stocks_bonds_future VARCHAR(1) ,
proud_grandparent VARCHAR(1) ,
purchase_via_internet VARCHAR(1) ,
active_military_mbr VARCHAR(1) ,
vetern VARCHAR(1) ,
own_a_cat VARCHAR(1) ,
own_a_dog VARCHAR(1) ,
own_a_pet VARCHAR(1) ,
shop_by_internet VARCHAR(1) ,
shop_by_mail VARCHAR(1) ,
shop_by_catalog VARCHAR(1) ,
apparel VARCHAR(1) ,
computerorelctronics VARCHAR(1) ,
books_music VARCHAR(1) ,
home_garden VARCHAR(1) ,
sports_related VARCHAR(1) ,
big_tall VARCHAR(1) ,
mailorder_books_mag VARCHAR(1) ,
mailorder_clothing VARCHAR(1) ,
mailorder_gardening VARCHAR(1) ,
mailorder_gift VARCHAR(1) ,
mailorder_jewel_cosm VARCHAR(1) ,
mailorder_music_vid VARCHAR(1) ,
mailorder_child_prod VARCHAR(1) ,
int_amuseparkvisit INTEGER ,
int_zoo_visit INTEGER ,
int_coffeeconnoisseu INTEGER ,
int_winelovers INTEGER ,
int_doityourselfers INTEGER ,
int_homeimprve_spndr INTEGER ,
hunting_enthusiasts INTEGER ,
buyer_laptopowner INTEGER ,
buyer_secursysowner INTEGER ,
buyer_tabletowner INTEGER ,
buyer_couponuser INTEGER ,
buyer_luxuryshopper INTEGER ,
buyer_yngadultcloth INTEGER ,
buyer_supercenter INTEGER ,
buyer_warehouseclub INTEGER ,
aarp_members INTEGER ,
life_insur_policy INTEGER ,
medical_policy INTEGER ,
medicare_policy INTEGER ,
digital_magnewspaper INTEGER ,
attnds_educationprog INTEGER ,
video_gamer INTEGER ,
mlb_enthusiast INTEGER ,
nascar_enthusiast INTEGER ,
nba_enthusiast INTEGER ,
nfl_enthusiast INTEGER ,
nhl_enthusiast INTEGER ,
pga_tour_enthusiast INTEGER ,
politicaltv_liberal INTEGER ,
politictv_libcomedy INTEGER ,
politicaltv_conserva INTEGER ,
eats_family_rest INTEGER ,
eats_fast_food INTEGER ,
canoeing_kayaking INTEGER ,
int_play_golf INTEGER ,
presence_automobile INTEGER ,
loyalty_card_user INTEGER ,
luxuryhomegood_shop INTEGER ,
union_member INTEGER ,
have_retirement_plan INTEGER ,
online_trading INTEGER ,
have_grandchildren INTEGER ,
nondept_store_makeup INTEGER ,
int_gourmet_cooking INTEGER ,
int_cat_owner INTEGER ,
int_dog_owner INTEGER ,
int_arts_crafts INTEGER ,
int_scrapbooking INTEGER ,
int_cultural_arts INTEGER ,
hobbies_gardening INTEGER ,
int_photography INTEGER ,
int_book_reader INTEGER ,
int_ebook_reader INTEGER ,
int_audiobk_listener INTEGER ,
int_pet_enthusiast INTEGER ,
int_music_download INTEGER ,
int_music_streaming INTEGER ,
int_avid_runners INTEGER ,
int_outdoor_enthusia INTEGER ,
int_fishing INTEGER ,
int_snowsports INTEGER ,
int_boating INTEGER ,
int_playshockey INTEGER ,
int_plays_soccer INTEGER ,
int_plays_tennis INTEGER ,
int_sportsenthusiast INTEGER ,
int_healthy_living INTEGER ,
int_fitnessenthusias INTEGER ,
int_on_a_diet INTEGER ,
int_weight_conscious INTEGER ,
buyer_highendspirits INTEGER ,
int_casinogambling INTEGER ,
int_swpstakeslottery INTEGER ,
hifreq_business_trvl INTEGER ,
hifreq_cruise_enthus INTEGER ,
hifreq_domestic_vac INTEGER ,
hifreq_foreign_vac INTEGER ,
freq_flyer_prg_mbr INTEGER ,
hotelguest_loyaprg INTEGER ,
contrib_charities INTEGER ,
contrib_artsculture INTEGER ,
contrib_education INTEGER ,
contrib_health INTEGER ,
contrib_political INTEGER ,
contrib_prvt_found INTEGER ,
contrib_volunteering INTEGER ,
debit_card_user INTEGER ,
corp_credit_crd_user INTEGER ,
maj_credit_crd_user INTEGER ,
prem_credit_crd_user INTEGER ,
credit_card_user INTEGER ,
store_creditcrd_user INTEGER ,
brokerage_acct_owner INTEGER ,
active_investor INTEGER ,
mutual_fund_investor INTEGER ,
deptstore_makeupuser INTEGER ,
int_christian_music INTEGER ,
int_classical_music INTEGER ,
int_country_music INTEGER ,
int_music INTEGER ,
int_oldies_music INTEGER ,
int_rock_music INTEGER ,
int_80s_music INTEGER ,
int_hiphopmusic INTEGER ,
int_alternativemusic INTEGER ,
int_jazz_music INTEGER ,
int_pop_music INTEGER ,
interest_in_religion INTEGER ,
military_active INTEGER ,
military_inactive INTEGER ,
working_couples INTEGER ,
poc_0_3_yrs_gender VARCHAR(1) ,
poc_4_6_yrs_gender VARCHAR(1) ,
poc_7_9_yrs_gender VARCHAR(1) ,
poc_10_12_yrs_gender VARCHAR(1) ,
poc_13_15_yrs_gender VARCHAR(1) ,
poc_16_18_yrs_gender VARCHAR(1) ,
discret_spend_level VARCHAR(1) ,
discret_spend_score INTEGER ,
p1_birthdt_indiv INTEGER ,
p1_combage_indiv VARCHAR(3) ,
p1_gender_indiv VARCHAR(1) ,
p1_marital_status VARCHAR(2) ,
recip_reliability_cd INTEGER ,
p1_pertype_indiv VARCHAR(1) ,
combined_homeowner VARCHAR(1) ,
dwelling_type VARCHAR(1) ,
length_of_residence INTEGER ,
dwellingunit_size_cd VARCHAR(1) ,
number_child_ltet18 INTEGER ,
nbr_adults_in_hh INTEGER ,
p2_person_type VARCHAR(1) ,
p2_name_first VARCHAR(15) ,
p2_name_mid_init VARCHAR(15) ,
p2_name_last VARCHAR(20) ,
p2_name_title VARCHAR(6) ,
p2_name_surnamesffx VARCHAR(4) ,
p2_gender_cd VARCHAR(1) ,
p2_combined_age VARCHAR(3) ,
p2_birthdt_ccyymm INTEGER ,
p3_person_type VARCHAR(1) ,
p3_name_first VARCHAR(15) ,
p3_name_mid_init VARCHAR(15) ,
p3_name_last VARCHAR(20) ,
p3_name_title VARCHAR(6) ,
p3_name_surnamesffx VARCHAR(4) ,
p3_gender_cd VARCHAR(1) ,
p3_combined_age VARCHAR(3) ,
p3_birthdt_ccyymm INTEGER ,
p4_person_type VARCHAR(1) ,
p4_name_first VARCHAR(15) ,
p4_name_mid_init VARCHAR(15) ,
p4_name_last VARCHAR(20) ,
p4_name_title VARCHAR(6) ,
p4_name_surnamesffx VARCHAR(4) ,
p4_gender_cd VARCHAR(1) ,
p4_combined_age VARCHAR(3) ,
p4_birthdt_ccyymm INTEGER ,
p5_person_type VARCHAR(1) ,
p5_name_first VARCHAR(15) ,
p5_name_mid_init VARCHAR(14) ,
p5_name_last VARCHAR(20) ,
p5_name_title VARCHAR(5) ,
p5_name_surnamesffx VARCHAR(4) ,
p5_gender_cd VARCHAR(1) ,
p5_combined_age VARCHAR(3) ,
p5_birthdt_ccyymm INTEGER ,
p6_person_type VARCHAR(1) ,
p6_name_first VARCHAR(15) ,
p6_name_mid_init VARCHAR(12) ,
p6_name_last VARCHAR(20) ,
p6_name_title VARCHAR(6) ,
p6_name_surnamesffx VARCHAR(4) ,
p6_gender_cd VARCHAR(1) ,
p6_combined_age VARCHAR(3) ,
p6_birthdt_ccyymm INTEGER ,
p7_person_type VARCHAR(1) ,
p7_name_first VARCHAR(15) ,
p7_name_mid_init VARCHAR(11) ,
p7_name_last VARCHAR(19) ,
p7_name_title VARCHAR(4) ,
p7_name_surnamesffx VARCHAR(4) ,
p7_gender_cd VARCHAR(1) ,
p7_combined_age VARCHAR(3) ,
p7_birthdt_ccyymm INTEGER ,
p8_person_type VARCHAR(1) ,
p8_name_first VARCHAR(15) ,
p8_name_mid_init VARCHAR(10) ,
p8_name_last VARCHAR(20) ,
p8_name_title VARCHAR(4) ,
p8_name_surnamesffx VARCHAR(3) ,
p8_gender_cd VARCHAR(1) ,
p8_combined_age VARCHAR(3) ,
p8_birthdt_ccyymm INTEGER ,
p1_occupation VARCHAR(8) ,
p1_occupation_grp VARCHAR(2) ,
p1_education VARCHAR(8) ,
presofchild_age_0_3 VARCHAR(2) ,
presofchild_age_4_6 VARCHAR(2) ,
presofchild_age_7_9 VARCHAR(2) ,
presofchild_age10_12 VARCHAR(2) ,
presofchild_age13_15 VARCHAR(2) ,
presofchild_age16_18 VARCHAR(2) ,
presofchild_age_0_18 VARCHAR(2) ,
home_business_ind VARCHAR(1) ,
p1_businessownerflg VARCHAR(1) ,
est_curr_home_value VARCHAR(8) ,
multiple_realty_prop VARCHAR(1) ,
mosaic_household VARCHAR(3) ,
est_household_income VARCHAR(1) ,
p1_retail_shoppers VARCHAR(8) ,
consumview_prfitscr INTEGER ,
cs_combined VARCHAR(1) ,
cs_clothing VARCHAR(1) ,
cs_entertainment VARCHAR(1) ,
cs_travel VARCHAR(1) ,
retail_demand_20200 INTEGER ,
retail_demand_20220 INTEGER ,
retail_demand_20240 INTEGER ,
retail_demand_20260 INTEGER ,
retail_demand_20530 INTEGER ,
retail_demand_20580 INTEGER ,
retail_demand_448 INTEGER ,
retail_demand_4481 INTEGER ,
retail_demand_44811 INTEGER ,
retail_demand_44812 INTEGER ,
retail_demand_44813 INTEGER ,
retail_demand_44814 INTEGER ,
retail_demand_44815 INTEGER ,
retail_demand_44819 INTEGER ,
retail_demand_451 INTEGER ,
retail_demand_4511 INTEGER ,
retail_demand_45111 INTEGER ,
avg_scorex_plus INTEGER ,
median_scorex_plus INTEGER ,
badvsgood_credit_seg INTEGER ,
scps_avg_taps_spend INTEGER ,
scpsavg_taps_payrate INTEGER ,
online_apparel INTEGER ,
online_shoes INTEGER ,
online_outdrsftgoods INTEGER ,
online_outdrhrdgoods INTEGER ,
ltd_best_recency TIMESTAMP WITHOUT TIME ZONE ,
ltd_total_dollars INTEGER ,
ltd_avg_dollar_pur INTEGER ,
ltc_credit_pur INTEGER ,
ltd_purchase_freq INTEGER ,
month_24_credit_pur INTEGER ,
month_24_recency_cd INTEGER ,
month_24_freq_cd INTEGER ,
month_24_dollar_cd INTEGER ,
month_24_avgdollarcd INTEGER ,
w_hiend_apparelrecen TIMESTAMP WITHOUT TIME ZONE ,
w_hiend_apparelfreq VARCHAR(8) ,
w_hiend_appareldolla VARCHAR(8) ,
w_hiend_apparelscore VARCHAR(8) ,
w_mid_apparelrecen VARCHAR(8) ,
w_mid_apparelfreq INTEGER ,
w_mid_appareldolla INTEGER ,
w_mid_apparelscore INTEGER ,
w_low_apparelrecen TIMESTAMP WITHOUT TIME ZONE ,
w_low_apparelfreq INTEGER ,
w_low_appareldolla INTEGER ,
w_low_apparelscore INTEGER ,
w_cas_apparelrecen TIMESTAMP WITHOUT TIME ZONE ,
w_cas_apparelfreq INTEGER ,
w_cas_appareldolla INTEGER ,
w_cas_apparelscore INTEGER ,
w_plus_apparelrecen TIMESTAMP WITHOUT TIME ZONE ,
w_plus_apparelfreq INTEGER ,
w_plus_appareldolla INTEGER ,
w_plus_apparelscore INTEGER ,
w_athle_apparelrecen TIMESTAMP WITHOUT TIME ZONE ,
w_athle_apparelfreq INTEGER ,
w_athle_appareldolla INTEGER ,
w_athle_apparelscore INTEGER ,
m_cas_apparelrecen TIMESTAMP WITHOUT TIME ZONE ,
m_cas_apparelfreq INTEGER ,
m_cas_appareldolla INTEGER ,
m_cas_apparelscore INTEGER ,
child_apparelrecen TIMESTAMP WITHOUT TIME ZONE ,
child_apparelfreq INTEGER ,
child_appareldolla INTEGER ,
child_apparelscore INTEGER ,
active_outdoor_recen TIMESTAMP WITHOUT TIME ZONE ,
active_outdoor_freq INTEGER ,
active_outdoor_dolla INTEGER ,
active_outdoor_score INTEGER ,
x_snowsports_recen TIMESTAMP WITHOUT TIME ZONE ,
x_snowsports_freq INTEGER ,
x_snowsports_dolla INTEGER ,
x_snowsports_score INTEGER ,
mth_0_3_trans_freq INTEGER ,
mth_0_3_trans_dollar INTEGER ,
mth_4_6_trans_freq INTEGER ,
mth_4_6_trans_dollar INTEGER ,
mth_7_9_trans_freq INTEGER ,
mth_7_9_trans_dollar INTEGER ,
mth_10_12_trans_freq INTEGER ,
mth_10_12_trans_doll INTEGER ,
mth_13_18_trans_freq INTEGER ,
mth_13_18_trans_doll INTEGER ,
mth_19_24_trans_freq INTEGER ,
mth_19_24_trans_doll INTEGER ,
most_recent_ordr_chan VARCHAR(1) ,
tt_brick_mortar INTEGER ,
tt_online_midhigh INTEGER ,
tt_e_tailer INTEGER ,
tt_online_discount INTEGER ,
tt_onlinebid_mrktplc INTEGER ,
tt_emailengagement INTEGER ,
p1_tt_emailengagemnt INTEGER ,
p2_tt_emailengagemnt INTEGER ,
p3_tt_emailengagemnt INTEGER ,
p4_tt_emailengagemnt INTEGER ,
p5_tt_emailengagemnt INTEGER ,
p6_tt_emailengagemnt INTEGER ,
p7_tt_emailengagemnt INTEGER ,
p8_tt_emailengagemnt INTEGER ,
tt_broadcast_cabletv INTEGER ,
tt_directmail INTEGER ,
tt_internetradio INTEGER ,
tt_mobile_display INTEGER ,
tt_mobile_video INTEGER ,
tt_online_video INTEGER ,
tt_online_display INTEGER ,
tt_online_streamtv INTEGER ,
tt_satelliteradio INTEGER ,
tt_buyamerican INTEGER ,
tt_showmethemoney INTEGER ,
tt_gowiththeflow INTEGER ,
tt_notimelikepresent INTEGER ,
tt_nvrshowupemptyhan INTEGER ,
tt_ontheroadagain INTEGER ,
tt_lookatmenow INTEGER ,
tt_stopsmellroses INTEGER ,
tt_workhardplayhard INTEGER ,
tt_pennysavedearned INTEGER ,
tt_its_all_in_name INTEGER ,
ethnicinsight_flag VARCHAR(1) ,
ethnicity_detail VARCHAR(2) ,
language VARCHAR(2) ,
religion VARCHAR(1) ,
e_tech_group VARCHAR(1) ,
country_of_origin INTEGER ,
totalenhanmatchtype VARCHAR(1) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;
COMMIT;

--DROP table vfap_retail.csv_vans_zeta_inputs_stage_1;
CREATE TABLE IF NOT EXISTS vfap_retail.csv_vans_zeta_inputs_stage_1(
customer_id INTEGER  ,
ssn_md2o_bts DOUBLE PRECISION ,
ssn_md2o_fco DOUBLE PRECISION ,
ssn_md2o_hco DOUBLE PRECISION ,
ssn_md2o_hol DOUBLE PRECISION ,
ssn_md2o_spr DOUBLE PRECISION ,
ssn_md2o_sum DOUBLE PRECISION ,
ssn_md2c_bts DOUBLE PRECISION ,
ssn_md2c_fco DOUBLE PRECISION ,
ssn_md2c_hco DOUBLE PRECISION ,
ssn_md2c_hol DOUBLE PRECISION ,
ssn_md2c_spr DOUBLE PRECISION ,
ssn_md2c_sum DOUBLE PRECISION ,
ssn_dsinco_bts DOUBLE PRECISION ,
ssn_dsinco_fco DOUBLE PRECISION ,
ssn_dsinco_hco DOUBLE PRECISION ,
ssn_dsinco_hol DOUBLE PRECISION ,
ssn_dsinco_spr DOUBLE PRECISION ,
ssn_dsinco_sum DOUBLE PRECISION ,
ssn_freq_s_bts DOUBLE PRECISION ,
ssn_freq_s_fco DOUBLE PRECISION ,
ssn_freq_s_hco DOUBLE PRECISION ,
ssn_freq_s_hol DOUBLE PRECISION ,
ssn_freq_s_spr DOUBLE PRECISION ,
ssn_freq_s_sum DOUBLE PRECISION ,
ssn_freq_o_bts DOUBLE PRECISION ,
ssn_freq_o_fco DOUBLE PRECISION ,
ssn_freq_o_hco DOUBLE PRECISION ,
ssn_freq_o_hol DOUBLE PRECISION ,
ssn_freq_o_spr DOUBLE PRECISION ,
ssn_freq_o_sum DOUBLE PRECISION ,
ssn_freq_c_bts DOUBLE PRECISION ,
ssn_freq_c_fco DOUBLE PRECISION ,
ssn_freq_c_hco DOUBLE PRECISION ,
ssn_freq_c_hol DOUBLE PRECISION ,
ssn_freq_c_spr DOUBLE PRECISION ,
ssn_freq_c_sum DOUBLE PRECISION ,
ssn_pct_o_bts DOUBLE PRECISION ,
ssn_pct_o_fco DOUBLE PRECISION ,
ssn_pct_o_hco DOUBLE PRECISION ,
ssn_pct_o_hol DOUBLE PRECISION ,
ssn_pct_o_spr DOUBLE PRECISION ,
ssn_pct_o_sum DOUBLE PRECISION ,
ssn_pct_c_bts DOUBLE PRECISION ,
ssn_pct_c_fco DOUBLE PRECISION ,
ssn_pct_c_hco DOUBLE PRECISION ,
ssn_pct_c_hol DOUBLE PRECISION ,
ssn_pct_c_spr DOUBLE PRECISION ,
ssn_pct_c_sum DOUBLE PRECISION ,
genage_md2o_a DOUBLE PRECISION ,
genage_md2o_k DOUBLE PRECISION ,
genage_md2o_m DOUBLE PRECISION ,
genage_md2o_w DOUBLE PRECISION ,
genage_md2c_a DOUBLE PRECISION ,
genage_md2c_k DOUBLE PRECISION ,
genage_md2c_m DOUBLE PRECISION ,
genage_md2c_w DOUBLE PRECISION ,
genage_dsinco_a DOUBLE PRECISION ,
genage_dsinco_k DOUBLE PRECISION ,
genage_dsinco_m DOUBLE PRECISION ,
genage_dsinco_w DOUBLE PRECISION ,
genage_freq_s_a DOUBLE PRECISION ,
genage_freq_s_k DOUBLE PRECISION ,
genage_freq_s_m DOUBLE PRECISION ,
genage_freq_s_w DOUBLE PRECISION ,
genage_freq_o_a DOUBLE PRECISION ,
genage_freq_o_k DOUBLE PRECISION ,
genage_freq_o_m DOUBLE PRECISION ,
genage_freq_o_w DOUBLE PRECISION ,
genage_freq_c_a DOUBLE PRECISION ,
genage_freq_c_k DOUBLE PRECISION ,
genage_freq_c_m DOUBLE PRECISION ,
genage_freq_c_w DOUBLE PRECISION ,
genage_pct_o_a DOUBLE PRECISION ,
genage_pct_o_k DOUBLE PRECISION ,
genage_pct_o_m DOUBLE PRECISION ,
genage_pct_o_w DOUBLE PRECISION ,
genage_pct_c_a DOUBLE PRECISION ,
genage_pct_c_k DOUBLE PRECISION ,
genage_pct_c_m DOUBLE PRECISION ,
genage_pct_c_w DOUBLE PRECISION ,
act_md2o_skate DOUBLE PRECISION ,
act_md2o_snwb DOUBLE PRECISION ,
act_md2o_surf DOUBLE PRECISION ,
act_md2c_skate DOUBLE PRECISION ,
act_md2c_snwb DOUBLE PRECISION ,
act_md2c_surf DOUBLE PRECISION ,
act_dsinco_skate DOUBLE PRECISION ,
act_dsinco_snwb DOUBLE PRECISION ,
act_dsinco_surf DOUBLE PRECISION ,
act_freq_s_skate DOUBLE PRECISION ,
act_freq_s_snwb DOUBLE PRECISION ,
act_freq_s_surf DOUBLE PRECISION ,
act_freq_o_skate DOUBLE PRECISION ,
act_freq_o_snwb DOUBLE PRECISION ,
act_freq_o_surf DOUBLE PRECISION ,
act_freq_c_skate DOUBLE PRECISION ,
act_freq_c_snwb DOUBLE PRECISION ,
act_freq_c_surf DOUBLE PRECISION ,
act_pct_o_skate DOUBLE PRECISION ,
act_pct_o_snwb DOUBLE PRECISION ,
act_pct_o_surf DOUBLE PRECISION ,
act_pct_c_skate DOUBLE PRECISION ,
act_pct_c_snwb DOUBLE PRECISION ,
act_pct_c_surf DOUBLE PRECISION ,
prs_md2o_artcollab DOUBLE PRECISION ,
prs_md2o_collab DOUBLE PRECISION ,
prs_md2o_event DOUBLE PRECISION ,
prs_md2o_new_arri DOUBLE PRECISION ,
prs_md2o_new_cust DOUBLE PRECISION ,
prs_md2o_reward DOUBLE PRECISION ,
prs_md2o_reengage DOUBLE PRECISION ,
prs_md2o_survey DOUBLE PRECISION ,
prs_md2o_abdn_cart DOUBLE PRECISION ,
prs_md2o_rtn_cust DOUBLE PRECISION ,
prs_md2c_artcollab DOUBLE PRECISION ,
prs_md2c_collab DOUBLE PRECISION ,
prs_md2c_event DOUBLE PRECISION ,
prs_md2c_new_arri DOUBLE PRECISION ,
prs_md2c_new_cust DOUBLE PRECISION ,
prs_md2c_reward DOUBLE PRECISION ,
prs_md2c_reengage DOUBLE PRECISION ,
prs_md2c_survey DOUBLE PRECISION ,
prs_md2c_abdn_cart DOUBLE PRECISION ,
prs_md2c_rtn_cust DOUBLE PRECISION ,
prs_dsinco_artcollab DOUBLE PRECISION ,
prs_dsinco_collab DOUBLE PRECISION ,
prs_dsinco_event DOUBLE PRECISION ,
prs_dsinco_new_arri DOUBLE PRECISION ,
prs_dsinco_new_cust DOUBLE PRECISION ,
prs_dsinco_reward DOUBLE PRECISION ,
prs_dsinco_reengage DOUBLE PRECISION ,
prs_dsinco_survey DOUBLE PRECISION ,
prs_dsinco_abdn_cart DOUBLE PRECISION ,
prs_dsinco_rtn_cust DOUBLE PRECISION ,
prs_freq_s_artcollab DOUBLE PRECISION ,
prs_freq_s_collab DOUBLE PRECISION ,
prs_freq_s_event DOUBLE PRECISION ,
prs_freq_s_new_arri DOUBLE PRECISION ,
prs_freq_s_new_cust DOUBLE PRECISION ,
prs_freq_s_reward DOUBLE PRECISION ,
prs_freq_s_reengage DOUBLE PRECISION ,
prs_freq_s_survey DOUBLE PRECISION ,
prs_freq_s_abdn_cart DOUBLE PRECISION ,
prs_freq_s_rtn_cust DOUBLE PRECISION ,
prs_freq_o_artcollab DOUBLE PRECISION ,
prs_freq_o_collab DOUBLE PRECISION ,
prs_freq_o_event DOUBLE PRECISION ,
prs_freq_o_new_arri DOUBLE PRECISION ,
prs_freq_o_new_cust DOUBLE PRECISION ,
prs_freq_o_reward DOUBLE PRECISION ,
prs_freq_o_reengage DOUBLE PRECISION ,
prs_freq_o_survey DOUBLE PRECISION ,
prs_freq_o_abdn_cart DOUBLE PRECISION ,
prs_freq_o_rtn_cust DOUBLE PRECISION ,
prs_freq_c_artcollab DOUBLE PRECISION ,
prs_freq_c_collab DOUBLE PRECISION ,
prs_freq_c_event DOUBLE PRECISION ,
prs_freq_c_new_arri DOUBLE PRECISION ,
prs_freq_c_new_cust DOUBLE PRECISION ,
prs_freq_c_reward DOUBLE PRECISION ,
prs_freq_c_reengage DOUBLE PRECISION ,
prs_freq_c_survey DOUBLE PRECISION ,
prs_freq_c_abdn_cart DOUBLE PRECISION ,
prs_freq_c_rtn_cust DOUBLE PRECISION ,
prs_pct_o_artcollab DOUBLE PRECISION ,
prs_pct_o_collab DOUBLE PRECISION ,
prs_pct_o_event DOUBLE PRECISION ,
prs_pct_o_new_arri DOUBLE PRECISION ,
prs_pct_o_new_cust DOUBLE PRECISION ,
prs_pct_o_reward DOUBLE PRECISION ,
prs_pct_o_reengage DOUBLE PRECISION ,
prs_pct_o_survey DOUBLE PRECISION ,
prs_pct_o_abdn_cart DOUBLE PRECISION ,
prs_pct_o_rtn_cust DOUBLE PRECISION ,
prs_pct_c_artcollab DOUBLE PRECISION ,
prs_pct_c_collab DOUBLE PRECISION ,
prs_pct_c_event DOUBLE PRECISION ,
prs_pct_c_new_arri DOUBLE PRECISION ,
prs_pct_c_new_cust DOUBLE PRECISION ,
prs_pct_c_reward DOUBLE PRECISION ,
prs_pct_c_reengage DOUBLE PRECISION ,
prs_pct_c_survey DOUBLE PRECISION ,
prs_pct_c_abdn_cart DOUBLE PRECISION ,
prs_pct_c_rtn_cust DOUBLE PRECISION ,
chnl_md2o_retail DOUBLE PRECISION ,
chnl_md2o_ecom DOUBLE PRECISION ,
chnl_md2c_retail DOUBLE PRECISION ,
chnl_md2c_ecom DOUBLE PRECISION ,
chnl_dsinco_retail DOUBLE PRECISION ,
chnl_dsinco_ecom DOUBLE PRECISION ,
chnl_freq_s_retail DOUBLE PRECISION ,
chnl_freq_s_ecom DOUBLE PRECISION ,
chnl_freq_o_retail DOUBLE PRECISION ,
chnl_freq_o_ecom DOUBLE PRECISION ,
chnl_freq_c_retail DOUBLE PRECISION ,
chnl_freq_c_ecom DOUBLE PRECISION ,
chnl_pct_o_retail DOUBLE PRECISION ,
chnl_pct_o_ecom DOUBLE PRECISION ,
chnl_pct_c_retail DOUBLE PRECISION ,
chnl_pct_c_ecom DOUBLE PRECISION ,
pcat_md2o_custom DOUBLE PRECISION ,
pcat_md2o_mte DOUBLE PRECISION ,
pcat_md2o_pnut DOUBLE PRECISION ,
pcat_md2c_custom DOUBLE PRECISION ,
pcat_md2c_mte DOUBLE PRECISION ,
pcat_md2c_pnut DOUBLE PRECISION ,
pcat_dsinco_custom DOUBLE PRECISION ,
pcat_dsinco_mte DOUBLE PRECISION ,
pcat_dsinco_pnut DOUBLE PRECISION ,
pcat_freq_s_custom DOUBLE PRECISION ,
pcat_freq_s_mte DOUBLE PRECISION ,
pcat_freq_s_pnut DOUBLE PRECISION ,
pcat_freq_o_custom DOUBLE PRECISION ,
pcat_freq_o_mte DOUBLE PRECISION ,
pcat_freq_o_pnut DOUBLE PRECISION ,
pcat_freq_c_custom DOUBLE PRECISION ,
pcat_freq_c_mte DOUBLE PRECISION ,
pcat_freq_c_pnut DOUBLE PRECISION ,
pcat_pct_o_custom DOUBLE PRECISION ,
pcat_pct_o_mte DOUBLE PRECISION ,
pcat_pct_o_pnut DOUBLE PRECISION ,
pcat_pct_c_custom DOUBLE PRECISION ,
pcat_pct_c_mte DOUBLE PRECISION ,
pcat_pct_c_pnut DOUBLE PRECISION ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.sl_members;
CREATE TABLE IF NOT EXISTS vfap_retail.sl_members(
member_id VARCHAR(11) ,
email VARCHAR(53) ,
first_name VARCHAR(150) ,
last_name VARCHAR(150) ,
mailing_street VARCHAR(153) ,
mailing_city VARCHAR(116) ,
mailing_state VARCHAR(2) ,
mailing_postal_code VARCHAR(212) ,
mailing_country VARCHAR(2) ,
birthdate TIMESTAMP WITHOUT TIME ZONE ,
mobile_phone BIGINT ,
member_since TIMESTAMP WITHOUT TIME ZONE ,
receive_sms_offers VARCHAR(5) ,
receive_e_statements VARCHAR(5) ,
suspend_email VARCHAR(5) ,
suspend_email_date TIMESTAMP WITHOUT TIME ZONE ,
suspend_email_cause VARCHAR(2199) ,
referrer_id VARCHAR(11) ,
last_active_at TIMESTAMP WITHOUT TIME ZONE ,
deactivated VARCHAR(5) ,
integration_id BIGINT ,
test_member VARCHAR(5) ,
visitor VARCHAR(5) ,
card_id VARCHAR(20) ,
created_at TIMESTAMP WITHOUT TIME ZONE ,
sl_contest VARCHAR(1) ,
sl_prize VARCHAR(1) ,
sl_referral bigint ,
point VARCHAR(19) ,
points_earned_total BIGINT ,
points_redeemed_total BIGINT ,
points_expired_total BIGINT ,
source VARCHAR(8) ,
sl_challenge VARCHAR(1) ,
sl_push INTEGER ,
crmnumber BIGINT ,
sl_login DOUBLE PRECISION  ,
employee VARCHAR(5) ,
sl_cancel_earn INTEGER ,
tenure INTEGER ,
sl_adjust_redeem INTEGER ,
shoe_size DOUBLE PRECISION  ,
sl_member_attribute INTEGER ,
sl_offer INTEGER ,
store_number VARCHAR(37) ,
sl_adjust_earn INTEGER ,
mailing_street2 VARCHAR(120) ,
sl_cancel_redeem BIGINT  ,
beta_allowed VARCHAR(5) ,
sl_survey INTEGER ,
sl_view INTEGER ,
has_device VARCHAR(5) ,
sl_reward INTEGER ,
sl_member_confirm INTEGER ,
confirmed_at TIMESTAMP WITHOUT TIME ZONE ,
sl_email INTEGER ,
sl_post VARCHAR(1) ,
interests VARCHAR(57) ,
receive_personalized_offers VARCHAR(5) ,
sl_member_preference INTEGER ,
sl_expire VARCHAR(1) ,
sl_purchase INTEGER ,
sl_visitor_sign_up INTEGER ,
employee_number BIGINT  ,
shoe_gender VARCHAR(6) ,
source_campaign VARCHAR(75) ,
sl_sign_up INTEGER ,
test_group VARCHAR(7) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_adobe_cart_abandon;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_adobe_cart_abandon(
dt TIMESTAMP WITHOUT TIME ZONE,
visitor_id VARCHAR(41),
webs_customer_id VARCHAR(11),
products VARCHAR(13),
cart_additions INTEGER,
sas_brand_id INTEGER,
fs_sk INTEGER,
customer_id VARCHAR(11),
sas_product_id VARCHAR(20),
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.weather_longrange;
CREATE TABLE IF NOT EXISTS vfap_retail.weather_longrange(
dt TIMESTAMP WITHOUT TIME ZONE ,
mintemp DOUBLE PRECISION ,
maxtemp DOUBLE PRECISION ,
avgtemp DOUBLE PRECISION ,
prcp DOUBLE PRECISION ,
location VARCHAR(10) ,
fs_sk INTEGER ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_email_click_hist;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_click_hist(
id INTEGER ,
event_uuid VARCHAR(38) ,
event_type_id INTEGER ,
account_id INTEGER ,
list_id INTEGER ,
riid BIGINT ,
custom_properties VARCHAR(2) ,
campaign_id INTEGER ,
launch_id INTEGER ,
email_format VARCHAR(3) ,
offer_name VARCHAR(44) ,
offer_number INTEGER ,
offer_category VARCHAR(40) ,
offer_url VARCHAR(200) ,
event_captured_dt TIMESTAMP WITHOUT TIME ZONE ,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE ,
customer_no BIGINT ,
customer_id INTEGER ,
sas_brand_id INTEGER  DEFAULT 4,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.weather_hist_weekly;
CREATE TABLE IF NOT EXISTS vfap_retail.weather_hist_weekly(
dt TIMESTAMP WITHOUT TIME ZONE ,
location VARCHAR(40) ,
sas_brand_id INTEGER ,
mintemp_min DOUBLE PRECISION ,
mintemp_max DOUBLE PRECISION ,
mintemp_avg DOUBLE PRECISION ,
maxtemp_min DOUBLE PRECISION ,
maxtemp_max DOUBLE PRECISION ,
maxtemp_avg DOUBLE PRECISION ,
rngtemp_min DOUBLE PRECISION ,
rngtemp_max DOUBLE PRECISION ,
rngtemp_avg DOUBLE PRECISION ,
prcpin_min DOUBLE PRECISION ,
prcpin_max DOUBLE PRECISION ,
prcpin_avg DOUBLE PRECISION ,
snowin_min DOUBLE PRECISION ,
snowin_max DOUBLE PRECISION ,
snowin_avg DOUBLE PRECISION ,
wspdmph_min DOUBLE PRECISION ,
wspdmph_max DOUBLE PRECISION ,
wspdmph_avg DOUBLE PRECISION ,
gustmph_min DOUBLE PRECISION ,
gustmph_max DOUBLE PRECISION ,
gustmph_avg DOUBLE PRECISION ,
rhpct_min DOUBLE PRECISION ,
rhpct_max DOUBLE PRECISION ,
rhpct_avg DOUBLE PRECISION ,
skycpct_min DOUBLE PRECISION ,
skycpct_max DOUBLE PRECISION ,
skycpct_avg DOUBLE PRECISION ,
presmb_min DOUBLE PRECISION ,
presmb_max DOUBLE PRECISION ,
presmb_avg DOUBLE PRECISION ,
prcpin_ndays INTEGER ,
snowin_ndays INTEGER ,
n_dt INTEGER ,
mintemp_n INTEGER ,
maxtemp_n INTEGER ,
rngtemp_n INTEGER ,
prcpin_n INTEGER ,
snowin_n INTEGER ,
wspdmph_n INTEGER ,
gustmph_n INTEGER ,
rhpct_n INTEGER ,
skycpct_n INTEGER ,
presmb_n INTEGER ,
sas_process_dt TIMESTAMP WITHOUT TIME ZONE ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.webs_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.webs_xref(
sas_brand_id INTEGER ,
webs_customer_id VARCHAR(88) ,
customer_id INTEGER ,
process_dtm TIMESTAMP
)
DISTSTYLE EVEN;
COMMIT;

CREATE TABLE IF NOT EXISTS vfap_retail.experian_gold_stage_1(
customer_id INTEGER ,
customer_no BIGINT ,
first_name VARCHAR(20) ,
last_name VARCHAR(28) ,
i1mailresponderindiv VARCHAR(2) ,
multicodirectmailresphh VARCHAR(2) ,
multibuyermin551551 VARCHAR(2) ,
giftgadgetdmmercbyrcat INTEGER ,
colspcfooddmmercbyrcat INTEGER ,
booksdmmercbyrcat INTEGER ,
gardenfarmdmmercbyrcat INTEGER ,
craftshobbiedmmercbyrcat INTEGER ,
femaleorientdmmercbyrcat INTEGER ,
maleorientdmmercbyrcat INTEGER ,
upscaledmmercbyrcat INTEGER ,
generaldmmercbyrcat INTEGER ,
healthfitnessmagbyrcat INTEGER ,
culinaryintmagbuyer INTEGER ,
gardenfarmingmagbyrcat INTEGER ,
religiousmagbyrcat INTEGER ,
malesportorienmagbyrcat INTEGER ,
femaleorientmagbyrcat INTEGER ,
familygeneralmagbyrcat INTEGER ,
generalcontribtrcat INTEGER ,
healthinstcontribtrcat INTEGER ,
politicalcontribtrcat INTEGER ,
religiouscontribtrcat INTEGER ,
sweepstakes INTEGER ,
doityourselfers INTEGER ,
newsfinancial INTEGER ,
photography INTEGER ,
oddsendsmailrespondr INTEGER ,
miscmailresp INTEGER ,
politicalaffiliation VARCHAR(3) ,
firsttimehomebuy INTEGER ,
interestingolf VARCHAR(2) ,
contributestocharities VARCHAR(2) ,
petenthusiast VARCHAR(2) ,
interestinculturalarts VARCHAR(2) ,
purchasedthroughthemail VARCHAR(2) ,
interestinfitness VARCHAR(2) ,
interestintheoutdoors VARCHAR(2) ,
interestintravel VARCHAR(2) ,
investors VARCHAR(2) ,
presenceofautomobile VARCHAR(2) ,
presenceofcreditcard VARCHAR(2) ,
interestingardening VARCHAR(2) ,
interestincrafts VARCHAR(2) ,
collectors VARCHAR(2) ,
cruiseenthusiasts VARCHAR(2) ,
interestinsports VARCHAR(2) ,
interestingourmetcooking VARCHAR(2) ,
sweepstakesgambling VARCHAR(2) ,
interestinpolitics VARCHAR(2) ,
interestinmusic VARCHAR(2) ,
interestinreading VARCHAR(2) ,
childrenparentingproducts VARCHAR(2) ,
doityourselfer VARCHAR(2) ,
selfimprovement VARCHAR(2) ,
interestinreligion VARCHAR(2) ,
grandparent VARCHAR(2) ,
interestinclothing VARCHAR(2) ,
donatesenvironmentalcauses VARCHAR(2) ,
investsmutualfundsannuities VARCHAR(2) ,
weightconscious VARCHAR(2) ,
mailordermultibuyer VARCHAR(2) ,
presenceofpremiumcreditcard VARCHAR(2) ,
dogenthusiasts VARCHAR(2) ,
catenthusiasts VARCHAR(2) ,
healthyliving VARCHAR(2) ,
interestinautomotive VARCHAR(2) ,
interestinskiing VARCHAR(2) ,
interestinboating VARCHAR(2) ,
presenceofcellphone VARCHAR(2) ,
communicationconnectivity VARCHAR(2) ,
computersperipherals VARCHAR(2) ,
hitechowner VARCHAR(2) ,
homedecoratingfurnishing VARCHAR(2) ,
homeentertainmenttvvideo VARCHAR(2) ,
kitchenaidssmallappliances VARCHAR(2) ,
mailorderbuyermusicvideo VARCHAR(2) ,
mailorderbuybooksmagazines VARCHAR(2) ,
mailorderbuyclothingshoes VARCHAR(2) ,
mailorderbuyinsfinance VARCHAR(2) ,
mailorderbuyergifts VARCHAR(2) ,
mailorderbuyergardening VARCHAR(2) ,
mailorderbuyerjewelrycosm VARCHAR(2) ,
classicaloperabigbandmusic VARCHAR(2) ,
countrymusic VARCHAR(2) ,
christianmusic VARCHAR(2) ,
rockmusic VARCHAR(2) ,
internetonlinesubscriber VARCHAR(2) ,
interestinphotography VARCHAR(2) ,
purchaseviaonline VARCHAR(2) ,
interestinaffluentlifestyle VARCHAR(2) ,
domestictraveler VARCHAR(2) ,
foreigntraveler VARCHAR(2) ,
contactlensewearer VARCHAR(2) ,
prescriptiondruguser VARCHAR(2) ,
interestinvideosdvd VARCHAR(2) ,
activemilitary VARCHAR(2) ,
inactivemilitary VARCHAR(2) ,
morbankdedcategoryhitcnt INTEGER ,
morbanknondedcategoryhit INTEGER ,
politicalaffiliation2 VARCHAR(3) ,
politicalaffiliation3 VARCHAR(3) ,
politicalaffiliation4 VARCHAR(3) ,
politicalaffiliation5 VARCHAR(3) ,
politicalaffiliation6 VARCHAR(3) ,
politicalaffiliation7 VARCHAR(3) ,
politicalaffiliation8 VARCHAR(3) ,
throughthemail VARCHAR(2) ,
othercollectibles VARCHAR(2) ,
artantiques VARCHAR(2) ,
stampscoins VARCHAR(2) ,
dolls VARCHAR(2) ,
figurines VARCHAR(2) ,
sportsmemorabilia VARCHAR(2) ,
cooking VARCHAR(2) ,
baking VARCHAR(2) ,
cookingweightconscience VARCHAR(2) ,
wineappreciation VARCHAR(2) ,
cookinggourmet VARCHAR(2) ,
crafts VARCHAR(2) ,
knittingneedlework VARCHAR(2) ,
quilting VARCHAR(2) ,
sewing VARCHAR(2) ,
woodworking VARCHAR(2) ,
losingweight VARCHAR(2) ,
includevitaminsupplements VARCHAR(2) ,
healthandnaturalfoods VARCHAR(2) ,
photographyhobbies VARCHAR(2) ,
gardeninghobbies VARCHAR(2) ,
homeworkshopdiy VARCHAR(2) ,
carsandautorepair VARCHAR(2) ,
birdwatching VARCHAR(2) ,
self_improvement VARCHAR(2) ,
christiangospel VARCHAR(2) ,
randb VARCHAR(2) ,
jazznewage VARCHAR(2) ,
classical VARCHAR(2) ,
rocknroll VARCHAR(2) ,
country VARCHAR(2) ,
musicingeneral VARCHAR(2) ,
othermusic VARCHAR(2) ,
bestsellingfiction VARCHAR(2) ,
bibledevotionalreligious VARCHAR(2) ,
childrensreading VARCHAR(2) ,
computer VARCHAR(2) ,
cookingculinary VARCHAR(2) ,
countrylifestyle VARCHAR(2) ,
entertainmentpeople VARCHAR(2) ,
fashion VARCHAR(2) ,
history VARCHAR(2) ,
interiordecorating VARCHAR(2) ,
medicalhealth VARCHAR(2) ,
military VARCHAR(2) ,
mystery VARCHAR(2) ,
naturalhealthremedies VARCHAR(2) ,
romance VARCHAR(2) ,
sciencefiction VARCHAR(2) ,
sciencetechnology VARCHAR(2) ,
sportsreading VARCHAR(2) ,
worldnewspolitics VARCHAR(2) ,
bookreader VARCHAR(2) ,
animalwelfaresoclcauseconc VARCHAR(2) ,
environmentwildlife VARCHAR(2) ,
politicalconservative VARCHAR(2) ,
politicalliberal VARCHAR(2) ,
religioussocialcauseconc VARCHAR(2) ,
children VARCHAR(2) ,
veterans VARCHAR(2) ,
healthsocialcausesconcern VARCHAR(2) ,
othersocialcausesconcern VARCHAR(2) ,
baseball VARCHAR(2) ,
basketball VARCHAR(2) ,
hockey VARCHAR(2) ,
campinghiking VARCHAR(2) ,
hunting VARCHAR(2) ,
fishing VARCHAR(2) ,
nascar VARCHAR(2) ,
personalfitnessexercise VARCHAR(2) ,
scubadiving VARCHAR(2) ,
football VARCHAR(2) ,
golf VARCHAR(2) ,
snowskiingboarding VARCHAR(2) ,
boatingsailing VARCHAR(2) ,
walking VARCHAR(2) ,
cycling VARCHAR(2) ,
motorcycles VARCHAR(2) ,
runningjogging VARCHAR(2) ,
soccer VARCHAR(2) ,
swimming VARCHAR(2) ,
playsportsingeneral VARCHAR(2) ,
sweepstakes2 VARCHAR(2) ,
lotteries VARCHAR(2) ,
casinogambling VARCHAR(2) ,
cruise VARCHAR(2) ,
international VARCHAR(2) ,
domestic VARCHAR(2) ,
timeshare VARCHAR(2) ,
recreationalvehicle VARCHAR(2) ,
wouldenjoyrvtravel VARCHAR(2) ,
businesstravel VARCHAR(2) ,
personaltravel VARCHAR(2) ,
owncomputer VARCHAR(2) ,
useinternetservice VARCHAR(2) ,
useinternetsvcdslhighspd VARCHAR(2) ,
plantobuycomputer VARCHAR(2) ,
cellphone VARCHAR(2) ,
compactdiscplayer VARCHAR(2) ,
dvdplayer VARCHAR(2) ,
satellitedish VARCHAR(2) ,
digitalcamera VARCHAR(2) ,
hdtv VARCHAR(2) ,
interestinelectronics VARCHAR(2) ,
pesonaldataassistantpda VARCHAR(2) ,
videocamera VARCHAR(2) ,
videogamesystem VARCHAR(2) ,
interestincultrualarts VARCHAR(2) ,
businessandfinance VARCHAR(2) ,
childrensmagazines VARCHAR(2) ,
computerelectronics VARCHAR(2) ,
craftsgamesandhobbies VARCHAR(2) ,
fitness VARCHAR(2) ,
foodwinecooking VARCHAR(2) ,
gardeningmagazines VARCHAR(2) ,
huntingandfishing VARCHAR(2) ,
mens VARCHAR(2) ,
music VARCHAR(2) ,
parentingbabies VARCHAR(2) ,
sportsmagazines VARCHAR(2) ,
subscription VARCHAR(2) ,
travel VARCHAR(2) ,
womens VARCHAR(2) ,
currenteventsnews VARCHAR(2) ,
haveleasedavehicle VARCHAR(2) ,
havepurchasedavehicle VARCHAR(2) ,
bookclub VARCHAR(2) ,
musicclub VARCHAR(2) ,
americanexpresspremium VARCHAR(2) ,
americanexpressregular VARCHAR(2) ,
discoverpremium VARCHAR(2) ,
discoverregular VARCHAR(2) ,
othercardpremium VARCHAR(2) ,
othercardregular VARCHAR(2) ,
storeorretailregular VARCHAR(2) ,
visaregular VARCHAR(2) ,
mastercardregular VARCHAR(2) ,
humanitarian VARCHAR(2) ,
buyprerecordedvideos VARCHAR(2) ,
watchcabletv VARCHAR(2) ,
watchvideos VARCHAR(2) ,
cdsmoneymarketcurrently VARCHAR(2) ,
irascurrently VARCHAR(2) ,
irasfutureinterest VARCHAR(2) ,
lifeinsurancecurrently VARCHAR(2) ,
mutualfundscurrently VARCHAR(2) ,
mutualfundsfutureinterest VARCHAR(2) ,
otherinvestmentscurrently VARCHAR(2) ,
otherinvestmtfutureinteres VARCHAR(2) ,
realestatecurrently VARCHAR(2) ,
realestatefutureinterest VARCHAR(2) ,
stocksbondscurrently VARCHAR(2) ,
stocksbondsfutureinterest VARCHAR(2) ,
proudgrandparent VARCHAR(2) ,
purchaseitemsviatheinterne VARCHAR(2) ,
activemilitarymember VARCHAR(2) ,
vetern VARCHAR(2) ,
ownacat VARCHAR(2) ,
ownadog VARCHAR(2) ,
ownapet VARCHAR(2) ,
byinternet VARCHAR(2) ,
bymail VARCHAR(2) ,
bycatalog VARCHAR(2) ,
apparel VARCHAR(2) ,
computerorelectronics VARCHAR(2) ,
booksandmusic VARCHAR(2) ,
homeandgarden VARCHAR(2) ,
sportsrelated VARCHAR(2) ,
bigtall VARCHAR(2) ,
mailorderbooksmag VARCHAR(2) ,
mailorderclothing VARCHAR(2) ,
mailordergardening VARCHAR(2) ,
mailordergift VARCHAR(2) ,
mailorderjewelrycosmetics VARCHAR(2) ,
mailordermusicvideos VARCHAR(2) ,
mailorderchildrensproducts VARCHAR(2) ,
actintamusementparkvisit INTEGER ,
actintzoovisit INTEGER ,
actintcoffeeconnoisseurs INTEGER ,
actintwinelovers INTEGER ,
actintdoityourselfers INTEGER ,
actinhomeimprov INTEGER ,
actinthuntingenthusiasts INTEGER ,
buyerlaptopowners INTEGER ,
buyersecuritysysowners INTEGER ,
buyertabletowners INTEGER ,
buyercouponusers INTEGER ,
buyerluxurystoreshop INTEGER ,
buyeryoungadultclothshop INTEGER ,
buyersupercentershop INTEGER ,
buywarehclubmember INTEGER ,
membershipsaarpmembers INTEGER ,
lifestylelifeinspolicy INTEGER ,
lifestylemedicalpolicy INTEGER ,
lifestylemedicarepolicy INTEGER ,
actindigimagnewspaper INTEGER ,
actinattendeduprog INTEGER ,
actintvideogamer INTEGER ,
actintmlbenthusiast INTEGER ,
actintnascarenthusiast INTEGER ,
actintnbaenthusiast INTEGER ,
actintnflenthusiast INTEGER ,
actintnhlenthusiast INTEGER ,
actintpgatourenthusiast INTEGER ,
actintpoliticaltvliberal INTEGER ,
actinpolitvlibcom INTEGER ,
actinpolitvcons INTEGER ,
actineatsfamrestrnt INTEGER ,
actinteatsfastfood INTEGER ,
actintcanoeingkayaking INTEGER ,
actintplaygolf INTEGER ,
buyautomobile INTEGER ,
buyerloyaltycarduser INTEGER ,
buyerluxhomegoodshop INTEGER ,
membershipsunionmember INTEGER ,
investhaveretirementplan INTEGER ,
investonlinetrading INTEGER ,
havegrandchild INTEGER ,
buyernondeptstoremakeup INTEGER ,
actintgourmetcooking INTEGER ,
actintcatowners INTEGER ,
actintdogowners INTEGER ,
actintartsandcrafts INTEGER ,
actintscrapbooking INTEGER ,
actintculturalarts INTEGER ,
hobbiesgardening INTEGER ,
actintphotography INTEGER ,
actintbookreader INTEGER ,
actintebookreader INTEGER ,
actintaudiobooklistener INTEGER ,
actintpetenthusiast INTEGER ,
actintmusicdownload INTEGER ,
actintmusicstreaming INTEGER ,
actintavidrunners INTEGER ,
actintoutdoorenthusiast INTEGER ,
actintfishing INTEGER ,
actintsnowsports INTEGER ,
actintboating INTEGER ,
actintplayshockey INTEGER ,
actintplayssoccer INTEGER ,
actintplaystennis INTEGER ,
actintsportsenthusiast INTEGER ,
actinthealthyliving INTEGER ,
actintfitnessenthusiast INTEGER ,
actintonadiet INTEGER ,
actintweightconscious INTEGER ,
buyhighendspiritdrink INTEGER ,
actintcasinogambling INTEGER ,
actintsweepstakeslottery INTEGER ,
stylehighfreqbusintrvl INTEGER ,
stylehighfreqcruise INTEGER ,
stylehighfreqdomesticvac INTEGER ,
stylehighfreqforeignvac INTEGER ,
stylefrequentflyerprgmbr INTEGER ,
stylehotelloyaltyprg INTEGER ,
donorcontribtocharities INTEGER ,
donorcontribarts INTEGER ,
donorcontribtoeducation INTEGER ,
donorcontribtohealth INTEGER ,
donorcontribtopolitical INTEGER ,
donorcontribtoprvtfound INTEGER ,
donorcontribvolunt INTEGER ,
findebitcarduser INTEGER ,
fincorpcreditcardusr INTEGER ,
finmajorcreditcarduser INTEGER ,
finpremiumcreditcarduser INTEGER ,
fincreditcarduser INTEGER ,
finstorecreditcarduser INTEGER ,
invbrokerageaccountowner INTEGER ,
invactiveinvor INTEGER ,
invmutualfundinvor INTEGER ,
buyerdeptstoremakeupuser INTEGER ,
actintchristianmusic INTEGER ,
actintclassicalmusic INTEGER ,
actintcountrymusic INTEGER ,
actintmusic INTEGER ,
actintoldiesmusic INTEGER ,
actintrockmusic INTEGER ,
actint80smusic INTEGER ,
actinthiphopmusic INTEGER ,
actintalternativemusic INTEGER ,
actintjazzmusic INTEGER ,
actintpopmusic INTEGER ,
lifestyleintinreligion INTEGER ,
lifestylemilitaryactive INTEGER ,
lifestylemilitaryinactive INTEGER ,
workingcouples INTEGER ,
poc03yrsoldgenderv3 VARCHAR(2) ,
poc46yrsoldgenderv3 VARCHAR(2) ,
poc79yrsoldgenderv3 VARCHAR(2) ,
poc1012yrsoldgenderv3 VARCHAR(2) ,
poc1315yrsoldgenderv3 VARCHAR(2) ,
poc1618yrsoldgenderv3 VARCHAR(2) ,
discretionaryspendestlevel VARCHAR(2) ,
discretspendestscore INTEGER ,
actinteatsfamilyrestaurant INTEGER ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cust_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.cust_xref(
sas_brand_id INTEGER ,
old_customer_no BIGINT ,
new_customer_no BIGINT ,
last_update_date TIMESTAMP WITHOUT TIME ZONE ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.loyalty_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.loyalty_xref(
customer_id INTEGER ,
loya_id VARCHAR(61) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_wcs_abandoned_cart;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_wcs_abandoned_cart(
customer_id INTEGER ,
sas_product_id VARCHAR(17) ,
email VARCHAR(102) ,
versioncode VARCHAR(5) ,
firstname VARCHAR(51) ,
lastname VARCHAR(51) ,
product_id VARCHAR(51) ,
cartlisturl VARCHAR(204) ,
giftbox INTEGER ,
cart_itm_dt TIMESTAMP WITHOUT TIME ZONE ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cm_conversion;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_conversion(
session_id VARCHAR(20) ,
cookie_id VARCHAR(24) ,
ts TIMESTAMP WITHOUT TIME ZONE ,
event_name VARCHAR(256) ,
event_category VARCHAR(256) ,
event_action_type INTEGER ,
event_points INTEGER ,
site_id VARCHAR(100) ,
conversion_event_attribute_1 VARCHAR(256) ,
conversion_event_attribute_2 VARCHAR(256) ,
conversion_event_attribute_3 VARCHAR(256) ,
conversion_event_attribute_4 VARCHAR(256) ,
conversion_event_attribute_5 VARCHAR(256) ,
conversion_event_attribute_6 VARCHAR(256) ,
conversion_event_attribute_7 VARCHAR(256) ,
conversion_event_attribute_8 VARCHAR(256) ,
conversion_event_attribute_9 VARCHAR(256) ,
conversion_event_attribute_10 VARCHAR(256) ,
conversion_event_attribute_11 VARCHAR(256) ,
conversion_event_attribute_12 VARCHAR(256) ,
conversion_event_attribute_13 VARCHAR(256) ,
conversion_event_attribute_14 VARCHAR(256) ,
conversion_event_attribute_15 VARCHAR(256) ,
conversion_event_attribute_16 VARCHAR(256) ,
conversion_event_attribute_17 VARCHAR(256) ,
conversion_event_attribute_18 VARCHAR(256) ,
conversion_event_attribute_19 VARCHAR(256) ,
conversion_event_attribute_20 VARCHAR(256) ,
conversion_event_attribute_21 VARCHAR(256) ,
conversion_event_attribute_22 VARCHAR(256) ,
conversion_event_attribute_23 VARCHAR(256) ,
conversion_event_attribute_24 VARCHAR(256) ,
conversion_event_attribute_25 VARCHAR(256) ,
conversion_event_attribute_26 VARCHAR(256) ,
conversion_event_attribute_27 VARCHAR(256) ,
conversion_event_attribute_28 VARCHAR(256) ,
conversion_event_attribute_29 VARCHAR(256) ,
conversion_event_attribute_30 VARCHAR(256) ,
conversion_event_attribute_31 VARCHAR(256) ,
conversion_event_attribute_32 VARCHAR(256) ,
conversion_event_attribute_33 VARCHAR(256) ,
conversion_event_attribute_34 VARCHAR(256) ,
conversion_event_attribute_35 VARCHAR(256) ,
conversion_event_attribute_36 VARCHAR(256) ,
conversion_event_attribute_37 VARCHAR(256) ,
conversion_event_attribute_38 VARCHAR(256) ,
conversion_event_attribute_39 VARCHAR(256) ,
conversion_event_attribute_40 VARCHAR(256) ,
conversion_event_attribute_41 VARCHAR(256) ,
conversion_event_attribute_42 VARCHAR(256) ,
conversion_event_attribute_43 VARCHAR(256) ,
conversion_event_attribute_44 VARCHAR(256) ,
conversion_event_attribute_45 VARCHAR(256) ,
conversion_event_attribute_46 VARCHAR(256) ,
conversion_event_attribute_47 VARCHAR(256) ,
conversion_event_attribute_48 VARCHAR(256) ,
conversion_event_attribute_49 VARCHAR(256) ,
conversion_event_attribute_50 VARCHAR(256) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_wcs_wishlist;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_wcs_wishlist(
customer_id INTEGER ,
sas_product_id VARCHAR(17) ,
email VARCHAR(102) ,
versioncode VARCHAR(5) ,
firstname VARCHAR(100) , -- changed to 100
lastname VARCHAR(100) , -- changed to 100
product_id VARCHAR(51) , -- changed to 100
cartlisturl VARCHAR(204) ,
giftbox INTEGER ,
cart_itm_dt TIMESTAMP WITHOUT TIME ZONE ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_wcs_abandoned_wish_cart;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_wcs_abandoned_wish_cart(
customer_id INTEGER ,
sas_product_id VARCHAR(17) ,
email VARCHAR(102) ,
versioncode VARCHAR(5) ,
firstname VARCHAR(100) , 
lastname VARCHAR(100) , 
product_id VARCHAR(100) ,
cartlisturl VARCHAR(204) ,
giftbox INTEGER ,
cart_itm_dt TIMESTAMP WITHOUT TIME ZONE ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.productxref;
CREATE TABLE IF NOT EXISTS vfap_retail.productxref(
sas_brand_id INTEGER ,
product_code BIGINT ,
style_id INTEGER ,
color_code INTEGER ,
primary_size_label VARCHAR(5) ,
secondary_size_label VARCHAR(3) ,
primary_size_code INTEGER ,
secondary_size_code INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_adobe_prodview_xref_final;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_adobe_prodview_xref_final(
visitor_id VARCHAR(41) ,
customer_id VARCHAR(11) ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_adobe_cart_xref_final;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_adobe_cart_xref_final(
visitor_id VARCHAR(41) ,
customer_id VARCHAR(11) ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;
COMMIT;

DROP table vfap_retail.vans_custom_shoes;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_custom_shoes(
orders_id VARCHAR(10) ,
orderitems_id VARCHAR(10) ,
timeplaced TIMESTAMP WITHOUT TIME ZONE ,
totalproduct DOUBLE PRECISION ,
shipcharge DOUBLE PRECISION ,
totaladjustment DOUBLE PRECISION ,
name VARCHAR(130) ,
shipping_firstname VARCHAR(39) ,
shipping_lastname VARCHAR(34) ,
shipping_address1 VARCHAR(71) ,
shipping_address2 VARCHAR(37) ,
shipping_city VARCHAR(38) ,
shipping_state VARCHAR(4) ,
shipping_country VARCHAR(4) ,
shipping_zipcode VARCHAR(13) ,
email_address VARCHAR(49) ,
billing_lastname VARCHAR(43) ,
billing_firstname VARCHAR(25) ,
billing_address1 VARCHAR(45) ,
billing_address2 VARCHAR(37) ,
billing_city VARCHAR(37) ,
billing_state VARCHAR(17) ,
billing_country VARCHAR(4) ,
billing_zipcode VARCHAR(13) ,
payment_method VARCHAR(19) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.experian_bronze;
CREATE TABLE IF NOT EXISTS vfap_retail.experian_bronze(
customer_id INTEGER ,
customer_no BIGINT ,
first_name VARCHAR(20) ,
last_name VARCHAR(26) ,
loyalty VARCHAR(5) ,
cbsa_code INTEGER ,
cbsa_name VARCHAR(56) ,
dma_code INTEGER ,
dma_name VARCHAR(50) ,
nearestoutletstorenumber INTEGER ,
nearestoutletstorename VARCHAR(42) ,
nearestoutletdistancemiles DOUBLE PRECISION ,
address_1 VARCHAR(40) ,
address_2 VARCHAR(11) ,
city VARCHAR(26) ,
state VARCHAR(2) ,
zip VARCHAR(10) ,
nearestretailstorenumber INTEGER ,
nearestretailstorename VARCHAR(41) ,
nearestretaildistancemiles DOUBLE PRECISION ,
cass_addresstypecode VARCHAR(2) ,
cass_addresstypestring VARCHAR(11) ,
cass_results VARCHAR(29) ,
email_address VARCHAR(43) ,
opt_in_flag INTEGER ,
mail_opt_in_flag INTEGER ,
personidnumber BIGINT ,
persontype VARCHAR(1) ,
fullname VARCHAR(30) ,
middleinitialv2 VARCHAR(2) ,
surname VARCHAR(20) ,
namesuffix VARCHAR(4) ,
nameprefix VARCHAR(5) ,
gender VARCHAR(1) ,
combinedage VARCHAR(3) ,
educationmodel INTEGER ,
maritalstatus VARCHAR(2) ,
occupationgroupv2 VARCHAR(2) ,
householdpersonnumber INTEGER ,
firstnamelastname VARCHAR(30) ,
addressid VARCHAR(12) ,
fipsstatecode INTEGER ,
stateabbreviation VARCHAR(2) ,
fipszipcode VARCHAR(5) ,
zip4 VARCHAR(4) ,
deliverypointcode INTEGER ,
carrierroute VARCHAR(4) ,
shortcityname VARCHAR(13) ,
cityname VARCHAR(25) ,
housenumber VARCHAR(10) ,
predirection VARCHAR(2) ,
streetname VARCHAR(28) ,
streetsuffix VARCHAR(4) ,
postdirection VARCHAR(2) ,
unitdesignator VARCHAR(6) ,
unitdesignatornumber VARCHAR(7) ,
primaryaddress VARCHAR(30) ,
secondaryaddress VARCHAR(13) ,
fipscountycode INTEGER ,
countyname VARCHAR(21) ,
cvmm_lat DOUBLE PRECISION ,
cvmm_lon DOUBLE PRECISION ,
matchlevelforgeodata VARCHAR(2) ,
timezone INTEGER ,
livingunitid VARCHAR(10) ,
phone_specialusagephone VARCHAR(10) ,
phone_number2 VARCHAR(10) ,
dwellingunitsize VARCHAR(1) ,
dwellingtype VARCHAR(2) ,
hmownercombinedhmownerrenter VARCHAR(1) ,
esthouseholdincomev5 VARCHAR(1) ,
ncoamoveupdatecode INTEGER ,
ncoamoveupdatedate VARCHAR(8) ,
mailresponder VARCHAR(1) ,
lengthofresidence INTEGER ,
numberofpersonsinlivingunit INTEGER ,
numberofadultsinlivingunit INTEGER ,
ruralurbancountysizecode INTEGER ,
activitydate VARCHAR(8) ,
morbankupscalemerchandisebuyer INTEGER ,
morbankmalemerchandisebuyer INTEGER ,
morbankfemalemerchandisebuyer INTEGER ,
morbkcraftshobbymerch_isebuyer INTEGER ,
morbankgardeningfarmingbuyer INTEGER ,
morbankbookbuyer INTEGER ,
morbkcollectspecialfoodsbuyer INTEGER ,
morbankgiftsandgadgetsbuyer INTEGER ,
morbankgeneralmerchandisebuyer INTEGER ,
morbkfamily_generalmagazine INTEGER ,
morbankfemaleorientedmagazine INTEGER ,
morbankmalesportsmagazine INTEGER ,
morbankreligiousmagazine INTEGER ,
morbkgardeningfarmingmagazine INTEGER ,
morbkculinaryinterestsmagazine INTEGER ,
morbkhealth_fitnessmagazine INTEGER ,
morbankdoityourselfers INTEGER ,
morbanknewsandfinancial INTEGER ,
morbankphotography INTEGER ,
morbankopportunityseekersandce INTEGER ,
morbankreligiouscontributor INTEGER ,
morbankpoliticalcontributor INTEGER ,
morbkhealth_inscontributor INTEGER ,
morbankgeneralcontributor INTEGER ,
morbankmiscellaneous INTEGER ,
morbankoddsandends INTEGER ,
morbankdedupedcategoryhitcount INTEGER ,
morbknondedupedcategoryhitcnt INTEGER ,
mortgagehmpurhmpurprice INTEGER ,
mortgagehmpurhmpurdt VARCHAR(8) ,
propertyrealtyhomeyearbuilt INTEGER ,
householdcomposition VARCHAR(1) ,
corebasedstatisticalareascbsa VARCHAR(5) ,
corebasedstatisticalareatype VARCHAR(1) ,
childrenage018version3 VARCHAR(2) ,
phone_activitydate VARCHAR(8) ,
census2010tractandblockgroup VARCHAR(7) ,
capeagepopmedianage INTEGER ,
capeagepop017 INTEGER ,
capeagepop1899 INTEGER ,
capeagepop6599 INTEGER ,
capebuilthumedianhsngunitage INTEGER ,
capechildhhwithpersonslt18 INTEGER ,
capechildhhmcfamwprsnslt18 INTEGER ,
capechildhhmcfamwoprsnslt18 INTEGER ,
capednstyprsnsperhhforpopinhh INTEGER ,
capeeducispsa INTEGER ,
capeeducispsadecile INTEGER ,
capeeducpop25medianeduattained INTEGER ,
capeethnicpopwhiteonly INTEGER ,
capeethnicpopblackonly INTEGER ,
capeethnicpopasianonly INTEGER ,
capeethnicpophispanic INTEGER ,
capehomvaloohumedianhomevalue INTEGER ,
capehhsizehhaveragehldsize INTEGER ,
capehustrhumobilehome INTEGER ,
capeincfamilyincstatedecile INTEGER ,
capeinchhmedianfamilyhldincome INTEGER ,
capelanghhspanishspeaking INTEGER ,
capetenancyocchuowneroccupied INTEGER ,
capetenancyocchurenteroccupied INTEGER ,
capetyphhmarriedcouplefamily INTEGER ,
mosaichousehold VARCHAR(3) ,
cvmacromatchgroup VARCHAR(16) ,
cvmacromatchlevel VARCHAR(1) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_email_convert;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_convert(
customer_no BIGINT ,
event_type_id INTEGER ,
account_id INTEGER ,
list_id INTEGER ,
riid BIGINT ,
customer_id BIGINT ,
event_captured_dt TIMESTAMP WITHOUT TIME ZONE ,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE ,
campaign_id INTEGER ,
launch_id INTEGER ,
source VARCHAR(7) ,
email_format VARCHAR(1) ,
offer_name VARCHAR(24) ,
offer_number INTEGER ,
offer_category VARCHAR(29) ,
offer_url VARCHAR(210) ,
order_id INTEGER ,
order_total DOUBLE PRECISION ,
order_quantity VARCHAR(1) ,
store_id INTEGER ,
user_agent_string VARCHAR(100) ,
operating_system VARCHAR(3) ,
browser VARCHAR(2) ,
browser_type VARCHAR(2) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.weather_forecast;
CREATE TABLE IF NOT EXISTS vfap_retail.weather_forecast(
location VARCHAR(10) ,
municipality VARCHAR(30) ,
state VARCHAR(2) ,
county VARCHAR(60) ,
dt TIMESTAMP WITHOUT TIME ZONE ,
mintempdegf DOUBLE PRECISION ,
maxtempdegf DOUBLE PRECISION ,
prcpin DOUBLE PRECISION ,
snowin DOUBLE PRECISION ,
wspdmph DOUBLE PRECISION ,
wdirdeg INTEGER ,
gustmph DOUBLE PRECISION ,
rhpct INTEGER ,
skycpct INTEGER ,
presmb DOUBLE PRECISION ,
poppct INTEGER ,
uvindex INTEGER ,
wxicon INTEGER ,
mintemplydegf DOUBLE PRECISION ,
maxtemplydegf DOUBLE PRECISION ,
mintempnormdegf DOUBLE PRECISION ,
maxtempnormdegf DOUBLE PRECISION ,
fs_sk INTEGER ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.dq_check;
CREATE TABLE IF NOT EXISTS vfap_retail.dq_check(
program_name VARCHAR(40) ,
input_tbl VARCHAR(60) ,
date_time TIMESTAMP WITHOUT TIME ZONE ,
dq_severity_level INTEGER ,
dq_description VARCHAR(60) ,
counts INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_wcs_returns_us;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_wcs_returns_us(
customer_id INTEGER ,
sas_product_id VARCHAR(17) ,
productid VARCHAR(51) ,
orderid INTEGER ,
userid BIGINT ,
price DOUBLE PRECISION ,
currency VARCHAR(6) ,
dt TIMESTAMP WITHOUT TIME ZONE ,
rmaid VARCHAR(100) ,  		--check with Harsh/Phani
returnreason VARCHAR(340) ,
firstname VARCHAR(100) ,
lastname VARCHAR(100) ,
emailid VARCHAR(102) ,
dim1 VARCHAR(17) ,
dim2 VARCHAR(17) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_wcs_returns_us;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_wcs_returns_us(
customer_id INTEGER ,
sas_product_id VARCHAR(17) ,
productid VARCHAR(51) ,
orderid INTEGER ,
userid INTEGER ,
price INTEGER ,
currency VARCHAR(6) ,
dt TIMESTAMP WITHOUT TIME ZONE ,
rmaid varchar(100) ,			--check with Harsh/Phani
returnreason VARCHAR(340) ,
firstname VARCHAR(100) ,
lastname VARCHAR(100) ,
emailid VARCHAR(102) ,
dim1 VARCHAR(17) ,
dim2 VARCHAR(17) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_email_launch;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_email_launch(  -- not done table is empty
account_id INTEGER ,
list_id INTEGER ,
event_captured_dt TIMESTAMP WITHOUT TIME ZONE ,
event_stored_dt TIMESTAMP WITHOUT TIME ZONE ,
campaign_id INTEGER ,
launch_id INTEGER ,
external_campaign_id VARCHAR(1) ,
sf_campaign_id VARCHAR(1) ,
campaign_name VARCHAR(38) ,
launch_name VARCHAR(1) ,
launch_status VARCHAR(1) ,
launch_type VARCHAR(1) ,
launch_charset VARCHAR(10) ,
purpose VARCHAR(1) ,
subject VARCHAR(255) ,
description VARCHAR(1) ,
product_category VARCHAR(1) ,
product_type VARCHAR(1) ,
marketing_strategy VARCHAR(13) ,
marketing_program VARCHAR(30) ,
launch_error_code VARCHAR(1) ,
launch_started_dt TIMESTAMP WITHOUT TIME ZONE ,
launch_completed_dt TIMESTAMP WITHOUT TIME ZONE ,
store_id VARCHAR(1) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cm_mmcclick;
CREATE TABLE IF NOT EXISTS vfap_retail.cm_mmcclick(
session_id VARCHAR(20) ,
cookie_id VARCHAR(24) ,
ts TIMESTAMP WITHOUT TIME ZONE ,
marketing_vendor VARCHAR(256) ,
marketing_category VARCHAR(256) ,
marketing_placement VARCHAR(256) ,
marketing_item VARCHAR(256) ,
site_id VARCHAR(100) ,
mmc_click_attribute_1 VARCHAR(256) ,
mmc_click_attribute_2 VARCHAR(256) ,
mmc_click_attribute_3 VARCHAR(256) ,
mmc_click_attribute_4 VARCHAR(256) ,
mmc_click_attribute_5 VARCHAR(256) ,
mmc_click_attribute_6 VARCHAR(256) ,
mmc_click_attribute_7 VARCHAR(256) ,
mmc_click_attribute_8 VARCHAR(256) ,
mmc_click_attribute_9 VARCHAR(256) ,
mmc_click_attribute_10 VARCHAR(256) ,
mmc_click_attribute_11 VARCHAR(256) ,
mmc_click_attribute_12 VARCHAR(256) ,
mmc_click_attribute_13 VARCHAR(256) ,
mmc_click_attribute_14 VARCHAR(256) ,
mmc_click_attribute_15 VARCHAR(256) ,
mmc_click_attribute_16 VARCHAR(256) ,
mmc_click_attribute_17 VARCHAR(256) ,
mmc_click_attribute_18 VARCHAR(256) ,
mmc_click_attribute_19 VARCHAR(256) ,
mmc_click_attribute_20 VARCHAR(256) ,
mmc_click_attribute_21 VARCHAR(256) ,
mmc_click_attribute_22 VARCHAR(256) ,
mmc_click_attribute_23 VARCHAR(256) ,
mmc_click_attribute_24 VARCHAR(256) ,
mmc_click_attribute_25 VARCHAR(256) ,
mmc_click_attribute_26 VARCHAR(256) ,
mmc_click_attribute_27 VARCHAR(256) ,
mmc_click_attribute_28 VARCHAR(256) ,
mmc_click_attribute_29 VARCHAR(256) ,
mmc_click_attribute_30 VARCHAR(256) ,
mmc_click_attribute_31 VARCHAR(256) ,
mmc_click_attribute_32 VARCHAR(256) ,
mmc_click_attribute_33 VARCHAR(256) ,
mmc_click_attribute_34 VARCHAR(256) ,
mmc_click_attribute_35 VARCHAR(256) ,
mmc_click_attribute_36 VARCHAR(256) ,
mmc_click_attribute_37 VARCHAR(256) ,
mmc_click_attribute_38 VARCHAR(256) ,
mmc_click_attribute_39 VARCHAR(256) ,
mmc_click_attribute_40 VARCHAR(256) ,
mmc_click_attribute_41 VARCHAR(256) ,
mmc_click_attribute_42 VARCHAR(256) ,
mmc_click_attribute_43 VARCHAR(256) ,
mmc_click_attribute_44 VARCHAR(256) ,
mmc_click_attribute_45 VARCHAR(256) ,
mmc_click_attribute_46 VARCHAR(256) ,
mmc_click_attribute_47 VARCHAR(256) ,
mmc_click_attribute_48 VARCHAR(256) ,
mmc_click_attribute_49 VARCHAR(256) ,
mmc_click_attribute_50 VARCHAR(256) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_progressive_upc;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_progressive_upc(
ip_sku VARCHAR(120) ,
ip_upc BIGINT ,
ip_vndr_styl VARCHAR(200) ,
ip_div INTEGER ,
ip_div_desc VARCHAR(120) ,
ip_dept INTEGER ,
ip_dept_desc VARCHAR(200) ,
ip_cls INTEGER ,
ip_cls_desc VARCHAR(200) ,
ip_itm_desc VARCHAR(280) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;


CREATE TABLE IF NOT EXISTS vfap_retail.etl_file_received( 
origination VARCHAR(3) ,
received_folder_name VARCHAR(128) ,
file_name VARCHAR(128) ,
file_size BIGINT ,
file_ts TIMESTAMP WITHOUT TIME ZONE ,
event VARCHAR(32) ,
chng_ts TIMESTAMP WITHOUT TIME ZONE ,
process_dtm TIMESTAMP default sysdate
)
DISTSTYLE EVEN;


DROP table vfap_retail.style;
CREATE TABLE IF NOT EXISTS vfap_retail.style(
style_id INTEGER ,
vendor_code INTEGER ,
class_code INTEGER ,
subclass_code INTEGER ,
vendor_style VARCHAR(8) ,
style_aka VARCHAR(20) ,
style_description VARCHAR(29) ,
retail DOUBLE PRECISION ,
cost DOUBLE PRECISION ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;


CREATE TABLE IF NOT EXISTS vfap_retail.first_name_gender_prob(
name VARCHAR(15) ,
prob_gender VARCHAR(1),
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;


DROP table vfap_retail.tnf_style_xref_clean;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_style_xref_clean
(
	department_code INTEGER
	,department_description VARCHAR(25)
	,class_code INTEGER
	,class_description VARCHAR(25)
	,style_id INTEGER
	,vendor_style VARCHAR(6)
	,style_description VARCHAR(25)
	,vendor_cd_4 VARCHAR(8)
	,vendor_cd_6 VARCHAR(6)
	,cnodeid INTEGER
	,style_description_short VARCHAR(25)
	,style_description_long VARCHAR(40)
	,product_line VARCHAR(39)
	,sbu VARCHAR(22)
	,series VARCHAR(13)
	,newconsumersegmentterritory VARCHAR(18)
	,newendusesport VARCHAR(23)
	,newproductcollectionfamily VARCHAR(18)
	,tnf___clm_description_for_sas VARCHAR(41)
	,tnf_sas_product_category VARCHAR(50)
	,product_gender VARCHAR(1)
	,product_age_group VARCHAR(5)
	,sas_style_description VARCHAR(100)
	,trgt_ma_flag INTEGER
	,trgt_ms_flag INTEGER
	,trgt_ml_flag INTEGER
	,trgt_ue_flag INTEGER
	,gender_age VARCHAR(7)
	,tb_flag INTEGER
	,sm_flag INTEGER
	,fn_flag INTEGER
	,ss_flag INTEGER
	,rw_flag INTEGER
	,fw_flag INTEGER
	,bcpck_flag INTEGER
	,process_dtm TIMESTAMP
)
DISTSTYLE EVEN
;

DROP table vfap_retail.style_xref_clean;
CREATE TABLE IF NOT EXISTS vfap_retail.style_xref_clean(
department_code INTEGER ,
department_description VARCHAR(25) ,
class_code INTEGER ,
class_description VARCHAR(25) ,
style_id INTEGER ,
vendor_style VARCHAR(6) ,
style_description VARCHAR(25) ,
vendor_cd_4 VARCHAR(8) ,
vendor_cd_6 VARCHAR(6) ,
cnodeid INTEGER ,
short_name VARCHAR(8) ,
long_name VARCHAR(8) ,
style_description_short VARCHAR(25) ,
style_description_long VARCHAR(40) ,
product_line VARCHAR(39) ,
sbu VARCHAR(22) ,
series VARCHAR(13) ,
newconsumersegmentterritory VARCHAR(18) ,
newendusesport VARCHAR(23) ,
newproductcollectionfamily VARCHAR(18) ,
tnf___clm_description_for_sas VARCHAR(41) ,
tnf_sas_product_category VARCHAR(50) ,
product_gender VARCHAR(1) ,
product_age_group VARCHAR(5) ,
sas_style_description VARCHAR(100) ,
gender_age VARCHAR(7) ,
tb_flag INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.tnf_style_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.tnf_style_xref(
department_code INTEGER ,
department_description VARCHAR(25) ,
class_code INTEGER ,
class_description VARCHAR(25) ,
style_id INTEGER ,
vendor_style VARCHAR(6) ,
style_description VARCHAR(25) ,
vendor_cd_4 VARCHAR(8) ,
vendor_cd_6 VARCHAR(6) ,
cnodeid BIGINT ,
short_name VARCHAR(8) ,
long_name VARCHAR(8) ,
style_description_short VARCHAR(25) ,
style_description_long VARCHAR(40) ,
product_line VARCHAR(39) ,
sbu VARCHAR(22) ,
series VARCHAR(13) ,
newconsumersegmentterritory VARCHAR(18) ,
newendusesport VARCHAR(23) ,
newproductcollectionfamily VARCHAR(18) ,
tnf___clm_description_for_sas VARCHAR(41) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_style_xref_clean;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_style_xref_clean(
department_code INTEGER ,
class_code INTEGER ,
class_description VARCHAR(27) ,
sas_brand_id INTEGER ,
style_id INTEGER ,
vendor_code INTEGER ,
subclass_code INTEGER ,
vendor_style VARCHAR(8) ,
style_aka VARCHAR(20) ,
style_description VARCHAR(29) ,
retail DOUBLE PRECISION ,
cost DOUBLE PRECISION ,
vans_prodcat VARCHAR(30) ,
mte_ind INTEGER ,
vans_sas_product_category VARCHAR(30) ,
product_family VARCHAR(30) ,
product_type VARCHAR(30) ,
product_gender VARCHAR(30) ,
product_age_group VARCHAR(30) ,
sas_style_description VARCHAR(30) ,
gen_age VARCHAR(200) ,
family_type VARCHAR(61) ,
skate_ind INTEGER ,
surf_ind INTEGER ,
snwb_ind INTEGER ,
nonsegment_ind INTEGER ,
trgt_peanuts_flag INTEGER ,
trgt_peanuts_like_flag INTEGER ,
peanuts_ind INTEGER ,
peanuts_like_ind INTEGER ,
trgt_ultrngls_flag INTEGER ,
trgt_ultrngpro_flag INTEGER ,
trgt_bts_flag INTEGER ,
trgt_new_ultrarange_flag INTEGER ,
process_dtm TIMESTAMP,
fs_sk INTEGER
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_zeta_preference;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_zeta_preference(
email VARCHAR(35) ,
int_skatebrd_flag VARCHAR(1) ,
int_surf_flag VARCHAR(1) ,
int_snowbrd_flag VARCHAR(1) ,
int_bmx_flag VARCHAR(1) ,
int_music_flag VARCHAR(1) ,
int_art_flag VARCHAR(1) ,
int_fashion_flag VARCHAR(1) ,
shppref_vansstore VARCHAR(1) ,
shppref_vansoutlet VARCHAR(1) ,
shppref_otherstore VARCHAR(1) ,
shppref_online VARCHAR(1) ,
add_date TIMESTAMP WITHOUT TIME ZONE ,
update_date TIMESTAMP WITHOUT TIME ZONE ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
customer_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.color;
CREATE TABLE IF NOT EXISTS vfap_retail.color(
color_code INTEGER ,
color_description VARCHAR(30) ,
fs_sk INTEGER ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.etl_rpt_missing_date;
CREATE TABLE IF NOT EXISTS vfap_retail.etl_rpt_missing_date(
table_name VARCHAR(30) ,
sas_brand_id VARCHAR(1) ,
missing_date TIMESTAMP WITHOUT TIME ZONE ,
process_dtm TIMESTAMP default sysdate
)
DISTSTYLE EVEN;

DROP table vfap_retail.snowsports_style;
CREATE TABLE IF NOT EXISTS vfap_retail.snowsports_style(
department_code INTEGER ,
department_description VARCHAR(24) ,
class_code INTEGER ,
class_description VARCHAR(25) ,
style_id INTEGER ,
vendor_style VARCHAR(6) ,
style_description VARCHAR(25) ,
vendor_cd_4 VARCHAR(4) ,
vendor_cd_6 VARCHAR(6) ,
cnodeid INTEGER ,
short_name VARCHAR(8) ,
long_name VARCHAR(8) ,
style_description_short VARCHAR(20) ,
style_description_long VARCHAR(40) ,
product_line VARCHAR(24) ,
sbu VARCHAR(13) ,
series VARCHAR(12) ,
newconsumersegmentterritory VARCHAR(15) ,
newendusesport VARCHAR(10) ,
newproductcollectionfamily VARCHAR(12) ,
tnf___clm_description_for_sas VARCHAR(40) ,
snowsports_flag VARCHAR(10) ,
snowsports_subsegment VARCHAR(12) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;


DROP table vfap_retail.rainwear_style;
CREATE TABLE IF NOT EXISTS vfap_retail.rainwear_style(
style_id INTEGER ,
style_description VARCHAR(25) ,
department_code INTEGER ,
department_description VARCHAR(17) ,
class_code INTEGER ,
class_description VARCHAR(21) ,
territory VARCHAR(18) ,
activity VARCHAR(23) ,
vendor_style VARCHAR(6) ,
vendor_cd_4 VARCHAR(4) ,
vendor_cd_6 VARCHAR(6) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.summit_style_ids_new;
CREATE TABLE IF NOT EXISTS vfap_retail.summit_style_ids_new(
department_code INTEGER ,
department_description VARCHAR(25) ,
class_code INTEGER ,
class_description VARCHAR(25) ,
style_id INTEGER ,
vendor_style VARCHAR(6) ,
style_description VARCHAR(25) ,
vendor_cd_4 VARCHAR(4) ,
vendor_cd_6 VARCHAR(6) ,
cnodeid INTEGER ,
short_name VARCHAR(8) ,
long_name VARCHAR(8) ,
style_description_short VARCHAR(21) ,
style_description_long VARCHAR(39) ,
product_line VARCHAR(36) ,
sbu VARCHAR(15) ,
series VARCHAR(13) ,
newconsumersegmentterritory VARCHAR(15) ,
newendusesport VARCHAR(23) ,
newproductcollectionfamily VARCHAR(13) ,
tnf___clm_description_for_sas VARCHAR(33) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.class;
CREATE TABLE IF NOT EXISTS vfap_retail.class(
class_code INTEGER ,
department_code INTEGER ,
class_description VARCHAR(27) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.store;
CREATE TABLE IF NOT EXISTS vfap_retail.store(
store_no INTEGER ,
store_name VARCHAR(25) ,
store_grouping_code_a VARCHAR(3) ,
store_address_1 VARCHAR(40) ,
store_address_2 VARCHAR(25) ,
store_address_3 VARCHAR(19) ,
store_address_4 VARCHAR(3) ,
store_post_code VARCHAR(10) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.vans_peanuts_style;
CREATE TABLE IF NOT EXISTS vfap_retail.vans_peanuts_style(
style_id INTEGER ,
vendor_code INTEGER ,
class_code INTEGER ,
product_code BIGINT ,
primary_size_code INTEGER ,
primary_size_label VARCHAR(3) ,
item_number VARCHAR(24) ,
gender VARCHAR(12) ,
category VARCHAR(11) ,
description VARCHAR(50) ,
color VARCHAR(32) ,
mtrl VARCHAR(6) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.farnorthern_style;
CREATE TABLE IF NOT EXISTS vfap_retail.farnorthern_style(
department_code INTEGER ,
department_description VARCHAR(15) ,
class_code INTEGER ,
class_description VARCHAR(11) ,
style_id INTEGER ,
vendor_style VARCHAR(6) ,
style_description VARCHAR(25) ,
vendor_cd_4 VARCHAR(4) ,
vendor_cd_6 VARCHAR(6) ,
cnodeid INTEGER ,
short_name VARCHAR(8) ,
long_name VARCHAR(8) ,
style_description_short VARCHAR(20) ,
style_description_long VARCHAR(36) ,
product_line VARCHAR(22) ,
sbu VARCHAR(13) ,
series VARCHAR(13) ,
newconsumersegmentterritory VARCHAR(17) ,
newendusesport VARCHAR(17) ,
newproductcollectionfamily VARCHAR(13) ,
tnf___clm_description_for_sas VARCHAR(27) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;


DROP table vfap_retail.thermoball_style;
CREATE TABLE IF NOT EXISTS vfap_retail.thermoball_style(
department_code INTEGER ,
department_description VARCHAR(22) ,
class_code INTEGER ,
class_description VARCHAR(24) ,
style_id INTEGER ,
vendor_style VARCHAR(6) ,
style_description VARCHAR(25) ,
vendor_cd_4 VARCHAR(4) ,
vendor_cd_6 VARCHAR(6) ,
cnodeid INTEGER ,
short_name VARCHAR(8) ,
long_name VARCHAR(8) ,
style_description_short VARCHAR(20) ,
style_description_long VARCHAR(39) ,
product_line VARCHAR(35) ,
sbu VARCHAR(15) ,
series VARCHAR(13) ,
newconsumersegmentterritory VARCHAR(18) ,
newendusesport VARCHAR(17) ,
newproductcollectionfamily VARCHAR(13) ,
tnf___clm_description_for_sas VARCHAR(28) ,
thermoball_flag VARCHAR(10) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.dept;
CREATE TABLE IF NOT EXISTS vfap_retail.dept(
department_code INTEGER ,
department_short_description VARCHAR(14) ,
department_description VARCHAR(27) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.event_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.event_xref(
attribute_grouping_code VARCHAR(4) ,
attribute_code VARCHAR(5) ,
attribute_description VARCHAR(30) ,
"primary" VARCHAR(28) ,
secondary1 VARCHAR(19) ,
secondary2 VARCHAR(19) ,
secondary3 VARCHAR(13) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.cmpgn_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.cmpgn_xref(
seq INTEGER ,
offer_category VARCHAR(34) ,
email_campaign_category VARCHAR(16) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.string_pos_xref;
CREATE TABLE IF NOT EXISTS vfap_retail.string_pos_xref(
group_code VARCHAR(50) ,
col_name VARCHAR(50) ,
string_pos INTEGER ,
pos_label VARCHAR(100) ,
pos_label_desc VARCHAR(100) ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.etl_rpt_min_max_date;
CREATE TABLE IF NOT EXISTS vfap_retail.etl_rpt_min_max_date(
table_name VARCHAR(30) ,
column_checked VARCHAR(30) ,
min_dt TIMESTAMP WITHOUT TIME ZONE ,
max_dt TIMESTAMP WITHOUT TIME ZONE ,
total_missing_dates INTEGER ,
total_cnt BIGINT ,
sas_brand_id varchar(1) ,
process_dtm TIMESTAMP default sysdate
)
DISTSTYLE EVEN;


DROP table vfap_retail.region;
CREATE TABLE IF NOT EXISTS vfap_retail.region(
region_code INTEGER ,
region_name VARCHAR(24) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;


DROP table vfap_retail.trans_category;
CREATE TABLE IF NOT EXISTS vfap_retail.trans_category(
transaction_category VARCHAR(1) ,
category_description VARCHAR(14) ,
sas_brand_id INTEGER ,
fs_sk INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.score_stat_history;
CREATE TABLE IF NOT EXISTS vfap_retail.score_stat_history(
model VARCHAR(80) ,
min_score INTEGER ,
max_score INTEGER ,
high_cutoff INTEGER ,
low_cutoff INTEGER ,
dt TIMESTAMP WITHOUT TIME ZONE ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.etl_parm;
CREATE TABLE IF NOT EXISTS vfap_retail.etl_parm(
key1 VARCHAR(20) ,
key2 VARCHAR(20) ,
num_value INTEGER ,
char_value VARCHAR(100) ,
description VARCHAR(100) ,
decimal_val DOUBLE PRECISION ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;

DROP table vfap_retail.brand;
CREATE TABLE IF NOT EXISTS vfap_retail.brand(
brand_id_desc VARCHAR(30) ,
sas_brand_id INTEGER ,
process_dtm TIMESTAMP default sysdate,
file_name VARCHAR(200)
)
DISTSTYLE EVEN;
COMMIT;

CREATE TABLE IF NOT EXISTS vfap_retail.experian_behavior
(
	customer_no BIGINT   
	,first_name VARCHAR(57)   
	,last_name VARCHAR(69)   
	,email_address VARCHAR(144)   
	,mailresponder_indiv VARCHAR(3)   
	,multicodmresp_hh VARCHAR(3)   
	,multicategorybuyer VARCHAR(3)   
	,dmcat_giftgadget INTEGER   
	,dmcat_collspcfood INTEGER   
	,dmcat_books INTEGER   
	,dmcat_gardenfarm INTEGER   
	,dmcat_craftshobbies INTEGER   
	,dmcat_femaleorient INTEGER   
	,dmcat_maleorient INTEGER   
	,dmcat_upscale INTEGER   
	,dmcat_general INTEGER   
	,magbyrcat_healthfit INTEGER   
	,magbyrcat_culinary INTEGER   
	,magbyrcat_gardenfarm INTEGER   
	,magbyrcat_religious INTEGER   
	,magbyrcat_malesport INTEGER   
	,magbyrcat_female INTEGER   
	,magbyrcat_familygen INTEGER   
	,contribcat_general INTEGER   
	,contribcat_health INTEGER   
	,contribcat_political INTEGER   
	,contribcat_religious INTEGER   
	,sweepstakes INTEGER   
	,do_it_yourselfers INTEGER   
	,news_financial INTEGER   
	,photography INTEGER   
	,mailrespond_oddsends INTEGER   
	,mailrespond_misc INTEGER   
	,political_affiliation VARCHAR(6)   
	,firsttimehmebuyr_enh INTEGER   
	,interest_in_golf VARCHAR(3)   
	,contrib_to_charities VARCHAR(3)   
	,pet_enthusiast VARCHAR(3)   
	,z_culturalarts VARCHAR(3)   
	,mail_order_buyer VARCHAR(3)   
	,z_int_in_fitness VARCHAR(3)   
	,z_int_in_outdoors VARCHAR(3)   
	,z_int_in_travel VARCHAR(3)   
	,z_investors VARCHAR(3)   
	,presence_of_auto VARCHAR(3)   
	,presence_creditcard VARCHAR(3)   
	,z_int_gardening VARCHAR(3)   
	,z_int_crafts VARCHAR(3)   
	,z_collectors VARCHAR(3)   
	,z_cruise_enthusiasts VARCHAR(3)   
	,z_int_sports VARCHAR(3)   
	,z_int_gourmet_cook VARCHAR(3)   
	,z_sweepstakes_gambling VARCHAR(3)   
	,z_int_politics VARCHAR(3)   
	,z_int_music VARCHAR(3)   
	,z_int_reading VARCHAR(3)   
	,z_childparentingprod VARCHAR(3)   
	,z_do_it_yourselfer VARCHAR(3)   
	,z_self_improve VARCHAR(3)   
	,z_int_religion VARCHAR(3)   
	,z_grandparent VARCHAR(3)   
	,z_int_clothing VARCHAR(3)   
	,z_donate_environment VARCHAR(3)   
	,z_investmutualfunds VARCHAR(3)   
	,z_weight_conscious VARCHAR(3)   
	,z_mailordermultibuyer VARCHAR(3)   
	,z_pres_premiumcc VARCHAR(3)   
	,z_dog_enthusiasts VARCHAR(3)   
	,z_cat_enthusiasts VARCHAR(3)   
	,z_healthy_living VARCHAR(3)   
	,z_int_automobile VARCHAR(3)   
	,z_int_skiing VARCHAR(3)   
	,z_int_boating VARCHAR(3)   
	,z_present_cellphone VARCHAR(3)   
	,z_comm_connectivity VARCHAR(3)   
	,z_comp_peripherals VARCHAR(3)   
	,z_hi_tech_owner VARCHAR(3)   
	,z_homedecor_furnish VARCHAR(3)   
	,z_homeenter_tv_video VARCHAR(3)   
	,z_kitchaidsmapplianc VARCHAR(3)   
	,z_mailorder_musicvideo VARCHAR(3)   
	,z_mailorder_booksmags VARCHAR(3)   
	,z_mailorderclothshoe VARCHAR(3)   
	,z_mailorder_insfin VARCHAR(3)   
	,z_mailorder_gifts VARCHAR(3)   
	,z_mailorder_garden VARCHAR(3)   
	,z_mailorder_jewelcos VARCHAR(3)   
	,z_music_classopera VARCHAR(3)   
	,z_music_country VARCHAR(3)   
	,z_music_christian VARCHAR(3)   
	,z_music_rock VARCHAR(3)   
	,z_interntonlne_subsc VARCHAR(3)   
	,z_photography VARCHAR(3)   
	,z_pur_via_online VARCHAR(3)   
	,z_int_afflulifestyle VARCHAR(3)   
	,z_domestic_traveler VARCHAR(3)   
	,z_foreign_traveler VARCHAR(3)   
	,z_contactlenswearer VARCHAR(3)   
	,z_prescrip_druguser VARCHAR(3)   
	,z_int_videodvd VARCHAR(3)   
	,z_active_military VARCHAR(3)   
	,z_inactive_military VARCHAR(3)   
	,morbank_dedcathit INTEGER   
	,morbank_nondedcathit INTEGER   
	,political_affil_2 VARCHAR(6)   
	,political_affil_3 VARCHAR(6)   
	,political_affil_4 VARCHAR(6)   
	,political_affil_5 VARCHAR(6)   
	,political_affil_6 VARCHAR(6)   
	,political_affil_7 VARCHAR(6)   
	,political_affil_8 VARCHAR(6)   
	,through_the_mail VARCHAR(3)   
	,other_collectibles VARCHAR(3)   
	,art_antiques VARCHAR(3)   
	,stamps_coins VARCHAR(3)   
	,dolls VARCHAR(3)   
	,figurines VARCHAR(3)   
	,sports_memorabilia VARCHAR(3)   
	,cooking VARCHAR(3)   
	,baking VARCHAR(3)   
	,cooking_weightcons VARCHAR(3)   
	,wine_appreciation VARCHAR(3)   
	,cooking_gourmet VARCHAR(3)   
	,crafts VARCHAR(3)   
	,knitting_needlework VARCHAR(3)   
	,quilting VARCHAR(3)   
	,sewing VARCHAR(3)   
	,woodworking VARCHAR(3)   
	,losing_weight VARCHAR(3)   
	,vitamin_supplements VARCHAR(3)   
	,health_naturalfoods VARCHAR(3)   
	,hobby_photography VARCHAR(3)   
	,hobby_gardening VARCHAR(3)   
	,home_workshop_diy VARCHAR(3)   
	,cars_auto_repair VARCHAR(3)   
	,bird_watching VARCHAR(3)   
	,self_improvement VARCHAR(3)   
	,christian_gospel VARCHAR(3)   
	,r_and_b VARCHAR(3)   
	,jazz_newage VARCHAR(3)   
	,classical VARCHAR(3)   
	,rock_n_roll VARCHAR(3)   
	,country VARCHAR(3)   
	,music_general VARCHAR(3)   
	,other_music VARCHAR(3)   
	,best_selling_fiction VARCHAR(3)   
	,bible_religious VARCHAR(3)   
	,childrensreading VARCHAR(3)   
	,computer VARCHAR(3)   
	,cooking_culinary VARCHAR(3)   
	,country_lifestyle VARCHAR(3)   
	,entertainment_people VARCHAR(3)   
	,fashion VARCHAR(3)   
	,history VARCHAR(3)   
	,interior_decorating VARCHAR(3)   
	,medical_health VARCHAR(3)   
	,military VARCHAR(3)   
	,mystery VARCHAR(3)   
	,naturalhlth_remedies VARCHAR(3)   
	,romance VARCHAR(3)   
	,science_fiction VARCHAR(3)   
	,science_technology VARCHAR(3)   
	,sports_reading VARCHAR(3)   
	,worldnews_politics VARCHAR(3)   
	,book_reader VARCHAR(3)   
	,animal_welfare_socl VARCHAR(3)   
	,environment_wildlife VARCHAR(3)   
	,political_conserve VARCHAR(3)   
	,political_liberal VARCHAR(3)   
	,religious_socialconc VARCHAR(3)   
	,children VARCHAR(3)   
	,veterans VARCHAR(3)   
	,health_social_conc VARCHAR(3)   
	,other_social_conc VARCHAR(3)   
	,sportsrec_baseball VARCHAR(3)   
	,sportsrec_basketball VARCHAR(3)   
	,sportsrec_hockey VARCHAR(3)   
	,sportsrec_camp_hike VARCHAR(3)   
	,sportsrec_hunting VARCHAR(3)   
	,sportsrec_fishing VARCHAR(3)   
	,sportsrec_nascar VARCHAR(3)   
	,sportsrec_per_fitexr VARCHAR(3)   
	,sportsrec_scuba_dive VARCHAR(3)   
	,sportsrec_football VARCHAR(3)   
	,sportsrec_golf VARCHAR(3)   
	,sportsrec_snowskibrd VARCHAR(3)   
	,sportsrec_boat_sail VARCHAR(3)   
	,sportsrec_walking VARCHAR(3)   
	,sportsrec_cycling VARCHAR(3)   
	,sportsrec_motorcycle VARCHAR(3)   
	,sportsrec_run_jog VARCHAR(3)   
	,sportsrec_soccer VARCHAR(3)   
	,sportsrec_swim VARCHAR(3)   
	,playsports_general VARCHAR(3)   
	,sweepstakes2 VARCHAR(3)   
	,lotteries VARCHAR(3)   
	,casino_gambling VARCHAR(3)   
	,cruise VARCHAR(3)   
	,international VARCHAR(3)   
	,domestic VARCHAR(3)   
	,time_share VARCHAR(3)   
	,recreational_vehicle VARCHAR(3)   
	,enjoy_rv_travel VARCHAR(3)   
	,business_travel VARCHAR(3)   
	,personal_travel VARCHAR(3)   
	,own_computer VARCHAR(3)   
	,use_internet_service VARCHAR(3)   
	,useinternetdsl_hspd VARCHAR(3)   
	,plan_to_buy_computer VARCHAR(3)   
	,cell_phone VARCHAR(3)   
	,compact_disc_player VARCHAR(3)   
	,dvd_player VARCHAR(3)   
	,satellite_dish VARCHAR(3)   
	,digital_camera VARCHAR(3)   
	,hdtv VARCHAR(3)   
	,int_electronics VARCHAR(3)   
	,personal_data_asst VARCHAR(3)   
	,video_camera VARCHAR(3)   
	,video_game_system VARCHAR(3)   
	,arts_int_culturlarts VARCHAR(3)   
	,business_finance VARCHAR(3)   
	,childrens_magazines VARCHAR(3)   
	,computer_electronics VARCHAR(3)   
	,crafts_games_hobbies VARCHAR(3)   
	,fitness VARCHAR(3)   
	,food_wine_cooking VARCHAR(3)   
	,gardening_magazines VARCHAR(3)   
	,hunting_fishing VARCHAR(3)   
	,mens VARCHAR(3)   
	,music VARCHAR(3)   
	,parenting_babies VARCHAR(3)   
	,sports_magazines VARCHAR(3)   
	,subscription VARCHAR(3)   
	,travel VARCHAR(3)   
	,womens VARCHAR(3)   
	,currentevents_news VARCHAR(3)   
	,have_leased_vehicle VARCHAR(3)   
	,havepurchasedvehicle VARCHAR(3)   
	,book_club VARCHAR(3)   
	,music_club VARCHAR(3)   
	,amer_express_premium VARCHAR(3)   
	,amer_express_regular VARCHAR(3)   
	,discover_premium VARCHAR(3)   
	,discover_regular VARCHAR(3)   
	,othercard_premium VARCHAR(3)   
	,othercard_regular VARCHAR(3)   
	,store_retail_regular VARCHAR(3)   
	,visa_regular VARCHAR(3)   
	,mastercard_regular VARCHAR(3)   
	,humanitarian VARCHAR(3)   
	,buyprerecrded_videos VARCHAR(3)   
	,watch_cable_tv VARCHAR(3)   
	,watch_videos VARCHAR(3)   
	,cds_moneymkt_current VARCHAR(3)   
	,iras_current VARCHAR(3)   
	,iras_future_interest VARCHAR(3)   
	,life_insur_current VARCHAR(3)   
	,mutual_funds_current VARCHAR(3)   
	,mutualfunds_future VARCHAR(3)   
	,otherinvestmt_curr VARCHAR(3)   
	,otherinvestmt_future VARCHAR(3)   
	,real_estate_current VARCHAR(3)   
	,real_estate_future VARCHAR(3)   
	,stocks_bonds_current VARCHAR(3)   
	,stocks_bonds_future VARCHAR(3)   
	,proud_grandparent VARCHAR(3)   
	,purchase_via_internet VARCHAR(3)   
	,active_military_mbr VARCHAR(3)   
	,vetern VARCHAR(3)   
	,own_a_cat VARCHAR(3)   
	,own_a_dog VARCHAR(3)   
	,own_a_pet VARCHAR(3)   
	,shop_by_internet VARCHAR(3)   
	,shop_by_mail VARCHAR(3)   
	,shop_by_catalog VARCHAR(3)   
	,apparel VARCHAR(3)   
	,computerorelctronics VARCHAR(3)   
	,books_music VARCHAR(3)   
	,home_garden VARCHAR(3)   
	,sports_related VARCHAR(3)   
	,big_tall VARCHAR(3)   
	,mailorder_books_mag VARCHAR(3)   
	,mailorder_clothing VARCHAR(3)   
	,mailorder_gardening VARCHAR(3)   
	,mailorder_gift VARCHAR(3)   
	,mailorder_jewel_cosm VARCHAR(3)   
	,mailorder_music_vid VARCHAR(3)   
	,mailorder_child_prod VARCHAR(3)   
	,int_amuseparkvisit INTEGER   
	,int_zoo_visit INTEGER   
	,int_coffeeconnoisseu INTEGER   
	,int_winelovers INTEGER   
	,int_doityourselfers INTEGER   
	,int_homeimprve_spndr INTEGER   
	,hunting_enthusiasts INTEGER   
	,buyer_laptopowner INTEGER   
	,buyer_secursysowner INTEGER   
	,buyer_tabletowner INTEGER   
	,buyer_couponuser INTEGER   
	,buyer_luxuryshopper INTEGER   
	,buyer_yngadultcloth INTEGER   
	,buyer_supercenter INTEGER   
	,buyer_warehouseclub INTEGER   
	,aarp_members INTEGER   
	,life_insur_policy INTEGER   
	,medical_policy INTEGER   
	,medicare_policy INTEGER   
	,digital_magnewspaper INTEGER   
	,attnds_educationprog INTEGER   
	,video_gamer INTEGER   
	,mlb_enthusiast INTEGER   
	,nascar_enthusiast INTEGER   
	,nba_enthusiast INTEGER   
	,nfl_enthusiast INTEGER   
	,nhl_enthusiast INTEGER   
	,pga_tour_enthusiast INTEGER   
	,politicaltv_liberal INTEGER   
	,politictv_libcomedy INTEGER   
	,politicaltv_conserva INTEGER   
	,eats_family_rest INTEGER   
	,eats_fast_food INTEGER   
	,canoeing_kayaking INTEGER   
	,int_play_golf INTEGER   
	,presence_automobile INTEGER   
	,loyalty_card_user INTEGER   
	,luxuryhomegood_shop INTEGER   
	,union_member INTEGER   
	,have_retirement_plan INTEGER   
	,online_trading INTEGER   
	,have_grandchildren INTEGER   
	,nondept_store_makeup INTEGER   
	,int_gourmet_cooking INTEGER   
	,int_cat_owner INTEGER   
	,int_dog_owner INTEGER   
	,int_arts_crafts INTEGER   
	,int_scrapbooking INTEGER   
	,int_cultural_arts INTEGER   
	,hobbies_gardening INTEGER   
	,int_photography INTEGER   
	,int_book_reader INTEGER   
	,int_ebook_reader INTEGER   
	,int_audiobk_listener INTEGER   
	,int_pet_enthusiast INTEGER   
	,int_music_download INTEGER   
	,int_music_streaming INTEGER   
	,int_avid_runners INTEGER   
	,int_outdoor_enthusia INTEGER   
	,int_fishing INTEGER   
	,int_snowsports INTEGER   
	,int_boating INTEGER   
	,int_playshockey INTEGER   
	,int_plays_soccer INTEGER   
	,int_plays_tennis INTEGER   
	,int_sportsenthusiast INTEGER   
	,int_healthy_living INTEGER   
	,int_fitnessenthusias INTEGER   
	,int_on_a_diet INTEGER   
	,int_weight_conscious INTEGER   
	,buyer_highendspirits INTEGER   
	,int_casinogambling INTEGER   
	,int_swpstakeslottery INTEGER   
	,hifreq_business_trvl INTEGER   
	,hifreq_cruise_enthus INTEGER   
	,hifreq_domestic_vac INTEGER   
	,hifreq_foreign_vac INTEGER   
	,freq_flyer_prg_mbr INTEGER   
	,hotelguest_loyaprg INTEGER   
	,contrib_charities INTEGER   
	,contrib_artsculture INTEGER   
	,contrib_education INTEGER   
	,contrib_health INTEGER   
	,contrib_political INTEGER   
	,contrib_prvt_found INTEGER   
	,contrib_volunteering INTEGER   
	,debit_card_user INTEGER   
	,corp_credit_crd_user INTEGER   
	,maj_credit_crd_user INTEGER   
	,prem_credit_crd_user INTEGER   
	,credit_card_user INTEGER   
	,store_creditcrd_user INTEGER   
	,brokerage_acct_owner INTEGER   
	,active_investor INTEGER   
	,mutual_fund_investor INTEGER   
	,deptstore_makeupuser INTEGER   
	,int_christian_music INTEGER   
	,int_classical_music INTEGER   
	,int_country_music INTEGER   
	,int_music INTEGER   
	,int_oldies_music INTEGER   
	,int_rock_music INTEGER   
	,int_80s_music INTEGER   
	,int_hiphopmusic INTEGER   
	,int_alternativemusic INTEGER   
	,int_jazz_music INTEGER   
	,int_pop_music INTEGER   
	,interest_in_religion INTEGER   
	,military_active INTEGER   
	,military_inactive INTEGER   
	,working_couples INTEGER   
	,poc_0_3_yrs_gender VARCHAR(3)   
	,poc_4_6_yrs_gender VARCHAR(3)   
	,poc_7_9_yrs_gender VARCHAR(3)   
	,poc_10_12_yrs_gender VARCHAR(3)   
	,poc_13_15_yrs_gender VARCHAR(3)   
	,poc_16_18_yrs_gender VARCHAR(3)   
	,discret_spend_level VARCHAR(3)   
	,discret_spend_score INTEGER   
	,p1_birthdt_indiv INTEGER   
	,p1_combage_indiv VARCHAR(9)   
	,p1_gender_indiv VARCHAR(3)   
	,p1_marital_status VARCHAR(6)   
	,recip_reliability_cd INTEGER   
	,p1_pertype_indiv VARCHAR(3)   
	,combined_homeowner VARCHAR(3)   
	,dwelling_type VARCHAR(3)   
	,length_of_residence INTEGER   
	,dwellingunit_size_cd VARCHAR(3)   
	,number_child_ltet18 INTEGER   
	,nbr_adults_in_hh INTEGER   
	,p2_person_type VARCHAR(3)   
	,p2_name_first VARCHAR(45)   
	,p2_name_mid_init VARCHAR(45)   
	,p2_name_last VARCHAR(60)   
	,p2_name_title VARCHAR(18)   
	,p2_name_surnamesffx VARCHAR(12)   
	,p2_gender_cd VARCHAR(3)   
	,p2_combined_age VARCHAR(9)   
	,p2_birthdt_ccyymm INTEGER   
	,p3_person_type VARCHAR(3)   
	,p3_name_first VARCHAR(45)   
	,p3_name_mid_init VARCHAR(45)   
	,p3_name_last VARCHAR(60)   
	,p3_name_title VARCHAR(18)   
	,p3_name_surnamesffx VARCHAR(12)   
	,p3_gender_cd VARCHAR(3)   
	,p3_combined_age VARCHAR(9)   
	,p3_birthdt_ccyymm INTEGER   
	,p4_person_type VARCHAR(3)   
	,p4_name_first VARCHAR(45)   
	,p4_name_mid_init VARCHAR(45)   
	,p4_name_last VARCHAR(60)   
	,p4_name_title VARCHAR(18)   
	,p4_name_surnamesffx VARCHAR(12)   
	,p4_gender_cd VARCHAR(3)   
	,p4_combined_age VARCHAR(9)   
	,p4_birthdt_ccyymm INTEGER   
	,p5_person_type VARCHAR(3)   
	,p5_name_first VARCHAR(45)   
	,p5_name_mid_init VARCHAR(42)   
	,p5_name_last VARCHAR(60)   
	,p5_name_title VARCHAR(15)   
	,p5_name_surnamesffx VARCHAR(12)   
	,p5_gender_cd VARCHAR(3)   
	,p5_combined_age VARCHAR(9)   
	,p5_birthdt_ccyymm INTEGER   
	,p6_person_type VARCHAR(3)   
	,p6_name_first VARCHAR(45)   
	,p6_name_mid_init VARCHAR(36)   
	,p6_name_last VARCHAR(60)   
	,p6_name_title VARCHAR(18)   
	,p6_name_surnamesffx VARCHAR(12)   
	,p6_gender_cd VARCHAR(3)   
	,p6_combined_age VARCHAR(9)   
	,p6_birthdt_ccyymm INTEGER   
	,p7_person_type VARCHAR(3)   
	,p7_name_first VARCHAR(45)   
	,p7_name_mid_init VARCHAR(33)   
	,p7_name_last VARCHAR(57)   
	,p7_name_title VARCHAR(12)   
	,p7_name_surnamesffx VARCHAR(12)   
	,p7_gender_cd VARCHAR(3)   
	,p7_combined_age VARCHAR(9)   
	,p7_birthdt_ccyymm INTEGER   
	,p8_person_type VARCHAR(3)   
	,p8_name_first VARCHAR(45)   
	,p8_name_mid_init VARCHAR(30)   
	,p8_name_last VARCHAR(60)   
	,p8_name_title VARCHAR(12)   
	,p8_name_surnamesffx VARCHAR(9)   
	,p8_gender_cd VARCHAR(3)   
	,p8_combined_age VARCHAR(9)   
	,p8_birthdt_ccyymm INTEGER   
	,p1_occupation VARCHAR(24)   
	,p1_occupation_grp VARCHAR(6)   
	,p1_education VARCHAR(24)   
	,presofchild_age_0_3 VARCHAR(6)   
	,presofchild_age_4_6 VARCHAR(6)   
	,presofchild_age_7_9 VARCHAR(6)   
	,presofchild_age10_12 VARCHAR(6)   
	,presofchild_age13_15 VARCHAR(6)   
	,presofchild_age16_18 VARCHAR(6)   
	,presofchild_age_0_18 VARCHAR(6)   
	,home_business_ind VARCHAR(3)   
	,p1_businessownerflg VARCHAR(3)   
	,est_curr_home_value VARCHAR(24)   
	,multiple_realty_prop VARCHAR(3)   
	,mosaic_household VARCHAR(9)   
	,est_household_income VARCHAR(3)   
	,p1_retail_shoppers VARCHAR(24)   
	,consumview_prfitscr INTEGER   
	,cs_combined VARCHAR(3)   
	,cs_clothing VARCHAR(3)   
	,cs_entertainment VARCHAR(3)   
	,cs_travel VARCHAR(3)   
	,retail_demand_20200 INTEGER   
	,retail_demand_20220 INTEGER   
	,retail_demand_20240 INTEGER   
	,retail_demand_20260 INTEGER   
	,retail_demand_20530 INTEGER   
	,retail_demand_20580 INTEGER   
	,retail_demand_448 INTEGER   
	,retail_demand_4481 INTEGER   
	,retail_demand_44811 INTEGER   
	,retail_demand_44812 INTEGER   
	,retail_demand_44813 INTEGER   
	,retail_demand_44814 INTEGER   
	,retail_demand_44815 INTEGER   
	,retail_demand_44819 INTEGER   
	,retail_demand_451 INTEGER   
	,retail_demand_4511 INTEGER   
	,retail_demand_45111 INTEGER   
	,avg_scorex_plus INTEGER   
	,median_scorex_plus INTEGER   
	,badvsgood_credit_seg INTEGER   
	,scps_avg_taps_spend INTEGER   
	,scpsavg_taps_payrate INTEGER   
	,online_apparel INTEGER   
	,online_shoes INTEGER   
	,online_outdrsftgoods INTEGER   
	,online_outdrhrdgoods INTEGER   
	,ltd_best_recency TIMESTAMP WITHOUT TIME ZONE   
	,ltd_total_dollars INTEGER   
	,ltd_avg_dollar_pur INTEGER   
	,ltc_credit_pur INTEGER   
	,ltd_purchase_freq INTEGER   
	,month_24_credit_pur INTEGER   
	,month_24_recency_cd INTEGER   
	,month_24_freq_cd INTEGER   
	,month_24_dollar_cd INTEGER   
	,month_24_avgdollarcd INTEGER   
	,w_hiend_apparelrecen TIMESTAMP WITHOUT TIME ZONE   
	,w_hiend_apparelfreq VARCHAR(24)   
	,w_hiend_appareldolla VARCHAR(24)   
	,w_hiend_apparelscore VARCHAR(24)   
	,w_mid_apparelrecen VARCHAR(24)   
	,w_mid_apparelfreq INTEGER   
	,w_mid_appareldolla INTEGER   
	,w_mid_apparelscore INTEGER   
	,w_low_apparelrecen TIMESTAMP WITHOUT TIME ZONE   
	,w_low_apparelfreq INTEGER   
	,w_low_appareldolla INTEGER   
	,w_low_apparelscore INTEGER   
	,w_cas_apparelrecen TIMESTAMP WITHOUT TIME ZONE   
	,w_cas_apparelfreq INTEGER   
	,w_cas_appareldolla INTEGER   
	,w_cas_apparelscore INTEGER   
	,w_plus_apparelrecen TIMESTAMP WITHOUT TIME ZONE   
	,w_plus_apparelfreq INTEGER   
	,w_plus_appareldolla INTEGER   
	,w_plus_apparelscore INTEGER   
	,w_athle_apparelrecen TIMESTAMP WITHOUT TIME ZONE   
	,w_athle_apparelfreq INTEGER   
	,w_athle_appareldolla INTEGER   
	,w_athle_apparelscore INTEGER   
	,m_cas_apparelrecen TIMESTAMP WITHOUT TIME ZONE   
	,m_cas_apparelfreq INTEGER   
	,m_cas_appareldolla INTEGER   
	,m_cas_apparelscore INTEGER   
	,child_apparelrecen TIMESTAMP WITHOUT TIME ZONE   
	,child_apparelfreq INTEGER   
	,child_appareldolla INTEGER   
	,child_apparelscore INTEGER   
	,active_outdoor_recen TIMESTAMP WITHOUT TIME ZONE   
	,active_outdoor_freq INTEGER   
	,active_outdoor_dolla INTEGER   
	,active_outdoor_score INTEGER   
	,x_snowsports_recen TIMESTAMP WITHOUT TIME ZONE   
	,x_snowsports_freq INTEGER   
	,x_snowsports_dolla INTEGER   
	,x_snowsports_score INTEGER   
	,mth_0_3_trans_freq INTEGER   
	,mth_0_3_trans_dollar INTEGER   
	,mth_4_6_trans_freq INTEGER   
	,mth_4_6_trans_dollar INTEGER   
	,mth_7_9_trans_freq INTEGER   
	,mth_7_9_trans_dollar INTEGER   
	,mth_10_12_trans_freq INTEGER   
	,mth_10_12_trans_doll INTEGER   
	,mth_13_18_trans_freq INTEGER   
	,mth_13_18_trans_doll INTEGER   
	,mth_19_24_trans_freq INTEGER   
	,mth_19_24_trans_doll INTEGER   
	,most_recent_ordr_chan VARCHAR(3)   
	,tt_brick_mortar INTEGER   
	,tt_online_midhigh INTEGER   
	,tt_e_tailer INTEGER   
	,tt_online_discount INTEGER   
	,tt_onlinebid_mrktplc INTEGER   
	,tt_emailengagement INTEGER   
	,p1_tt_emailengagemnt INTEGER   
	,p2_tt_emailengagemnt INTEGER   
	,p3_tt_emailengagemnt INTEGER   
	,p4_tt_emailengagemnt INTEGER   
	,p5_tt_emailengagemnt INTEGER   
	,p6_tt_emailengagemnt INTEGER   
	,p7_tt_emailengagemnt INTEGER   
	,p8_tt_emailengagemnt INTEGER   
	,tt_broadcast_cabletv INTEGER   
	,tt_directmail INTEGER   
	,tt_internetradio INTEGER   
	,tt_mobile_display INTEGER   
	,tt_mobile_video INTEGER   
	,tt_online_video INTEGER   
	,tt_online_display INTEGER   
	,tt_online_streamtv INTEGER   
	,tt_satelliteradio INTEGER   
	,tt_buyamerican INTEGER   
	,tt_showmethemoney INTEGER   
	,tt_gowiththeflow INTEGER   
	,tt_notimelikepresent INTEGER   
	,tt_nvrshowupemptyhan INTEGER   
	,tt_ontheroadagain INTEGER   
	,tt_lookatmenow INTEGER   
	,tt_stopsmellroses INTEGER   
	,tt_workhardplayhard INTEGER   
	,tt_pennysavedearned INTEGER   
	,tt_its_all_in_name INTEGER   
	,ethnicinsight_flag VARCHAR(3)   
	,ethnicity_detail VARCHAR(6)   
	,"language" VARCHAR(6)   
	,religion VARCHAR(3)
	,e_tech_group VARCHAR(3)
	,country_of_origin INTEGER   
	,totalenhanmatchtype VARCHAR(3)   
	,process_dtm TIMESTAMP WITHOUT TIME ZONE   
	,sas_brand_id INTEGER   
	,fs_sk INTEGER   
	,file_name VARCHAR(200)   
)
;