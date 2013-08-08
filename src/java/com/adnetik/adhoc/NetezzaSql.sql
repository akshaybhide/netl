
create table imp_test_1
(
	date_time datetime NOT NULL,
	entry_date date NOT NULL,
	wtp_user_id varchar(40), 
	transaction_id varchar(40),
	ad_exchange varchar(15),
	campaign_id int,
	line_item_id int,
	size varchar(10),
	utw int,
	currency char(3),
	language varchar(20),
	domain varchar(100),
	user_country char(2),
	user_region char(2),
	user_city varchar(100),
	user_postal varchar(15),
	bid double,
	winner_price double,
	rand999 int
)

create table bk_test_1
(
	wtp_user_id varchar(40) NOT NULL,
	date_info date NOT NULL,
	sed_id int NOT NULL
);

date_time
entry_date
uuid            or wtp_user_id
transaction_id
ad_exchange
campaign_id
line_item_id
creative_id
size
utw
currency
language        or user_language
content
deal_id
visibility
domain
country         or user_country
region          or user_region
user_DMA
user_city
user_postal
dbh_publisher_id
is_mobile_app
ua_device_type
ua_device_maker or mobile_device_make?
ua_device_model or mobile_device_model?
os              or ua_os
os_version       or ua_os_version
browser  or ua_browser
                   ua_browser_version

In addition, the impression table has:

bid
winner_price
rand999
