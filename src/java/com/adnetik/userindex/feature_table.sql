
CREATE TABLE report_info 
(
	`CAN_DAY` date NOT NULL,
	`POS_LIST_CODE` varchar(50) NOT NULL,
	`NEG_LIST_CODE` varchar(50) NOT NULL,
	REPORT_ID int NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (REPORT_ID)
);

CREATE INDEX report_idx ON report_info (pos_list_code);

create table listen_code 
( 
	listcode varchar(100), 
	nickname varchar(100),
	country char(2) NOT NULL,
	isgeo bit NOT NULL,
	entry_date date NOT NULL,
	complete_date date NOT NULL DEFAULT '2100-01-01',
	primary key(listcode)
)



CREATE TABLE user_counts 
(
	REPORT_ID int NOT NULL,
	POS_USER_TOTAL int NOT NULL,
	NEG_USER_TOTAL int NOT NULL,
	PRIMARY KEY (REPORT_ID),
	FOREIGN KEY (REPORT_ID) REFERENCES report_info (REPORT_ID)
)

CREATE TABLE `feature_table` (
	REPORT_ID int NOT NULL,
	`FEAT_NAME` 	varchar(200) NOT NULL,
	`FEAT_CODE` 	varchar(20) NOT NULL,
	`MUT_INFO`	double NOT NULL,
	`POS_COUNT` int NOT NULL,
	`NEG_COUNT` int NOT NULL,
	PRIMARY KEY (REPORT_ID, FEAT_NAME),
	FOREIGN KEY (REPORT_ID) REFERENCES report_info (REPORT_ID)	
);

CREATE TABLE `adaclass_info` (
	REPORT_ID int NOT NULL,
	`FEAT_NAME` 	varchar(200) NOT NULL,
	`NAME_KEY` 	varchar(100) NOT NULL,
	`WEIGHT`	double NOT NULL,
	FOREIGN KEY (REPORT_ID) REFERENCES report_info (REPORT_ID)	
);

CREATE INDEX ada_report_idx ON adaclass_info (REPORT_ID);




CREATE TABLE `lift_report` (
	REPORT_ID int NOT NULL,
	USER_RANK int NOT NULL,
	USER_SCORE double NOT NULL,
	PRIMARY KEY (REPORT_ID, USER_RANK),
	FOREIGN KEY (REPORT_ID) REFERENCES report_info (REPORT_ID)	
);

CREATE TABLE `eval_scheme` (
	REPORT_ID int NOT NULL,
	USER_RANK int NOT NULL,
	USER_SCORE double NOT NULL,
	PRIMARY KEY (REPORT_ID, USER_RANK),	
	FOREIGN KEY (REPORT_ID) REFERENCES report_info (REPORT_ID)	
);

CREATE TABLE `party3_report` (
	REPORT_ID int NOT NULL,
	`FEAT_NAME` 	varchar(200) NOT NULL,
	`FEAT_CODE` 	varchar(20) NOT NULL,
	`MUT_INFO`	double NOT NULL,
	`TRUE4SEED` int NOT NULL,
	`TRUE4CTRL` int NOT NULL,
	`TOTALSEED` int NOT NULL,
	`TOTALCTRL` int NOT NULL,
	PRIMARY KEY (REPORT_ID, FEAT_NAME),
	FOREIGN KEY (REPORT_ID) REFERENCES report_info (REPORT_ID)
);

CREATE TABLE `status_info` (
	`listcode` varchar(100) NOT NULL,
	`reptype` varchar(20) NOT NULL,
	`status` char(4), /* OKAY or FAIL */
	`tstamp` timestamp, 
	`comment` text
);


CREATE TABLE `bluekai_data` (
	REPORT_ID int NOT NULL,
	WTP_ID char(40) NOT NULL,
	RECENCY date,
	BK_SEG_ID int NOT NULL,
	KEY (REPORT_ID),
	FOREIGN KEY (REPORT_ID) REFERENCES report_info (REPORT_ID)	
);

CREATE TABLE list_pxprm_str 
(
	LISTCODE varchar(100) NOT NULL,
	PARAM_KEY varchar(40) NOT NULL,
	PARAM_VAL varchar(80) NOT NULL,
	FOREIGN KEY (LISTCODE) REFERENCES listen_code (LISTCODE)
)


CREATE OR REPLACE VIEW v__feature_sub_1 AS 
SELECT FT.*, 
UC.POS_USER_TOTAL as TOTALSEED,
UC.NEG_USER_TOTAL as TOTALCTRL,
FT.pos_count / UC.pos_user_total as PERC_POSITIVE, 
FT.neg_count / UC.neg_user_total as PERC_NEGATIVE
FROM feature_table FT 
INNER JOIN user_counts UC ON UC.report_id = FT.report_id
WHERE FT.feat_code != 'noop';


CREATE OR REPLACE VIEW v__full_feature AS 
SELECT VS.*,
IF(PERC_POSITIVE > PERC_NEGATIVE, +1, -1) AS DIRECTION,
IF(PERC_POSITIVE > PERC_NEGATIVE, MUT_INFO, -1*MUT_INFO) AS FEATURE_POWER
FROM v__feature_sub_1 as VS;

CREATE OR REPLACE VIEW v__full_eval AS 
SELECT ES.*, RI.CAN_DAY, RI.POS_LIST_CODE, RI.NEG_LIST_CODE 
FROM eval_scheme ES
JOIN report_info RI on ES.report_id = RI.report_id; 

CREATE OR REPLACE VIEW v__full_lift AS 
SELECT LR.*, RI.CAN_DAY, RI.POS_LIST_CODE, RI.NEG_LIST_CODE 
FROM lift_report LR
JOIN report_info RI on LR.report_id = RI.report_id; 

/* Multipixel auxiliary table */
CREATE TABLE `multipix` 
(
	listcode varchar(100) NOT NULL,
	pixid int NOT NULL,
	FOREIGN KEY (listcode) REFERENCES listen_code (listcode)
);



CREATE OR REPLACE TABLE `user_activity` (
	listcode 		varchar(20) NOT NULL,
	canday			date,
	date_time 		datetime NOT NULL,
	ad_exchange 		varchar(20) NOT NULL,
	url			varchar(300) NOT NULL,
	domain 			varchar(200) NOT NULL,
	url_keywords		varchar(200) NOT NULL,
	google_main_vertical	int NOT NULL,
	user_ip			varchar(20) NOT NULL,
	user_country 		varchar(10) NOT NULL,
	user_region 		varchar(20) NOT NULL,
	user_DMA		varchar(20) NOT NULL,
	user_city		varchar(50) NOT NULL,
	user_postal		varchar(20) NOT NULL,
	user_language		varchar(5) NOT NULL,
	browser			varchar(20) NOT NULL,
	os			varchar(20) NOT NULL,
	wtp_user_id		varchar(40) NOT NULL,
	time_zone		varchar(10) NOT NULL,
	size			varchar(10) NOT NULL
);

