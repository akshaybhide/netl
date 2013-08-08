
CREATE TABLE ex_target_info (
	daycode date,
	line_item int,
	seg_id int,
	primary key(daycode, line_item, seg_id)
);

CREATE TABLE ex_price_info (
	daycode date,
	seg_id int, 
	price_cpm double,
	primary key (daycode, seg_id)
);

CREATE TABLE ex_usage_main (

	line_item int,
	max_seg_id int,
	price_cpm double,
	uuid char(40),
	nfsdate date, 
	key (max_seg_id)
);
