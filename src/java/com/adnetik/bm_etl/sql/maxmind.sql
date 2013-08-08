

create table ip_to_geo ( 
	min_ip int, 
	max_ip int, 
	country varchar(5), 
	region varchar(100), 
	city varchar(100),
	postal varchar(20),
	latitude double,
	longitude double,
	dmacode varchar(10),
	areacode varchar(10),
	PRIMARY KEY (min_ip)
)

