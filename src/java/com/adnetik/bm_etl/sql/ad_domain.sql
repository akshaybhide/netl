
DROP TABLE IF EXISTS ad_domain;

CREATE TABLE ad_domain (
	/* Generic Columns */
	ID_DATE			date,
	ID_CAMPAIGN		int(11),
	ID_LINEITEM 		int(11),
	ID_CREATIVE		int(11),	
	ID_EXCHANGE		tinyint(4),
	ID_CURRCODE		tinyint(4) DEFAULT 1,
	ID_UTW			int(6) DEFAULT NULL,	
	ID_COUNTRY		int(6),	
	
	/* specific to ad_domain */
	ID_DOMAIN 		varchar(200),
	
	/* Internal only */
	/* ID_QUARTER		tinyint(3), */		

	/* Generic Facts */	
	NUM_BIDS		int,
	NUM_IMPRESSIONS 	int,
	NUM_CLICKS		int,
	NUM_CONVERSIONS		int,
	NUM_CONV_POST_VIEW	int,
	NUM_CONV_POST_CLICK	int,
	IMP_COST		double,
	IMP_BID_AMOUNT		double,
	EXT_LINEITEM 		bigint(20) unsigned DEFAULT NULL,		
	ENTRY_DATE		date	
) 
ENGINE = 'InnoDB' 
PARTITION BY HASH(id_campaign) PARTITIONS 1024;

CREATE INDEX entry_date_idx ON ad_general (entry_date);
CREATE INDEX id_date_idx ON ad_general (id_date);

/* okay just do this in Java for now */
/* ALTER TABLE rmx_data ADD CONSTRAINT FOREIGN KEY (size_name) REFERENCES cat_size (name); *'
