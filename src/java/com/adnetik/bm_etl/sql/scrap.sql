
DROP TABLE IF EXISTS ad_general_new;

CREATE TABLE ad_general_new (
	/* Generic Columns */
	ID_DATE			date,
	ID_CAMPAIGN		int(11),
	ID_LINEITEM 		int(11),
	ID_CREATIVE		int(11),	
	ID_EXCHANGE		tinyint(4),
	ID_CURRCODE		tinyint(4) DEFAULT 1,
	ID_UTW			int(6) DEFAULT NULL,	
	ID_COUNTRY		int(6),	

	/* specific to ad_general */	
	ID_ADVERTISER		int(12),
	ID_METROCODE		int(6),
	ID_REGION		int(6),	
	ID_SIZE			tinyint(4),
	ID_VISIBILITY		tinyint(4),
	ID_BROWSER		tinyint(4),
	ID_LANGUAGE		int(6),

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

/* CREATE INDEX entry_date_idx ON ad_general_new (entry_date);
CREATE INDEX id_date_idx ON ad_general_new (id_date); */

