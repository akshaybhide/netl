
DROP TABLE IF EXISTS rmx_data;


CREATE TABLE rmx_data (
	ID_DATE			date,
	AdnetikAdvertiserId 	int,
	AdnetikAdvertiserName 	varchar(200),
	campaign_id		int,
	AdnetikCampaignName	varchar(200),
	external_line_item_id	bigint(20) unsigned, /* note: this must be the same as adnetik.external_line_item.id */
	advertiser_line_item_name 	varchar(200),
	size_name		varchar(100),
	buyer_imps		int,
	buyer_clicks		int,
	buyer_convs		int,
	network_gross_cost	double,
	COST_TYPE		int,
	DOLLARS			int,
	PERCENTAGE		int,
	AdnetikCalculatedCost 	double,
	AdnetikNotes		varchar(200)
) ENGINE = 'InnoDB';

ALTER TABLE rmx_data ADD CONSTRAINT FOREIGN KEY (external_line_item_id) REFERENCES adnetik.external_line_item (id);

/* okay just do this in Java for now */
/* ALTER TABLE rmx_data ADD CONSTRAINT FOREIGN KEY (size_name) REFERENCES cat_size (name); *'
