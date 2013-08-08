
DROP TABLE IF EXISTS domain_staging;


CREATE TABLE domain_staging (
	ID_DATE			date,
	ID_CAMPAIGN		int,
	ID_LINEITEM	 	int,
	ID_EXCHANGE	 	int,
	NAME_DOMAIN		varchar(200),
	NUM_BIDS		int,
	NUM_IMPRESSIONS 	int,
	NUM_CLICKS		int,
	NUM_CONVERSIONS		int,
	NUM_CONV_POST_VIEW	int,
	NUM_CONV_POST_CLICK	int,
	IMP_COST		double,
	IMP_COST_EURO		double,
	IMP_COST_POUND		double,
	IMP_BID_AMOUNT		double
)  ENGINE = 'InnoDB';
	

/* ALTER TABLE rmx_data ADD CONSTRAINT FOREIGN KEY (external_line_item_id) REFERENCES adnetik.external_line_item (id); */

CREATE OR REPLACE VIEW domain_staging_view AS SELECT DS.*, DOMCAT.id as ID_DOMAIN
	FROM domain_staging DS 
	INNER JOIN test_cat_domain DOMCAT on DS.name_domain = DOMCAT.name;
	
CREATE INDEX domidx ON domain_staging (name_domain);
	
/* okay just do this in Java for now */
/* ALTER TABLE rmx_data ADD CONSTRAINT FOREIGN KEY (size_name) REFERENCES cat_size (name); *'
