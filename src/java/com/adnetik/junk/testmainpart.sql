CREATE TABLE main_fact 
(  entry_date date , dim_date date , dim_hour tinyint , dim_ad_exchange TINYINT , dim_campaign_id SMALLINT , dim_line_item_id INT , 
dim_creative_id MEDIUMINT , dim_browser SMALLINT , dim_user_country SMALLINT , dim_user_DMA MEDIUMINT , dim_user_region MEDIUMINT , dim_user_postal 
MEDIUMINT , dim_user_language SMALLINT , dim_domain INT , dim_visibility TINYINT , dim_size SMALLINT  )  
PARTITION BY RANGE(dim_date) SUBPARTITION BY HASH (dim_campaign) SUBPARTITIONS 100  ( PARTITION p0 VALUES LESS THAN '2012-05-25')


CREATE TABLE main_fact 
(  entry_date date , dim_date date , dim_hour tinyint , dim_ad_exchange TINYINT , dim_campaign_id SMALLINT , dim_line_item_id INT , 
dim_creative_id MEDIUMINT , dim_browser SMALLINT , dim_user_country SMALLINT , dim_user_DMA MEDIUMINT , dim_user_region MEDIUMINT , dim_user_postal 
MEDIUMINT , dim_user_language SMALLINT , dim_domain INT , dim_visibility TINYINT , dim_size SMALLINT  )  
PARTITION BY RANGE( TO_DAYS(dim_date) ) SUBPARTITION BY HASH (dim_campaign_id) SUBPARTITIONS 100 ( 
	PARTITION p0 VALUES LESS THAN (735017)
)


select ID_DATE, ID_LINEITEM, ID_DOMAIN, ID_CR, ID_EXCHANGE, ID_QUARTER
	sum(num_clicks), sum(num_impressions), sum(num_conversions),  sum(num_bids), max(IMP_COST) 
	from ( select a.creative_id as ID_CR, b.* from adnetik.assignment a join fastetl.fast_domain b on a.id = b.ID_ASSIGNMENT where b.ID_CAMPAIGN=988) 
	as T group by ID_DATE, ID_LINEITEM, ID_CR, ID_EXCHANGE, ID_DOMAIN, ID_QUARTER order by ID_QUARTER;
	
select ID_DATE, ID_LINEITEM, ID_DOMAIN, ID_CR, ID_EXCHANGE , 
	sum(num_clicks), sum(num_impressions), sum(num_conversions),  sum(num_bids), sum(IMP_COST) 
	from ( select a.creative_id as ID_CR, b.* from adnetik.assignment a join fastetl.fast_domain b on a.id = b.ID_ASSIGNMENT where b.ID_CAMPAIGN=988) 
	as T group by ID_DATE, ID_LINEITEM, ID_CR, ID_EXCHANGE, ID_DOMAIN;	
	

/** cost in most recent quarter */
select sum(IMP_COST) from fast_general 
