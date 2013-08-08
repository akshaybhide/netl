
SELECT count(*), sum(ADG.num_impressions), ADG.id_date, REG.name
	from ad_general ADG left join cat_region REG on ADG.id_region = REG.id
	where ADG.id_campaign = 2038
	and id_date > '2012-06-22'
	group by ADG.id_date, REG.name;
	
SELECT count(*), sum(ADG.num_impressions), ADG.id_date, REG.name
	from fast_general ADG left join cat_region REG on ADG.id_region = REG.id
	where ADG.id_campaign = 1929
	and id_date > 20120625
	group by ADG.id_date, REG.name;	
	
SELECT id,name FROM adnetik.campaign WHERE name LIKE 'UK_%';
