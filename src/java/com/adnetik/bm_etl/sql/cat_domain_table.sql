
DROP TABLE IF EXISTS test_cat_domain;

CREATE TABLE test_cat_domain (
	name			varchar(200) PRIMARY KEY,
	id 			int AUTO_INCREMENT UNIQUE KEY,
	add_date		date
) ENGINE = 'InnoDB';
