CREATE TABLE `control_table` 
(
	fpath varchar(1000) NOT NULL,
	fdate date NOT NULL,
	completed datetime DEFAULT NULL,
	started datetime NOT NULL
)

CREATE INDEX pathidx ON control_table (fpath(700));

