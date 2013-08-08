
CREATE TABLE exc2iab_map 
(
	excname varchar(20) NOT NULL,
	exc_id int NOT NULL,
	iab_id int NOT NULL
	/* PRIMARY KEY (excname, exc_id) */
	/* FOREIGN KEY */
)

CREATE TABLE seginfo 
(
	id int NOT NULL,
	segname varchar(50) NOT NULL,
	PRIMARY KEY(id)
)
