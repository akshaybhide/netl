
CREATE TABLE pxprm_base
(
	`nfsdate` date NOT NULL,
	`pixfire_id` int unsigned NOT NULL,
	`pix_id` int NOT NULL,
	`wtp_id` char(40) NOT NULL,
	PRIMARY KEY (nfsdate, pixfire_id)
);

CREATE INDEX pix_base_idx ON pxprm_base (pix_id);

CREATE TABLE pxprm_keyv
(
	`nfsdate` date NOT NULL,
	`pixfire_id` int unsigned NOT NULL,
	`param_key` varchar(40) NOT NULL,
	`param_val` varchar(80) NOT NULL,
	FOREIGN KEY (nfsdate, pixfire_id) REFERENCES pxprm_base (nfsdate, pixfire_id),	
	PRIMARY KEY (nfsdate, pixfire_id, param_key)
);

CREATE INDEX pix_param_key_idx ON pxprm_keyv (param_key);

CREATE VIEW v__combined AS SELECT BS.*, KV.param_key, KV.param_val FROM pxprm_keyv KV left join pxprm_base BS 
	ON KV.nfsdate = BS.nfsdate AND KV.pixfire_id = BS.pixfire_id;
