
DELETE FROM fast_domain_r
 WHERE ID_DATE = CAST(@theday AS date);

INSERT INTO fast_domain__new
 SELECT
	ID_CAMPAIGN,
	ID_DATE,
	ID_HOUR,
	ID_LINEITEM,
	ID_CREATIVE,
	ID_EXCHANGE,
	ID_COUNTRY,
	ID_CURRCODE,
	ID_CONTENT,
	ID_UTW,
	ID_DOMAIN,
	NUM_BIDS,
	NUM_CLICKS,
	NUM_IMPRESSIONS,
	NUM_CONVERSIONS,
	NUM_CONV_POST_VIEW,
	NUM_CONV_POST_CLICK,
	IMP_COST,
	IMP_BID_AMOUNT,
	unassign_text_0,
	unassign_text_1,
	unassign_text_2,
	unassign_text_3,
	unassign_int_0,
	unassign_int_1,
	unassign_int_2,
	unassign_int_3,
	unassign_int_4,
	unassign_decimal,
	(CASE WHEN (NUM_CLICKS > 0 OR NUM_CONVERSIONS > 0) THEN 1 ELSE 0 END)
	      AS HAS_CC,
	FLOOR(RAND()*(100 - 1e-14)) AS RAND99,
	ENTRY_DATE,
	EXT_LINEITEM
   FROM fast_domain
  WHERE ID_DATE = date('2012-10-04')
    AND ID_CAMPAIGN > 0 AND ID_CAMPAIGN < 65536;

