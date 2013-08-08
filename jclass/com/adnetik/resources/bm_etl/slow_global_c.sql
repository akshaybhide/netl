
SET @theday = XX_MAGIC_DAYCODE_XX;

DELETE FROM slow_global_c 
	WHERE TheDate = CAST(@theday AS date);

INSERT INTO slow_global_c
 SELECT augc.user_group_id AS user_group_id,
	dd.TheDate AS TheDate,
	acv.office_id AS office_id,
	fg.ID_COUNTRY AS ID_COUNTRY,
	SUM(fg.NUM_BIDS) AS NUM_BIDS,
	SUM(fg.NUM_CLICKS) AS NUM_CLICKS,
	SUM(fg.NUM_IMPRESSIONS) AS NUM_IMPRESSIONS,
	SUM(fg.NUM_CONVERSIONS) AS NUM_CONVERSIONS,
	SUM(fg.NUM_CONV_POST_VIEW) AS NUM_CONV_POST_VIEW,
	SUM(fg.NUM_CONV_POST_CLICK) AS NUM_CONV_POST_CLICK,
	SUM(fg.IMP_COST/der.ONE_USD_BUYS) AS IMP_COST,
	SUM(fg.IMP_BID_AMOUNT/der.ONE_USD_BUYS) AS IMP_BID_AMOUNT
   FROM ( ( ( adnetik.campaign ac
	      LEFT OUTER JOIN adnetik.user_group_client augc
	      ON ac.client_id = augc.client_id )
	    JOIN fast_general fg
	    ON ac.id = fg.ID_CAMPAIGN )
	  LEFT OUTER JOIN adnetik.client acv
	  ON ac.client_id = acv.id )
	JOIN dim_Date dd
	JOIN ( cat_currcode cc
	       JOIN daily_exchange_rates der
	       ON der.CURR_CODE = cc.code )
	  ON dd.PK_Date = fg.ID_DATE
	 AND dd.TheDate = CAST(@theday AS date)
	 AND der.ID_DATE = dd.TheDate
	 AND cc.id = fg.ID_CURRCODE
  GROUP BY user_group_id, TheDate, ID_COUNTRY, office_id;

