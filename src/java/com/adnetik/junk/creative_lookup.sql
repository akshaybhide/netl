CREATE TABLE `__creative_lookup__reg` (
  id bigint(20) NOT NULL,
  ctv_id bigint(20) NOT NULL,
  PRIMARY KEY(id)
)


CREATE TABLE `__creative_lookup__appnexus` (
  id bigint(20) NOT NULL,
  ctv_id bigint(20) NOT NULL,
  PRIMARY KEY(id)
)


CREATE OR REPLACE VIEW v__creative_general AS SELECT FAST.*, REG.id as REG_CTV_ID, APPN.id as APPN_CTV_ID,
IF(FAST.ID_EXCHANGE = 5, APPN.id, REG.id) as FINAL_CTV_ID
FROM fast_general FAST  
LEFT JOIN __creative_lookup__reg REG ON FAST.id_assignment = REG.id  
LEFT JOIN __creative_lookup__appnexus APPN ON FAST.id_assignment = APPN.id ;

