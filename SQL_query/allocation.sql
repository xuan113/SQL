USE alexl;
DROP TABLE IF EXISTS alexl.allocation_${testid} ;
CREATE TABLE IF NOT EXISTS alexl.allocation_${testid} AS
SELECT 
	test_id, 
	test_name, 
	test_cell_nbr, 
	test_cell_name, 
	account_id, 
	signup_subregion, 
	allocation_type,
  allocation_region_date,
  subscrn_id,
  signup_region,
  allocation_utc_date,
  allocation_unix_ts
FROM dse.exp_allocation_denorm_f 
WHERE test_id = ${testid}
AND allocation_region_date BETWEEN ${allocation_start} AND ${allocation_end}
AND deallocation_region_date IS NULL;