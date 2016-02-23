USE alexl;
DROP TABLE IF EXISTS alexl.phone_collection_${test_id};
CREATE TABLE IF NOT EXISTS alexl.phone_collection_${test_id} AS
SELECT 
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
  COALESCE(a.signup_subregion, 'All') AS filter_subregion,
  SUM(CASE WHEN b.phone_verified = 'true' THEN 1 ELSE 0 END) AS tab1_99Phone99phone_verified_rate,
  SUM(CASE WHEN b.phone_verified = 'true' THEN 1 ELSE 0 END) AS tab1_99Phone99phone_verified_rate_1,
  -- added the duplicated since the code only takes >= 2 metrics
  COUNT(a.account_id) AS number_of_allocations
FROM alexl.allocation_${test_id} a
JOIN dse.account_d b
ON b.account_id = a.account_id
GROUP BY
   a.test_id,
   a.test_name,
   a.test_cell_nbr,
   a.test_cell_name,
   a.allocation_type,
   a.signup_subregion
WITH CUBE
HAVING a.test_id IS NOT NULL
   AND a.test_name IS NOT NULL
   AND a.test_cell_nbr IS NOT NULL
   AND a.test_cell_name IS NOT NULL;