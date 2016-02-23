USE alexl;
DROP TABLE IF EXISTS alexl.CS_call_${test_id};
CREATE TABLE IF NOT EXISTS alexl.CS_call_${test_id} AS

SELECT 
	a.test_id,
	a.test_name,
	a.test_cell_nbr,
	a.test_cell_name,
	COALESCE(a.signup_subregion, 'All') AS filter_region,
	COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
	SUM(IF(b.account_id IS NOT NULL, 1, 0)) AS tab1_99CS_call99CS_call_cnt,
	COUNT(a.account_id) AS number_of_allocations
FROM dse.exp_allocation_denorm_f a
LEFT OUTER JOIN(
	SELECT DISTINCT c.account_id 
	FROM 
	dse.cs_contact_f c,
	dse.cs_transfer_type_d trt
	WHERE c.escalation_code NOT IN ('G-Escalation', 'SC-Consult','SC-Escalation')
	AND c.transfer_type_id = trt.transfer_type_id
	AND trt.major_transfer_type_desc NOT IN ('TRANSFER_OUT')
	AND c.contact_channel_id IN ('Phone', 'Chat','voip')
	AND dateint >= ${activity_start}
	AND dateint <= ${activity_end}
	AND c.answered_cnt > 0
	AND c.account_id > 0
) b
ON a.account_id = b.account_id
WHERE a.test_id = ${test_id}
AND a.allocation_region_date BETWEEN ${activity_start} AND ${activity_end}
GROUP BY a.test_id, a.test_name, a.test_cell_nbr, a.test_cell_name, a.signup_subregion, a.allocation_type
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL