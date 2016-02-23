INSERT OVERWRITE TABLE etl.ignite_custom_filters_d
PARTITION (test_id=5715, group_id=1) 
SELECT account_id, 'saw_onramp'
FROM bratchev.onramp_qa_5715
WHERE saw_onramp = 1;

INSERT OVERWRITE TABLE etl.ignite_custom_filters_d
PARTITION (test_id=5715, group_id=2) 
SELECT account_id, 'saw_no_onramp'
FROM bratchev.onramp_qa_5715
WHERE saw_onramp = 0;