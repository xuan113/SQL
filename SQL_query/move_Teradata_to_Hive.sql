

	USE alexl;
	CREATE EXTERNAL TABLE alexl.DVD_all_metric_6666_20151109
	( account_id BIGINT
	, test_cell_nbr INT
	, has_send INT
	, has_open INT
	, has_click INT
  , is_unsub INT
	)
	STORED AS TEXTFILE
	LOCATION "s3n://netflix-dataoven-prod-users/alexl/6666_Dvd/allMetric_6666_20151109";
