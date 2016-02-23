-- Create a table in hive-get all column names and types right. Also choose a name for the table itself
-- just name replace in a text editor may work best.  useful for reading a tsv or csv file into a Hive table. 
use ${userdb};
drop table if exists ${userdb}.${table_name};
create external table ${userdb}.${table_name}(
    -- modify columns here
    account_id bigint,
    title_desc string

) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

-- SCP the tsv or csv table to your home directory on the query server
-- Now on the query server, load the data from your file into the created table
-- Important: The file should have the raw data without the column names. All coumns should be in the order they were created above.
LOAD DATA LOCAL INPATH '/home/${userdb}/${file_name}' OVERWRITE INTO TABLE ${userdb}.${table_name};