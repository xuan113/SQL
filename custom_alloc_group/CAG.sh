#!/bin/bash

set -u #check that all the shell parameters/variables are initialized.

TestId=$1
GroupName=$2
QueryFile=$3

echo "------------------------------------"
echo "Validating number of groups for test ${TestId} ..."
echo "------------------------------------"
QueryCount3=`presto --execute "select count(distinct group_id) from etl.ignite_custom_filters_d where test_id = ${TestId};"`
QueryCountClean3=${QueryCount3//\"/}

if [ ${QueryCountClean3} -gt 3 ]; then
  echo "*******************************************************************************************"
  echo "Max number of possible groups per test is 3, this test allready has 3 groups."
  echo "Donating \$10 to the CSD group will remove this limit in 5 days, \$50 will remove the limit in 3 days, and \$100 will remove the limit in 1 day, get you a hand shake, and %5 equity on the Brooklyn Bridge."
  echo "*******************************************************************************************"
  printf "User: "$USER"\nToo many groups for test ${TestId}" | mail -s "Custom Allocation Groups - failed" 'lsagi@netflix.com,msanver@netflix.com,svelayutham@netflix.com'
  exit
fi

# echo "------------------------------------"
# echo "Validating group name uniqueness ..."
# echo "------------------------------------"
# QueryCount=`presto --execute "select count(*) from etl.ignite_custom_filters_d where test_id = ${TestId} and group_name = '${GroupName}';"`
# QueryCountClean=${QueryCount//\"/}

# if [ ${QueryCountClean} != 0 ]; then
#   echo "*******************************************************************************************"
#   echo "Group \"${GroupName}\" for test ${TestId} allready exists and has ${QueryCountClean} accounts in it."
#   echo "*******************************************************************************************"
#   printf "User: "$USER"\nGroup Name Exists: \"${GroupName}\"" | mail -s "Custom Allocation Groups - failed" 'lsagi@netflix.com,msanver@netflix.com,svelayutham@netflix.com'
#   exit
# fi

QueryFull=""
while read Query; do
  QueryFull=${QueryFull}" "${Query}
done < ${QueryFile}
QueryFull=${QueryFull}" "${Query}
QueryFull=${QueryFull/\account_id/account_id,"'"${GroupName}"'"}

echo "----------------------------------------"
echo "Validating 20,000,000 accounts limit ..."
echo "----------------------------------------"
QueryCount2=`presto --execute "select count(*) from (${QueryFull});"`
QueryCountClean2=${QueryCount2//\"/}

if [ ${QueryCountClean2} -gt 20000000 ]; then
  echo "*******************************************************************************************"
  echo "You are trying to insert ${QueryCountClean2//\"/} accounts, 20,000,000 is the limit."
  echo "Please refine your query and try again"
  echo "*******************************************************************************************"
  printf "User: "$USER"\nToo many account: ${QueryCountClean2//\"/}\n${QueryFull}" | mail -s "Custom Allocation Groups - failed" 'lsagi@netflix.com,msanver@netflix.com,svelayutham@netflix.com'
  exit
fi

# echo "----------------------------------------"
# echo "Dropping group \"${GroupName}\" for test ${TestId} if it already exists..."
# echo "----------------------------------------"

# GroupId=`presto --execute "select distinct group_id from etl.ignite_custom_filters_d where test_id = ${TestId} and group_name = '${GroupName}'"`
# GroupIdClean=${GroupId//\"/}

# echo ${GroupIdClean}
# exit

# if [ ${GroupIdClean} ]; then
#   echo "dropping"
#   zztop=`prodhive -e "use etl; alter table ignite_custom_filters_d drop partition (test_id=${TestId}, group_id=${GroupIdClean}) "`
# fi

echo "----------------------------------------"
echo "Creating group \"${GroupName}\" for test ${TestId} ..."
echo "----------------------------------------"

IsExists=`presto --execute "select distinct group_id from etl.ignite_custom_filters_d where test_id = ${TestId} and group_name='${GroupName}';"`
IsExistsClean=${IsExists//\"/}

if [ ${IsExistsClean} -gt 0 ]; then
  echo "******************************************************"
  echo "******************************************************"
  echo -n "Group ${GroupName} for test ${TestId} already exists, are you sure you want to overwrite it? (y/n): "
  read IsOverwrite
  echo "******************************************************"
  echo "******************************************************"
  if [ ${IsOverwrite} != "y" ]; then
    exit
  fi
fi

if [ ${IsExistsClean} -gt 0 ] && [ ${IsOverwrite} == "y" ]; then
  NewId=${IsExists//\"/}
else
  GetMaxId=`presto --execute "select max(group_id) from etl.ignite_custom_filters_d where test_id >= 0 and group_id >= 0;"`
  MaxId=${GetMaxId//\"/}
  NewId=`expr $MaxId + 1` 
  echo ${NewId}
fi

zztop=`prodhive -e "insert overwrite table etl.ignite_custom_filters_d partition (test_id=${TestId}, group_id=${NewId}) ${QueryFull};"`

printf "User: "$USER"\ninsert overwrite table etl.ignite_custom_filters_d partition (test_id=${TestId}, group_id=${NewId}) ${QueryFull};" | mail -s "Custom Allocation Groups - activity" 'lsagi@netflix.com,msanver@netflix.com,svelayutham@netflix.com'

exit


