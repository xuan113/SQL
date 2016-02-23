#!/usr/bin/python2.7
import argparse
import subprocess
import sys
import locale
import smtplib
import getpass
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def eMail(User, Status, TestId, CustomFilterId, GroupName, Query):
	From = "svelayutham@netflix.com"
	To = "lsagi@netflix.com;msanver@netflix.com;svelayutham@netflix.com"

	Msg = MIMEMultipart('alternative')
	Msg['Subject'] = "MM Custom Allocation Groups - {Status}".format(Status = Status)
	Msg['From'] = From
	Msg['To'] = To

	HTML = """\
	<html>
	  <head>
        <style>
		  table {{border-collapse: collapse; width: 80%;}}
          table, td, th {{border: 1px solid black;}}
          th {{width: 100px; text-align: left}}
	    </style>
	  </head>
	  <body>
	    <table>
	      <tr>
	        <th><b>User</b> </th>
	        <td>{User}</td>
	      </tr>
	      <tr>
	        <th><b>Group info</b></th>
	        <td>Test = {TestId}, Custom Filter ID = {CustomFilterId}, Group Name = {GroupName}</td>
	      </tr>
	      <tr>
	        <th><b>User Query</b></th>
	        <td>{Query}</td>
	      </tr>
	      <tr>
	        <th><b>Status:</b></th>
	        <td>{Status}</td>
	      </tr>
	    </table>
	    </p>
	  </body>
	</html>
	""".format(User = User, TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName , Status = Status, Query = Query)

	Part = MIMEText(HTML, 'html')
	Msg.attach(Part)
	s = smtplib.SMTP('localhost')
	s.sendmail(From, To, Msg.as_string())
	s.quit()
	exit()

def IsExists(TestId, CustomFilterId, GroupName, QueryFile, UC4):
    """Validate that the users wants to overwrite the group if exists."""
    Groups = subprocess.check_output('presto --execute "select count(*) from etl.ignite_mm_custom_filters_d where test_id = {TestId} and custom_filter_id = {CustomFilterId} and group_name = \'{GroupName}\';" 2>/dev/null'.format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName ), shell=True)
    Groups = int(Groups.strip('" \n'))

    if Groups <= 0 and QueryFile == "d":
        print "Group \"{GroupName}\" for test {TestId}, CustomFilterId {CustomFilterId} was not found...it's time get your act together and make sure you have the correct test_id, filter_id, group_name.".format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName ) 
        exit()   

    if Groups > 0 and UC4 == "y":
        Response = "o"
        return Response

    elif Groups > 0:

         Response = raw_input("Group \"{GroupName}\" for test {TestId}, CustomFilterId {CustomFilterId} already exists, what do you want to do? (d = drop, o = overwrite, x = do nothing) ".format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName ))

         if Response == "x":
            exit()
         elif Response == "o":
            Response_2 = raw_input("Group \"{GroupName}\" for test {TestId}, CustomFilterId {CustomFilterId} already exists, are you sure you want to overwrite it? (y/n) ".format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName ))
            if Response_2 == "y": 
                return Response
            else:
                exit()
         elif Response == "d":
            Response_2 = raw_input("Group \"{GroupName}\" for test {TestId}, CustomFilterId {CustomFilterId} already exists, are you sure you want to delete it? (y/n) ".format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName ))
            if Response_2 == "y": 
                return Response
            else:
                exit()
         else:
            print "Your response was not undershood, please try again."
            eMail(getpass.getuser(), "Failed", args.TestId, args.CustomFilterId, args.GroupName, "your response was not undershood, please try again.")   
            exit()          

    else:
        Groups = GetGroupsPerTest(TestId, CustomFilterId)
        if Groups > 3:
           print "Test {TestId}, CustomFilterId {CustomFilterId} has {Groups} groups already, the current limit is 3 groups per test and custom filter.".format(TestId = TestId, CustomFilterId = CustomFilterId, Groups = Groups)
           eMail(getpass.getuser(), "Failed groups per test limit", TestId, CustomFilterId, GroupName, "", "")
           exit()
        else:
           return "c"

def GetQuery(QueryFile):
    with open (QueryFile, "r") as QFile:
        Query = QFile.read().replace('\n', '')
    return Query.lower()

def IsRightSize(Query):
    """Validate that there are no more than 20,000,000 to be inserted"""
    Count = subprocess.check_output('presto --execute "select count(*) from ({Query});" 2>/dev/null'.format(Query = Query), shell=True)
    Count = int(Count.strip('" \n'))
    
    if Count > 20000000:
        Count = "{0:n}".format(Count)
        print "You are trying to create a group with {Count} accounts, the limit is 20,000,000. Please revise your query and try again.".format(Count = Count)
        eMail(getpass.getuser(), "Failed 20,000,000 limit", "", "", "", "", Query)
        exit()

def DupAcctInGrpFilter(TestId, CustomFilterId, GroupName):
    """Validate that the accounts in the new group are not in another group in the same filter id"""
    Count = subprocess.check_output('presto --execute "select count(*) from (select account_id from etl.ignite_mm_custom_filters_d where test_id = {TestId} and custom_filter_id = {CustomFilterId} and group_name != \'{GroupName}\') a join (select account_id from etl.ignite_mm_custom_filters_d where test_id = {TestId} and custom_filter_id = {CustomFilterId} and group_name = \'{GroupName}\') b on (a.account_id=b.account_id);" 2>/dev/null'.format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName), shell=True)
    Count = int(Count.strip('" \n'))
    
    if Count > 0:
        Count = "{0:n}".format(Count)
        print "Dropping group because you are trying to create it with accounts that are in another group in the same filter id, try again with a different query."
        DropGroup(TestId, CustomFilterId, GroupName)
        print "Group \"{GroupName}\" for test {TestId} and CustomFilterId {CustomFilterId} was dropped!".format(TestId = TestId, GroupName = GroupName, CustomFilterId = CustomFilterId )
        exit()

def InsertGroup (TestId, CustomFilterId, GroupName, User, CreateTS, Query): 
    """Creating the group"""
    Query = Query.replace("account_id", "distinct account_id, '{User}', '{CreateTS}'".format(User = User, CreateTS = CreateTS))
    Insert = subprocess.check_output('prodhive -e "set mapred.reduce.tasks=1; insert overwrite table etl.ignite_mm_custom_filters_d partition (test_id={TestId}, custom_filter_id={CustomFilterId}, group_name= \'{GroupName}\') {Query};" 2>/dev/null'.format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName , Query = Query), shell=True)

def GetGroupsPerTest(TestId, CustomFilterId):    
    Count = subprocess.check_output('presto --execute "select count(distinct group_name) from etl.ignite_mm_custom_filters_d where test_id = {TestId} and custom_filter_id = {CustomFilterId};" 2>/dev/null'.format(TestId = TestId, CustomFilterId = CustomFilterId), shell=True)
    return int(Count.strip('" \n'))

def DropGroup (TestId, CustomFilterId, GroupName):
    """Droping a group"""
    Drop = subprocess.check_output('prodhive -e "use etl; alter table ignite_mm_custom_filters_d drop partition (test_id={TestId}, custom_filter_id={CustomFilterId}, group_name=\'{GroupName}\');" 2>/dev/null'.format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName ), shell=True)
    #print "hadoop fs -rm -r s3n://netflix-dataoven-prod-users/hive/warehouse/etl.db/ignite_mm_custom_filters_d/test_id={TestId}/custom_filter_id={CustomFilterId}/group_name=\'{GroupName}\'".format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName )
    DropS3 = subprocess.check_output('hadoop fs -rm -r s3n://netflix-dataoven-prod-users/hive/warehouse/etl.db/ignite_mm_custom_filters_d/test_id={TestId}/custom_filter_id={CustomFilterId}/group_name=\'{GroupName}\' 2>/dev/null'.format(TestId = TestId, CustomFilterId = CustomFilterId, GroupName = GroupName ), shell=True)
def main():
    locale.setlocale(locale.LC_ALL, '')
    parser = argparse.ArgumentParser()
    parser.add_argument("TestId", help="A numeric value for your test.", type=int)
    parser.add_argument("CustomFilterId", help="A numeric value for your custom_filter_id(1,2 or 3).", type=int)
    parser.add_argument("GroupName", help="A group name encapsulated by double-quotes.", type=str)
    parser.add_argument("QueryFile", help="A file that contains the query which generates a list of accounts.", type=str)
    parser.add_argument("UC4", help="enable group creation/update via UC4.", nargs='?', default="n", type=str )
    args = parser.parse_args()

    #print args.GroupName
    args.GroupName = args.GroupName.replace (" ", "_")
    #print args.GroupName

    if args.CustomFilterId == 1 or args.CustomFilterId == 2 or args.CustomFilterId == 3:
        print "Checking if group \"{GroupName}\" for test {TestId} and CustomFilterId {CustomFilterId} exists...".format(GroupName = args.GroupName, TestId = args.TestId, CustomFilterId = args.CustomFilterId)
        Action = IsExists(args.TestId, args.CustomFilterId, args.GroupName, args.QueryFile, args.UC4)
        #import pdb; pdb.set_trace()
        if Action == "c" or Action == "o":
            Query = GetQuery(args.QueryFile)
            print "Validating the size of your new group (must be no more than 20,000,000 accounts)..."
            IsRightSize(Query)                
            
            print "Creating/Overwriting group \"{GroupName}\" for test {TestId} and CustomFilterId {CustomFilterId}...".format(GroupName = args.GroupName, TestId = args.TestId, CustomFilterId = args.CustomFilterId)
            CreateTs = datetime.datetime.now().isoformat()
            InsertGroup(args.TestId, args.CustomFilterId, args.GroupName, getpass.getuser(), CreateTs, Query)
            print "Group \"{GroupName}\" for test {TestId} and CustomFilterId {CustomFilterId} was created!".format(GroupName = args.GroupName, TestId = args.TestId, CustomFilterId = args.CustomFilterId)
            
            #Check to see if the account in this group exists in another group in the same filter id
            DupAcctInGrpFilter(args.TestId, args.CustomFilterId, args.GroupName)
            eMail(getpass.getuser(), "Success", args.TestId, args.CustomFilterId, args.GroupName, Query)
        elif Action == "d":
            print "Dropping group {GroupName} for test {TestId} and CustomFilterId {CustomFilterId}.".format(GroupName = args.GroupName, TestId = args.TestId, CustomFilterId = args.CustomFilterId)
            Drop = DropGroup(args.TestId, args.CustomFilterId, args.GroupName)        
            eMail(getpass.getuser(), "Success", args.TestId, args.CustomFilterId, args.GroupName, Drop)

    else:
        print "Custom Filter ID can only be either: 1, 2 or 3 "
        exit()        

if __name__ == '__main__':
    main()