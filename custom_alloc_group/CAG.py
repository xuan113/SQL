#!/usr/bin/python2.7
import argparse
import subprocess
import sys
import locale
import smtplib
import getpass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def eMail(User, Status, TestId, GroupName, Query):
	From = "lsagi@netflix.com"
	To = "lsagi@netflix.com;msanver@netflix.com;svelayutham@netflix.com"

	Msg = MIMEMultipart('alternative')
	Msg['Subject'] = "Custom Allocation Groups - {Status}".format(Status = Status)
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
	        <td>Test = {TestId}, Group Name = {GroupName}</td>
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
	""".format(User = User, TestId = TestId, GroupName = GroupName, Status = Status, Query = Query)

	Part = MIMEText(HTML, 'html')
	Msg.attach(Part)
	s = smtplib.SMTP('localhost')
	s.sendmail(From, To, Msg.as_string())
	s.quit()
	exit()

def IsExists(TestId, GroupName, QueryFile):
    """Validate that the users wants to overwrite the group if exists."""
    Groups = subprocess.check_output('presto --execute "select count(*) from etl.ignite_custom_filters_d where test_id = {TestId} and group_name = \'{GroupName}\';" 2>/dev/null'.format(TestId = TestId, GroupName = GroupName), shell=True)
    Groups = int(Groups.strip('" \n'))

    if Groups <= 0 and QueryFile == "d":
        print "Group \"{GroupName}\" for test {TestId} was not found...it's time get your act together and make sure you have the correct test_id and group name.".format(TestId = TestId, GroupName = GroupName) 
        exit()   

    if Groups > 0:
         Response = raw_input("Group \"{GroupName}\" for test {TestId} allready exists, what do you want to do? (d = drop, o = overwrite, x = do nothing) ".format(TestId = TestId, GroupName = GroupName))

         if Response == "x":
            exit()
         elif Response == "o":
            Response_2 = raw_input("Group \"{GroupName}\" for test {TestId} allready exists, are you sure you want to overwrite it? (y/n) ".format(TestId = TestId, GroupName = GroupName))
            if Response_2 == "y": 
                return Response
            else:
                exit()
         elif Response == "d":
            Response_2 = raw_input("Group \"{GroupName}\" for test {TestId} allready exists, are you sure you want to delete it? (y/n) ".format(TestId = TestId, GroupName = GroupName))
            if Response_2 == "y": 
                return Response
            else:
                exit()
         else:
            print "Your response was not undershood, please try again."
            eMail(getpass.getuser(), "Failed", args.TestId, args.GroupName, "our response was not undershood, please try again.")   
            exit()          
    
    else:
        Groups = GetGroupsPerTest(TestId)
        if Groups >= 3:
           print "Test {TestId} has {Groups} groups allready, the current limit is 3 groups per test.".format(TestId = TestId, Groups = Groups)
           eMail(getpass.getuser(), "Failed groups per test limit", TestId, GroupName, "")
           exit()
        else:
           return "c"

def GetQuery(QueryFile):
    with open (QueryFile, "r") as QFile:
        Query = QFile.read().replace('\n', '')
    return Query

def IsRightSize(Query):
    """Validate that there are no more than 20,000,000 to be inserted"""
    Count = subprocess.check_output('presto --execute "select count(*) from ({Query});" 2>/dev/null'.format(Query = Query), shell=True)
    Count = int(Count.strip('" \n'))
    
    if Count > 20000000:
        Count = "{0:n}".format(Count)
        print "You are trying to create a group with {Count} accounts, the limit is 20,000,000. Please revise your query and try again.".format(Count = Count)
        eMail(getpass.getuser(), "Failed 20,000,000 limit", "", "", Query)
        exit()

def GetNewGroupId(TestId, GroupName):
    GroupId = subprocess.check_output('presto --execute "select distinct group_id from etl.ignite_custom_filters_d where test_id = {TestId} and group_name = \'{GroupName}\';" 2>/dev/null'.format(TestId = TestId, GroupName = GroupName), shell=True)    
    
    if GroupId:
        return int(GroupId.strip('" \n'))
    else:
        GroupId = subprocess.check_output('presto --execute "select max(group_id) + 1 from etl.ignite_custom_filters_d;" 2>/dev/null', shell=True)    
        return int(GroupId.strip('" \n'))

def InsertGroup (TestId, GroupName, Query): 
    """Creating the group"""
    GroupId = GetNewGroupId(TestId, GroupName)
    Query = Query.replace("account_id", "account_id, '{GroupName}'".format(GroupName = GroupName))
    print "insert overwrite table etl.ignite_custom_filters_d partition (test_id={TestId}, group_id={GroupId}) {Query};".format(TestId = TestId, GroupId = GroupId, Query = Query)
    Insert = subprocess.check_output('prodhive -e "insert overwrite table etl.ignite_custom_filters_d partition (test_id={TestId}, group_id={GroupId}) {Query};" 2>/dev/null'.format(TestId = TestId, GroupId = GroupId, Query = Query), shell=True)

def GetGroupsPerTest(TestId):    
    Count = subprocess.check_output('presto --execute "select count(distinct group_id) from etl.ignite_custom_filters_d where test_id = {TestId};" 2>/dev/null'.format(TestId = TestId), shell=True)
    return int(Count.strip('" \n'))

def DropGroup (TestId, GroupName):
    """Droping a group"""
    GroupId = GetNewGroupId(TestId, GroupName)
    Drop = subprocess.check_output('prodhive -e "use etl; alter table ignite_custom_filters_d drop partition (test_id={TestId}, group_id={GroupId});" 2>/dev/null'.format(TestId = TestId, GroupId = GroupId), shell=True)

def main():
    locale.setlocale(locale.LC_ALL, '')
    parser = argparse.ArgumentParser()
    parser.add_argument("TestId", help="A numeric value for your test.", type=int)
    parser.add_argument("GroupName", help="A group name encapsulated by double-quotes.", type=str)
    parser.add_argument("QueryFile", help="A file that contains the query which generates a list of acocounts.", type=str)
    args = parser.parse_args()

    print "Checking if group \"{GroupName}\" for test {TestId} exists...".format(GroupName = args.GroupName, TestId = args.TestId)
    Action = IsExists(args.TestId, args.GroupName, args.QueryFile)
    
    if Action == "c" or Action == "o":
        Query = GetQuery(args.QueryFile)
        print "Validating the size of your new group (must be no more than 20,000,000 accounts)..."
        IsRightSize(Query)
        print "Creating/Overwriting group \"{GroupName}\" for test {TestId}...".format(GroupName = args.GroupName, TestId = args.TestId)
        InsertGroup(args.TestId, args.GroupName, Query)
        print "Group {GroupName} for test {TestId} was created!".format(GroupName = args.GroupName, TestId = args.TestId)
        eMail(getpass.getuser(), "Success", args.TestId, args.GroupName, Query)
    elif Action == "d":
        print "Dropping group {GroupName} for test {TestId}.".format(GroupName = args.GroupName, TestId = args.TestId)
        Drop = DropGroup(args.TestId, args.GroupName)        
        eMail(getpass.getuser(), "Success", args.TestId, args.GroupName, Drop)

if __name__ == '__main__':
    main()
