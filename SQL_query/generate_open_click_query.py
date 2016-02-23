# To generate query for 6741, use the command
#   python generate_open_click_query.py --test_id=6741 --start_date='2015-10-27' --event_id='1542,1967,1987'
# 

import os
import argparse

parser = argparse.ArgumentParser(description='Generate send-open-click summary.')
parser.add_argument('--test_id', help='test id')
parser.add_argument('--start_date', help='start date')
parser.add_argument('--event_id', help='event id')

args = parser.parse_args()

# Create experiment directory

# Create shiny_dashboard directory
if not os.path.exists('shiny_dashboard'):
    os.mkdir('shiny_dashboard')

# Read query template
with open('query_templates/template_send_open_click_teradata.sql') as file:
    query = file.read()

query = query.replace('${test_id}', args.test_id)
query = query.replace('${start_date}', args.start_date)
query = query.replace('${event_id}', args.event_id)

# Create output file
queryFile = 'send_open_click_teradata_' + args.test_id + '.sql'
with open(queryFile, "w") as output:
    output.write(query)


# Create ui.R file
with open('shiny_templates/ui.R') as file:
    shinyUI = file.read()
shinyUI = shinyUI.replace('${test_id}', args.test_id)
with open('shiny_dashboard/ui.R', "w") as output:
    output.write(shinyUI)

# Create server.R file
with open('shiny_templates/server.R') as file:
    shinyServer = file.read()
shinyServer = shinyServer.replace('${test_id}', args.test_id)
with open('shiny_dashboard/server.R', "w") as output:
    output.write(shinyServer)

