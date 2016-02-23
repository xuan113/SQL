from query_launcher import query_launcher
import argparse

class create_allocation_full_ignite(query_launcher):
     
    query_filename = "create_allocations_full_ignite.sql"
         
    def launch(self, params):
        job_name = "Full allocation ignite {testid} {tenure_grouping} {allocation_start} {allocation_end}".format(**params)
        self.launch_hive_job(job_name, params)

class create_allocation_full_custom(query_launcher):
     
    query_filename = "create_allocations_full_custom.sql"
         
    def launch(self, params):
        job_name = "Full allocation custom {testid} {tenure_grouping} {allocation_start} {allocation_end}".format(**params)
        self.launch_hive_job(job_name, params)


class partiotionless_vhs_dump(query_launcher):
     
    query_filename = "partitionless_vhs_dump.sql"
         
    def launch(self, params):
        job_name = "VHS Dump {testid} {tenure_grouping} {allocation_start} {allocation_end}".format(**params)
        self.launch_hive_job(job_name, params)

class manual_dashboard_prep(object):
    '''
    Coordinates launching of individual queries
    '''
    def __init__(self, username):
        self.allocs_ignite = create_allocation_full_ignite(username)
        self.allocs_custom = create_allocation_full_custom(username)
        self.vhs = partiotionless_vhs_dump(username)
        self.running = []
        
    def launch(self, params, run_vhs):
        run_ignite = params['tenure_grouping'] in (7,35,63,98,126) and params['latest_activity'] == 0
        if run_ignite:
            launcher = self.allocs_ignite
        else:
            launcher = self.allocs_custom
        print launcher
        launcher.launch(params)
        self.running.append(launcher)
        if run_vhs:
            self.vhs.launch(params)
            self.running.append(self.vhs)
    
    def wait(self):
        for qry in self.running:
            qry.wait()
    
def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-userdb", type=str, required=True)
    parser.add_argument("-testid", type=int, required=True)
    parser.add_argument("-tenure_grouping", type=int, required=True)
    parser.add_argument("-allocation_start", type=int, required=True)
    parser.add_argument("-allocation_end", type=int, required=True)
    parser.add_argument("-run_vhs_dump", type=int, required = False, default = 0, choices = [0,1])
    parser.add_argument("-latest_activity", type=int, required = False, default = 0)
    return parser

def main():
    parser = create_parser()
    args = parser.parse_args()
    params = vars(args)
    run_vhs_dump = params.pop('run_vhs_dump')
    launcher = manual_dashboard_prep(params['userdb'])
    launcher.launch(params, run_vhs_dump)
    launcher.wait()

if __name__ == '__main__':
    """
    to use:
    python manual_dashboard_prep.py -userdb mramm -testid 5846 -tenure_grouping 35 -allocation_start 20150116 -allocation_end 20150319
    python manual_dashboard_prep.py -userdb mramm -testid 5846 -tenure_grouping 35 -allocation_start 20150116 -allocation_end 20150319 -run_vhs_dump 1
    python manual_dashboard_prep.py -userdb mramm -testid 5846 -tenure_grouping 180 -allocation_start 20150116 -allocation_end 20150319
    python manual_dashboard_prep.py -userdb mramm -testid 5846 -tenure_grouping 180 -allocation_start 20150116 -allocation_end 20150319 -latest_activity 20150515
    """
    main()