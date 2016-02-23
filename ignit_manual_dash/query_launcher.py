import os
import kragle as kg
import datetime
import re

class query_launcher(object):
    
    '''
    filename where the query is stored, should be overwritten by subclass
    '''
    query_filename = None
    
    def __init__(self, username):
        kg.dj.username(username)
        self.username = username
        self.query, self.lastmodtime = self._load_query()
        self.running_jobs = []
        self.output_uris = []
    
    def _load_query(self):
        if self.query_filename is None:
            raise Exception("Please provide the filename of the query")
        script_dir = os.path.dirname(__file__) #absolute dir the script is in
        rel_path = "{}".format(self.query_filename) #relative path from the script file
        abs_file_path = os.path.join(script_dir, rel_path)
        with open(abs_file_path, 'r') as f:
            query = f.read()
            lastmodtime = os.path.getmtime(abs_file_path)
            lastmodtime = datetime.datetime.fromtimestamp(lastmodtime)
        return query, lastmodtime

    def get_last_mod_time(self):
        return self.lastmodtime

    def extract_query_parameters(self):
        required = re.findall('{(\w+)}', self.query)
        #remove duplicates
        required = set(required)
        return required

    def extract_looper_parameters(self):
        required = self.extract_query_parameters()
        required.remove('dateint')
        return required
    
    def get_dateint(self, add = 0):
        today = datetime.date.today()
        day = today + datetime.timedelta(days = add)
        dateint = day.strftime('%Y%m%d')
        dateint = int(dateint)
        return dateint
    
    def current_dateint(self):
        return self.get_dateint(add = 0)
    
    def launch_hive_job(self, job_name, params):
        provided_set = set(params.keys())
        required_set = self.extract_query_parameters()
        if len(required_set - provided_set):
            raise Exception("Some parameters not provided {}".format(required_set - provided_set))
        rj = kg.dj.HiveJob().job_name(job_name).script(self.query).parameters(**params).execute()
        self.output_uris.append(rj.output_uri)
        self.running_jobs.append(rj.job_id)

    def dateint_to_dt(self, dateint):
        return datetime.datetime.strptime(str(dateint), '%Y%m%d')

    def dt_to_dateint(self, dt):
        return int(dt.strftime('%Y%m%d'))

    def looper_parameters(self, fixed_parameters, start_dateint, end_dateint):
        '''
        iterates over dates to produce parameters for the looper job
        '''
        if not 'dateint' in self.extract_query_parameters():
            raise Exception('Can not loop over dateint')
        current_date = self.dateint_to_dt(start_dateint)
        end_date = self.dateint_to_dt(end_dateint)
        looper_params = []
        while current_date <= end_date:
            fixed_parameters['dateint'] = self.dt_to_dateint(current_date)
            #check that all parametesr are provided
            provided_set = set(fixed_parameters.keys())
            required_set = self.extract_query_parameters()
            if len(required_set - provided_set):
                raise Exception("Some parameters not provided {}".format(required_set - provided_set))
            #output is in the form -d dateint=20141217 -d userdb=mramm -d testid=5551
            iteration_parameters = ' '.join(['-d {0}={1}'.format(k,v) for k,v in fixed_parameters.iteritems()])
            looper_params.append(iteration_parameters)
            current_date += datetime.timedelta(days = 1)
        return looper_params

    def launch_looper_job(self, job_name, fixed_parameters, start_dateint, stop_dateint, parallel_jobs):
        looper_params = self.looper_parameters(fixed_parameters, start_dateint, stop_dateint)
        looper_job = kg.looper.HiveLooperJob()\
        .job_name(job_name)\
        .username(self.username)\
        .failure_handling('retry')\
        .parameters(looper_params)\
        .parallel(parallel_jobs)\
        .script(self.query)\
        .execute()
        return looper_job
    
    def wait(self):
        while len(self.running_jobs):
            job_id = self.running_jobs.pop()
            rj = kg.dj.reattach_job(job_id)
            rj.wait()
            if not rj.status == 'SUCCEEDED':
                print rj.status
                raise Exception(rj.stderr())
    
    def launch(self):
        '''
        should be implemented by the subclass
        '''
        pass

    def backfill(self):
        '''
        should be implemented by the subclass if appropriate
        '''
        pass