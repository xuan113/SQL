import kragle as kg
import os
import textwrap

class manual_dashboard_generator(object):

    template_filename = 'query_template.sql'

    def __init__(self, username):
        kg.dj.username(username)
        self.username = username
        self.template = self._load_file(self.template_filename)
        self.query = None

    def _load_file(self, filename):
        script_dir = os.path.dirname(__file__) #absolute dir the script is in
        rel_path = "{}".format(filename) #relative path from the script file
        abs_file_path = os.path.join(script_dir, rel_path)
        with open(abs_file_path, 'r') as f:
            file_content = f.read()
        return file_content

    def set_filters(self, allocation_filter_list, activity_filter_list, cell_mapping = None):
        self.allocation_filter_list = allocation_filter_list
        self.activity_filter_list = activity_filter_list
        self.cell_mapping = cell_mapping

    def generate_query(self, test_params):
        allocation_filter_list = self.allocation_filter_list
        activity_filter_list = self.activity_filter_list
        cell_mapping = self.cell_mapping
        
        def combine_cells(mapping = None):
            if mapping is None:
                test_cell_sel = "test_cell_nbr"
                name_cell_sel = "test_cell_name"
            else:
                test_cell_sel = "CASE "
                for k,v in cell_mapping.iteritems():
                    test_cell_sel += "WHEN test_cell_nbr IN {} THEN {} ".format(k,v)
                test_cell_sel += "ELSE test_cell_nbr END AS test_cell_nbr"
                
                name_cell_sel = "CASE "
                for k,v in cell_mapping.iteritems():
                    name_cell_sel += "WHEN test_cell_nbr IN {0} THEN 'Combining {0}' ".format(k)
                name_cell_sel += "ELSE test_cell_name END AS test_cell_name"
            return test_cell_sel, name_cell_sel
    
        def to_filter_name(name, prepend = ''):
            return "{prepend}filter_{name}".format(prepend = prepend, name = name)

        def to_outer_join(query, name):
            query = textwrap.dedent(query).strip()
            return """
        LEFT OUTER JOIN (
        {query}
        ) {name}
        ON allocs.account_id = {name}.account_id
        """.format(query = query, name = name).strip()

        def to_alloc_case(name):
            return "CASE WHEN {name}.account_id IS NOT NULL THEN 1 ELSE 0 END".format(name = name)

        def to_alloc_coalesce(name):
            case = to_alloc_case(name)
            filtername = to_filter_name(name)
            return "COALESCE({case}, 'All Allocations') AS {filtername}".format(case = case, filtername = filtername)

        def to_active_case(name):
            return "CASE WHEN vhs.{name} IS NULL THEN 'No Streaming' ELSE vhs.{name} END".format(name = name)

        def to_active_case_as(name):
            case = to_active_case(name)
            filtername = to_filter_name(name)
            return "{case} AS {filtername}".format(case = case, filtername = filtername)

        def to_active_coalesce(case, name):
            return "COALESCE({case}, 'All Streaming') AS {name}".format(case = case, name = name)

        def and_active_case(name):
            case = to_active_case(name)
            return "AND {case} IS NOT NULL".format(case = case)    

        def active_in_all(name):
            filtername = to_filter_name(name)
            return "{filtername} IN ('All Streaming', 'No Streaming')".format(filtername = filtername)

        def stream_ret_join(name):
            filtername = to_filter_name(name)
            return "AND stream.{filtername} = ret.{filtername}".format(filtername = filtername)

        test_cell_sel, name_cell_sel = combine_cells(cell_mapping)

        alloc_joins = '\n'.join((to_outer_join(*t)) for t in allocation_filter_list)
        allocation_filters_coalesce = ',\n'.join(to_alloc_coalesce(name) for (query,name) in allocation_filter_list) + ','
        allocation_case = ',\n'.join(to_alloc_case(name) for (query,name) in allocation_filter_list) + ','

        activity_filters = ',\n'.join(to_active_case_as(name) for query,name in activity_filter_list) + ','
        activity_coalesce = ',\n'.join(to_active_coalesce(query, name) for query,name in activity_filter_list) + ','
        activity_case = ',\n'.join(query for query,name in activity_filter_list)
        activity_case_null = ',\n'.join(to_active_case(name) for query,name in activity_filter_list)
        and_activty_filters = '\n'.join(and_active_case(name) for query,name in activity_filter_list)

        allocation_filter_names = ',\n'.join(to_filter_name(name) for query,name in allocation_filter_list) + ','
        activity_filter_names = ',\n'.join(to_filter_name(name) for query,name in activity_filter_list) + ','

        stream_allocation_filter_names = ',\n'.join(to_filter_name(name, 'stream.') for query,name in allocation_filter_list) + ','
        stream_activity_filter_names = ',\n'.join(to_filter_name(name,'stream.') for query,name in activity_filter_list) + ','

        allocation_filter_names_nc = ',\n'.join(to_filter_name(name) for query,name in allocation_filter_list)
        activity_filter_names_in_all_or_no = "\nAND ".join(active_in_all(name) for query,name in activity_filter_list)
        allocation_filter_join = '\n'.join(stream_ret_join(name) for query,name in allocation_filter_list)

        self.query = self.template.format(
            userdb = test_params['userdb'],
            testid = test_params['testid'],
            tenure_grouping = test_params['tenure_grouping'],

            allocation_filters_coalesce = allocation_filters_coalesce,
            activity_filters = activity_filters,
            alloc_joins = alloc_joins,
            activity_coalesce = activity_coalesce,
            activity_case = activity_case,
            allocation_case = allocation_case,
            and_activty_filters = and_activty_filters,
            activity_case_null = activity_case_null,
            
            allocation_filter_names = allocation_filter_names,
            activity_filter_names = activity_filter_names,
            activity_filter_names_in_all_or_no = activity_filter_names_in_all_or_no,
            allocation_filter_names_nc = allocation_filter_names_nc,
            allocation_filter_join = allocation_filter_join,
            stream_allocation_filter_names = stream_allocation_filter_names,
            stream_activity_filter_names = stream_activity_filter_names,

            test_cell_sel = test_cell_sel,
            name_cell_sel = name_cell_sel
            )
        return self.query

    def run_dashboard(self, email = True):
        name = 'Manual Dashboard'
        rj = kg.dj.HiveJob().job_name(name).query(self.query).execute()
        rj.wait()
        if email:
            rj.email()

