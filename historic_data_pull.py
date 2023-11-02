#region imports
import smartsheet
from smartsheet.exceptions import ApiError
from smartsheet_grid import grid
import requests
import json
import time
from globals import bamb_token_base64_Coby, bamb_token_base64, smartsheet_token
from globals import smartsheet_token
from logger import ghetto_logger
import xml.etree.ElementTree as ET
#endregion

class HistoricBambooUpdater():
    """
    The HistoricBambooUpdater class provides utilities for interfacing between BambooHR's API
    and Smartsheet's API to fetch, transform, and update employee historical data.

    This class specifically fetches detailed employment status, hiring, and termination data
    for each employee and then formats this data suitably for posting to a Smartsheet grid.

    Methods:
        - pull_report_682: Fetches a specific BambooHR report with detailed employee data.
        - extract_employee_id_list: Extracts a list of employee IDs and their names.
        - pullnclean_employement_status_table: Fetches and transforms employment status data from BambooHR.
        - query_empl_directory: Queries the employee directory using an ID and returns specific field value.
        - api_original_hire_date: Fetches the "Original Hire Date" for a given employee ID.
        - api_sage_id: Fetches the Sage ID for a given employee ID.
        - get_original_hire_date: Computes the correct original hire date from possibly inconsistent data.
        - get_date: Determines the date of a specific employment event based on its occurrence.
        - arrange_posting_data: Structures the data in preparation for posting to Smartsheet.
        - run: Executes the primary sequence of tasks: fetching, transforming, and posting data.

    Usage:
        To use this class, instantiate it with a config dictionary, and then call the `run` method.

    Requirements:
        you will need logger.py and smartsheet_grid.py in the same folder as this file for it to run correctly
    """
    def __init__(self, config):
        self.config = config
        self.smartsheet_token=config.get('smartsheet_token')
        self.bamb_token=config.get('bamb_token_base64')
        self.bamb_token2=config.get('bamb_token_base64_Coby')
        grid.token=smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.start_time = time.time()
        self.log=ghetto_logger("historic_data_pull.py")
        self.histdata_grid=grid(config.get('historicbamboodata_sheetid'))
        self.annirecog_grid=grid(config.get('anni_recognition_sheetid'))
        
#region grab-data
    def pull_report_649(self):
        '''report 682 has all the parameters designed for this system (LMS:IT *DON'T DELETE/CHANGE*)'''
        url = "https://api.bamboohr.com/api/gateway.php/dowbuilt/v1/reports/649?format=JSON&onlyCurrent=true"
        headers = {"authorization": f"Basic {self.bamb_token}"}
        response = requests.get(url, headers=headers)
        hris_data=json.loads(response.text).get('employees')
        return hris_data
    def extract_employee_id_list(self):
        '''the employement status is id and then statuses, this employee id list allows us to map each id to the empl name'''
        self.log.log('Pulling BambooHr Data...')
        self.empl_directory = self.pull_report_649()
        employee_id_list = [{'name': F"{employee.get('firstName')} {employee.get('lastName')}", 'id':employee.get('id')} for employee in self.empl_directory]
        return employee_id_list 
    def pullnclean_employement_status_table(self):
        '''messy func which gets employement status table as html, and then uses Element Tree to mine the data and clean it into a list of dictionaries'''
        url = "https://api.bamboohr.com/api/gateway.php/dowbuilt/v1/employees/changed/tables/employmentStatus?since=2000-01-01T11%3A54%3A00Z"
        headers = {"authorization": f"Basic {self.bamb_token}"}
        response = requests.get(url, headers=headers)

        root = ET.fromstring(response.text)

        result = {
            "table": {
                "@id": root.attrib['id'],
                "employee": []
            }
        }

        for employee in root.findall('employee'):
            emp_data = {
                "@id": employee.attrib['id'],
                "@lastChanged": employee.attrib['lastChanged'],
                "row": []
            }
            for row in employee.findall('row'):
                row_data = {}
                for field in row.findall('field'):
                    field_id = field.attrib['id']
                    row_data[field_id] = field.text
                emp_data["row"].append(row_data)
            result["table"]["employee"].append(emp_data)
        
        empl_stat_data = [{'id':empl.get('@id'), 'data':empl.get('row')} for empl in result.get('table').get('employee')]
        return empl_stat_data
#endregion
#region prep posting
    def query_empl_directory(self, id, search_str_key):
        '''we got employee directory from report 682, so instead of doing api call, we can just look through our data dict to find some fields'''
        for empl in self.empl_directory:
            if empl['id'] == id:
                return empl[search_str_key]
    def api_original_hire_date(self, id):
        '''looks specifically for Original Hire Date Field, which is used inconsistently'''
        url = f"https://api.bamboohr.com/api/gateway.php/dowbuilt/v1/employees/{id}/?fields=originalHireDate&onlyCurrent=true"

        headers = {
            "accept": "application/json",
            "authorization": f"Basic {self.bamb_token}"
        }

        response = requests.get(url, headers=headers)
        resp_dict = json.loads(response.content.decode('utf-8'))
        return resp_dict.get("originalHireDate")
    def api_sage_id(self, id):
        '''grabs sage ID'''
        url = f"https://api.bamboohr.com/api/gateway.php/dowbuilt/v1/employees/{id}/?fields=customSageID&onlyCurrent=true"
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {self.bamb_token2}"
        }
        response = requests.get(url, headers=headers)
        resp_dict = json.loads(response.content.decode('utf-8'))
        return resp_dict.get('customSageID')
    def get_original_hire_date(self, input):
        """
        Get the correct original hire date, as the input data is not clean/easy to read.
        - input_data: [{'id': '1',
        'data': [{'date': '2016-02-08', 'employmentStatus': 'Original Hire Date'},
        {'date': '2015-09-28', 'employmentStatus': 'Original Hire Date'},
        {'date': '2016-02-08', 'employmentStatus': 'Salary Full Time - Admin'},
        {'date': '2019-01-24', 'employmentStatus': 'Terminated'}],
        """
        input_data = input.get('data')

        if input_data[0].get('date') ==  "0000-00-00":
            original_hiredate = self.api_original_hire_date(input.get('id'))
        else:
            original_hiredate = input_data[0].get('date')

        # Attempt to get the second hire date, and handle possible IndexError
        try:
            second_hiredate = input_data[1].get('date')
            if input_data[1].get('employmentStatus') != "Terminated" and second_hiredate < original_hiredate:
                return second_hiredate
        except IndexError:
            # If there's no second event, pass so it can return original_diredate
            pass
        
        return original_hiredate
    def get_date(self, input_data, param_index, search_value):
        """
        Get the date of a specific employment event (Hire, Termination) based on its occurrence.
        """

        if search_value == "Original Hire":
            return self.get_original_hire_date(input_data)

        term_count = 0  # Counter for occurrences of termination

        for event in input_data.get('data'):
            status = event.get('employmentStatus')
            date = event.get('date')

            # Check for any form of termination in the status, if so add to the counter. then return value if its the correct termination
            if 'erminated' in status:
                term_count += 1
                if search_value == "Terminated" and term_count == param_index:
                    return date

            # If looking for a hire date, return the date after the required termination
            elif search_value == "Hire" and term_count == param_index:
                return date

        return ""
    def arrange_posting_data(self):
        # Constructing posting_data list 20 mins w/ department
        self.log.log(f'Arranging {len(self.empl_stat_data)} Posting records (this will take time)')
        posting_data = []
        for i, empl in enumerate(self.empl_stat_data):
            # for testing
            # if i < 200:
            posting_data.append({
                'Name': f"{self.query_empl_directory(empl.get('id'), 'firstName')} {self.query_empl_directory(empl.get('id'), 'lastName')}",
                'Id': empl.get('id'),
                'HRIS Original Hire': self.get_date(empl, 0, "Original Hire"),
                'HRIS Original Termination': self.get_date(empl, 1, "Terminated"),
                'HRIS Rehire': self.get_date(empl, 1, "Hire"),
                'HRIS Retermination': self.get_date(empl, 2, "Terminated"),
                'HRIS Final Hire': self.get_date(empl, 2, "Hire"),
                'HRIS Final Termination': self.get_date(empl, 3, "Terminated"),
                'Sage Id': self.api_sage_id(empl.get('id')),
                'Location': self.query_empl_directory(empl.get('id'), 'location'),
                'Job Title': self.query_empl_directory(empl.get('id'), 'jobTitle'),
                'Department': self.query_empl_directory(empl.get('id'), 'department'),
                'Work Email': self.query_empl_directory(empl.get('id'), 'workEmail'),
            })
            if int(i) % 100 == 0 and i != 0:
                self.log.log(f"   Records {i-100}-{i} Arranged.")
        return posting_data
#endregion

    def run(self):
        '''runs main script as intended'''
        self.employee_id_list = self.extract_employee_id_list()
        self.empl_stat_data = self.pullnclean_employement_status_table()
        self.posting_data = self.arrange_posting_data()
        self.log.log('Posting Data...')
        # posting for Powerbi re: ticket data
        self.histdata_grid.post_new_rows(self.posting_data, post_fresh=True)
        self.histdata_grid.handle_update_stamps()
        # posting for Recognition Smartsheet re: bonuses/shouts/swag
        self.annirecog_grid.update_rows(self.posting_data, "Name")
        self.annirecog_grid.handle_update_stamps()
        self.log.log('~Fin~')


if __name__ == "__main__":
    config = {
        'smartsheet_token':smartsheet_token,
        'bamb_token_base64_Coby':bamb_token_base64_Coby, 
        'bamb_token_base64': bamb_token_base64,
        'historicbamboodata_sheetid':820659588386692,
        'anni_recognition_sheetid':2371507443421060,
    }
    hbu= HistoricBambooUpdater(config)
    hbu.run()