#!/usr/bin/env python

import smartsheet
import pandas as pd
import datetime

class grid:
    """
    A class that interacts with Smartsheet using its API.

    This class provides functionalities such as fetching sheet content, 
    and posting new rows to a given Smartsheet sheet.

    Important:
    ----------
    Before using this class, the 'token' class attribute should be set 
    to the SMARTSHEET_ACCESS_TOKEN.

    Attributes:
    -----------
    token : str, optional
        The access token for Smartsheet API.
    grid_id : int
        ID of an existing Smartsheet sheet.
    grid_content : dict, optional
        Content of the sheet fetched from Smartsheet as a dictionary.

    Methods:
    --------
    get_column_df() -> DataFrame:
        Returns a DataFrame with details about the columns, such as title, type, options, etc.

    fetch_content() -> None:
        Fetches the sheet content from Smartsheet and sets various attributes like columns, rows, row IDs, etc.

    fetch_summary_content() -> None:
        Fetches and constructs a summary DataFrame for summary columns.

    reduce_columns(exclusion_string: str) -> None:
        Removes columns from the 'column_df' attribute based on characters/symbols provided in the exclusion_string.

    prep_post(filtered_column_title_list: Union[str, List[str]]="all_columns") -> None:
        Prepares a dictionary for column IDs based on their titles. Used internally for posting new rows.

    delete_all_rows() -> None:
        Deletes all rows in the current sheet.

    post_new_rows(posting_data: List[Dict[str, Any]], post_fresh: bool=False, post_to_top: bool=False) -> None:
        Posts new rows to the Smartsheet. Can optionally delete the whole sheet before posting or set the position of the new rows.

    Dependencies:
    -------------
    - smartsheet (from smartsheet-python-sdk)
    - pandas as pd
    """

    token = None

    def __init__(self, grid_id):
        self.grid_id = grid_id
        self.grid_content = None
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.smart = smartsheet.Smartsheet(access_token=self.token)
            self.smart.errors_as_exceptions(True)
#region core get requests   
    def get_column_df(self):
        '''returns a df with data on the columns: title, type, options, etc...'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            return pd.DataFrame.from_dict(
                (self.smart.Sheets.get_columns(
                    self.grid_id, 
                    level=2, 
                    include='objectValue', 
                    include_all=True)
                ).to_dict().get("data"))
    def fetch_content(self):
        '''this fetches data, ask coby why this is seperated
        when this is done, there are now new objects created for various scenarios-- column_ids, row_ids, and the main sheet df'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.grid_content = (self.smart.Sheets.get_sheet(self.grid_id)).to_dict()
            self.grid_name = (self.grid_content).get("name")
            self.grid_url = (self.grid_content).get("permalink")
            # this attributes pulls the column headers
            self.grid_columns = [i.get("title") for i in (self.grid_content).get("columns")]
            # note that the grid_rows is equivelant to the cell's 'Display Value'
            self.grid_rows = []
            if (self.grid_content).get("rows") == None:
                self.grid_rows = []
            else:
                for i in (self.grid_content).get("rows"):
                    b = i.get("cells")
                    c = []
                    for i in b:
                        l = i.get("displayValue")
                        m = i.get("value")
                        if l == None:
                            c.append(m)
                        else:
                            c.append(l)
                    (self.grid_rows).append(c)
            
            # resulting fetched content
            self.grid_rows = self.grid_rows
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("rows")]
            self.grid_column_ids = [i.get("id") for i in (self.grid_content).get("columns")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.grid_columns)
            self.df["id"]=self.grid_row_ids
            self.column_df = self.get_column_df()
    def fetch_summary_content(self):
        '''builds the summary df for summary columns'''
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            self.grid_content = (self.smart.Sheets.get_sheet_summary_fields(self.grid_id)).to_dict()
            # this attributes pulls the column headers
            self.summary_params=['title','createdAt', 'createdBy', 'displayValue', 'formula', 'id', 'index', 'locked', 'lockedForUser', 'modifiedAt', 'modifiedBy', 'objectValue', 'type']
            self.grid_rows = []
            if (self.grid_content).get("data") == None:
                self.grid_rows = []
            else:
                for summary_field in (self.grid_content).get("data"):
                    row = []
                    for param in self.summary_params:
                        row_value = summary_field.get(param)
                        row.append(row_value)
                    self.grid_rows.append(row)
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("data")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.summary_params)
#endregion 
#region helpers     
    def reduce_columns(self,exclusion_string):
        """a method on a grid{sheet_id}) object
        take in symbols/characters, reduces the columns in df that contain those symbols"""
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            regex_string = f'[{exclusion_string}]'
            self.column_reduction =  self.column_df[self.column_df['title'].str.contains(regex_string,regex=True)==False]
            self.reduced_column_ids = list(self.column_reduction.id)
            self.reduced_column_names = list(self.column_reduction.title)
#endregion
#region ss post
    #region new row(s)
    def prep_post(self, filtered_column_title_list="all_columns"):
        '''preps for ss post 
        creating a dictionary per column:
        { <title of column> : <column id> }
        filtered column title list is a list of column title str to prep for posting (if you are not posting to all columns)
        [NOT USED INDEPENDENTLY, BUT USED INSIDE OF POST_NEW_ROWS]'''

        column_df = self.get_column_df()

        if filtered_column_title_list == "all_columns":
            filtered_column_title_list = column_df['title'].tolist()
    
        self.column_id_dict = {title: column_df.loc[column_df['title'] == title]['id'].tolist()[0] for title in filtered_column_title_list}
    def delete_all_rows(self):
        '''deletes up to 400 rows in 200 row chunks by grabbing row ids and deleting them one at a time in a for loop
        [NOT USED INDEPENDENTLY, BUT USED INSIDE OF POST_NEW_ROWS]'''
        self.fetch_content()

        row_list_del = []
        for rowid in self.df['id'].to_list():
            row_list_del.append(rowid)
            # Delete rows to sheet by chunks of 200
            if len(row_list_del) > 199:
                self.smart.Sheets.delete_rows(self.grid_id, row_list_del)
                row_list_del = []
        # Delete remaining rows
        if len(row_list_del) > 0:
            self.smart.Sheets.delete_rows(self.grid_id, row_list_del) 
    def post_new_rows(self, posting_data, post_fresh = False, post_to_top=False):
        '''posts new row to sheet, does not account for various column types at the moment
        posting data is a list of dictionaries, one per row, where the key is the name of the column, and the value is the value you want to post
        then this function creates a second dictionary holding each column's id, and then posts the data one dictionary at a time (each is a row)
        post_to_top = the new row will appear on top, else it will appear on bottom
        post_fresh = first delete the whole sheet, then post (else it will just update existing sheet)
        TODO: if using post_to_top==False, I should really delete the empty rows in the sheet so it will properly post to bottom'''
        
        posting_sheet_id = self.grid_id
        column_title_list = list(posting_data[0].keys())
        self.prep_post(column_title_list)
        if post_fresh:
            self.delete_all_rows()
        
        rows = []

        for item in posting_data:
            row = smartsheet.models.Row()
            row.to_top = post_to_top
            row.to_bottom= not(post_to_top)
            for key in self.column_id_dict:
                if item.get(key) != None:     
                    row.cells.append({
                    'column_id': self.column_id_dict[key],
                    'value': item[key]
                    })
            rows.append(row)

        self.post_response = self.smart.Sheets.add_rows(posting_sheet_id, rows)
    #endregion
    #region post timestamp
    def handle_update_stamps(self):
        '''grabs summary id, and then runs the function that posts the date'''
        current_date = datetime.date.today()
        formatted_date = current_date.strftime('%m/%d/%y')

        sum_id = self.grabrcreate_sum_id("Last API Automation", "DATE")
        self.post_to_summary_field(sum_id, formatted_date)
    def grabrcreate_sum_id(self, field_name_str, sum_type):
        '''checks if there is a DATE summary field called "Last API Automation", if Y, pulls id, if N, creates the field.
        then posts today's date to that field
        [ONLY TESTED FOR DATE FIELDS FOR NOW]'''
        # First, let's fetch the current summary fields of the sheet
        self.fetch_summary_content()

        # Check if "Last API Automation" summary field exists
        automation_field = self.df[self.df['title'] == field_name_str]

        # If it doesn't exist, create it
        if automation_field.empty:
            new_field = smartsheet.models.SummaryField({
                "title": field_name_str,
                "type": sum_type
            })
            response = self.smart.Sheets.add_sheet_summary_fields(self.grid_id, [new_field])
            # Assuming the response has the created field's data, extract its ID
            self.sum_id = response.data[0].id
        else:
            # Extract the ID from the existing field
            self.sum_id = automation_field['id'].values[0]

        return self.sum_id

    def post_to_summary_field(self, sum_id, post):
        '''posts to sum field, 
        designed to: posts date to summary column to tell ppl when the last time this script succeeded was
        [ONLY TESTED FOR DATE FIELDS FOR NOW]'''

        sum = smartsheet.models.SummaryField({
            "id": sum_id,
            "ObjectValue":post
        })
        resp = self.smart.Sheets.update_sheet_summary_fields(
            self.grid_id,    # sheet_id
            [sum],
            False    # rename_if_conflict
        )
    #endregion
#endregion