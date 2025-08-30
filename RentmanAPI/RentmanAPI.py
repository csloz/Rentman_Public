'''
    API documentation for Rentman can be found at https://api.rentman.net/docs
    This code provides a class to interact with the Rentman API, allowing for 
    fetching and processing data related to projects, crew, and costs, despite the Rentman API :)

    It includes methods for fetching data, normalizing it into pandas DataFrames, and performing various operations such as getting display names, calculating costs, and handling crew data.
    The class also includes caching for API responses to improve performance and reduce the number of API calls
    made during execution.
    The class is designed to be used in a Python environment with the requests, pandas, and requests_cache libraries installed.
    It also includes a decorator for timing function execution and a method for pretty printing JSON data.
    The class can be extended or modified to include additional functionality as needed.    

'''

import time, requests, requests_cache, logging, atexit, json, pandas as pd
# from functools import wraps 

#Import data from Constants File
from .RentmanConstants import *

# Setup staffing class
class RentmanAPI:
    def __init__(self, api_token, api_url, debug=False):
        
        self.debug=debug
        self.api_token = api_token
        self.api_url = api_url
        self.api_call_count = 0
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        requests_cache.install_cache('api_cache') # Cache responses for 1 hour

        atexit.register(self.print_api_call_count)

    def print_api_call_count(self):
        if self.debug:
            print(f"Total Rentman API calls made: {self.api_call_count}")

    # def timeit(self, func):
    #     """Decorator to time a function."""
    #     @wraps(func)
    #     def wrapper(*args, **kwargs):
    #         start_time = time.perf_counter()
    #         result = func(*args, **kwargs)
    #         end_time = time.perf_counter()
    #         print(f"Function '{func.__name__}' took {end_time - start_time:0.4f} seconds")
    #         return result
    #     return wrapper

    def get_all_pages(self, what, max=5000):
        """Iterate through all pages of results for a given endpoint."""
        offset = 0
        limit = 50 #at some point make this a parameter
        results = []
        while True:
            params = {"offset": offset, "limit": limit}
            endpoint = f"{self.api_url}/{what}"
            response = requests.get(endpoint, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data['data'], list):
                    results.extend(data['data'])
                elif isinstance(data['data'], dict):
                    results.append(data['data'])
                if (len(data["data"]) < limit):
                    break
                else:
                    offset += limit
            else:
                print(f"Error fetching offset: {offset}")
                break
        return results

    def get_item(self, what, item_id):
        """Get a specific item by ID."""
        endpoint = f"{self.api_url}/{what}/{item_id}"
        response = requests.get(endpoint, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    def pretty_print_json(self, json_data):
        """Pretty print JSON data."""
        print(json.dumps(json_data, indent=2))

    def get_displayname(self, id, df, field='displayname', sourcefield='id'):
        """Get display name for a given folder ID."""
        try:
            if id.isnumeric():
                id = int(id)
        except:
            pass
        if id not in df[sourcefield].values:
            logging.debug(f"ID [{id}] not found in df for field={field} sourcefield={sourcefield}")
        try:
            retval = df.loc[df[sourcefield] == id, field].values[0] if not df.loc[df[sourcefield] == id, field].empty else None
            return retval
        except:
            return None

    
    #Batching wrapper for larger requests to avoid API overload as max request size is 102400 bytes for AWS
    def batch_fetch_and_normalize(self, cmd_string, unique_project_ids, batch_size=200, max_results=-1):
        """
        Wrapper to fetch projectfunctions in batches to avoid API overload.
        Returns a concatenated DataFrame for all project IDs.
        """
        
        resultsdf = []

        #Split batch into smaller batches
        items = [item.strip() for item in unique_project_ids.split(',') if item.strip()]
        batches =  [",".join(items[i:i + batch_size]) for i in range(0, len(items), batch_size)]


        for batch in batches:
            df = self.fetch_and_normalize(f"{cmd_string}{batch}", max_results=max_results)
            if not df.empty:
                resultsdf.append(df)
        if resultsdf:
            return pd.concat(resultsdf, ignore_index=True)
        else:
            return pd.DataFrame()
        

    def fetch_and_normalize(self, endpoint, max_results=-1) :
        '''
            Fetch JSON data
        '''
        self.api_call_count += 1
        if (self.debug):
                logging.debug(endpoint)
        data = self.get_all_pages(endpoint, max_results)
        data = pd.json_normalize(data)
        data = data.replace(r'/\w+/', '', regex=True) #remove prefixes eg /project/xxx -> xxx
        return data
    
    
    
    def remove_timezones(self,df):
        '''
        Remove Time Zones from DF
        '''
        for col in df.select_dtypes(include=['datetimetz']).columns:
            df[col] = df[col].dt.tz_localize(None)
        return df
    

    def getprojectid (self, id):
        ''' 
        Get the project ID from the project number (project number is the external ui identifier for the project)
        '''
        projectid = None
        df = self.fetch_and_normalize(f"projects?number={id}&fields=id,number")
        if not df.empty:
            projectid = str(df['id'].values[0])
        return projectid
    
    def getprojectnumber (self, id):
        ''' 
        Get the project number from the project ID (project number is the external ui identifier for the project)
        '''
        projectnumber = None
        df = self.fetch_and_normalize(f"projects/{id}?fields=number")
        if not df.empty:
            projectnumber = str(df['number'].values[0])
        return projectnumber