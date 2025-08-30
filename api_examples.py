"""
# Example code for using the API to pull data from Rentman

"""


import os,  pandas as pd, requests_cache, time,  pandas as pd, requests
from functools import wraps
from dotenv import load_dotenv, find_dotenv  # Install with pip install python-dotenv

start_date = "2025-01-01"
end_date = "2025-12-31" 
debug = False

#Library in subfolder RentmanAPI
from RentmanAPI.RentmanAPI import RentmanAPI  


#import key and url from .env file
# -------------------------------------
env_path = find_dotenv()
if not env_path or not os.path.exists(env_path):
    print(".env file not found!")
    quit(1)
else:
    load_dotenv()
    api_token = os.getenv("RENTMAN_API_KEY")
    api_url  = os.getenv("RENTMAN_URL")
# -------------------------------------



#Set full display of columns in pandas DataFrame when debugging
pd.set_option('display.max_columns', None) 


#Instantiate the RentmanAPI class with the api token and URL
rentman = RentmanAPI(api_token=api_token, api_url=api_url, debug=True)

    
if __name__ == "__main__":

    #Example of getting data from Rentman
    contactsdf = rentman.fetch_and_normalize("contacts")
    contactdf = rentman.fetch_and_normalize("contactpersons")
    crewdf= rentman.fetch_and_normalize("crew")

    #Example of updating ID's to Values in DF's
    contactdf['creator'] = contactdf['creator'].apply(lambda id: rentman.get_displayname(id, df=crewdf, sourcefield='id')) 
    contactdf['contact'] = contactdf['contact'].apply(lambda id: rentman.get_displayname(id, df=contactsdf, sourcefield='id'))
    contactsdf['creator'] = contactsdf['creator'].apply(lambda id: rentman.get_displayname(id, df=crewdf, sourcefield='id'))
    contactsdf['default_person'] = contactsdf['default_person'].apply(lambda id: rentman.get_displayname(id, df=contactdf, sourcefield='id'))
    
    ####################################################################################
    # Start on the project data
    ####################################################################################

    #Example to pull all data related to projects within a date range.
    projectfunctiongroupsdf = rentman.fetch_and_normalize(f"projectfunctiongroups?fields=id,project&planperiod_start[gte]={start_date}&planperiod_end[lte]={end_date}", max_results=-1)
    #convert id data to be useful
    unique_project_ids = projectfunctiongroupsdf['project'].dropna().unique().tolist()
    unique_project_ids_str = ",".join(map(str, unique_project_ids))
    
    #Pull Project Details individually, as if we pull as a group, we don't get the useful data.
    projectsdf = pd.DataFrame()
    for number in unique_project_ids:
        if debug:
            print (f"Fetching ProjectID:{number}")
        data = rentman.fetch_and_normalize(f'projects/{number}')
        projectsdf = pd.concat([projectsdf, data], ignore_index=True)

    
    #and pull costs for those projects - use batch as this can easily be larger than the max call size.
    costsdf = rentman.batch_fetch_and_normalize(f'costs?project=', unique_project_ids_str)
    
    print(costsdf.head())
    print(projectsdf.head())
    
    #Batch Fetch pulls larger data sets in batches - useful where data is too big for an API call.
    #Fetch and normalize is used for smaller API calls that are under the max API size.
    
    #You can now process the DFs to do what you need for output.
    