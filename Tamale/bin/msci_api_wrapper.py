"""  
    Downloads MSCI ESG reports from MSCI's API.
    Uploads the MSCI ESG reports to Tamale as a Entry/Note for respective Entity or Entities.
"""
# Import Namespaces
import json
import datetime
import logging
import re
import os
from requests import request, HTTPError, RequestException
import configparser

# Import Libraries (from lib and Common as required)
from SEG.utils.SEGUtils import get_app_root_dir, get_script_name_no_ext, check_file_readable, check_file_writable, coalesce_variables, get_app_work_dir

class MSCIWrapper:

    def __init__(self):

        config_file = coalesce_variables(get_app_root_dir() + '\\conf\\' + get_script_name_no_ext() + '_' + os.getenv('RUNMODE') + '.conf') 
        check_file_readable(config_file)
        flat_file_config = configparser.ConfigParser()
        flat_file_config.read(config_file)

        self.access_key = eval(flat_file_config.get('MSCIAPI', 'access_key'))
        self.secret_key = eval(flat_file_config.get('MSCIAPI', 'secret_key'))
        self.version = eval(flat_file_config.get('MSCIAPI', 'version'))
        self.uri = eval(flat_file_config.get('MSCIAPI', 'baseuri'))
    
    def __call_api(self, endpoint, additionalfields = {}, additionalheaders = {}, method = 'GET'):        
        '''
            This is a private generic method to make calls to MSCI API.
            Accepts endpoint name as parameter.
            Returns json response.
        '''
        # prepare the url        
        endpoint = endpoint.replace('#version',self.version)
        url = self.uri + endpoint

        # print / log url
        #print(url)

        # prepare the request headers
        headers = {
            "Accept":"Application/JSON,*/*"
        }
        headers.update(additionalheaders)

        # print / log headers
        #print(headers.items())

        # prepare the request parameters 
        fields = {}
        fields.update(additionalfields)

        # print / log fields
        #print(fields.items())

        try:
            response = request(method.upper(),
                               url,
                               params = fields,
                               headers = headers,
                               auth = (self.access_key, self.secret_key))
            response.raise_for_status()
        except HTTPError as ex1:
            print('HTTPError Exception Raised')
            raise ex1
        except RequestException as ex2:
            raise ex2
        finally:
            # Pending : log the status code and message | response.status_code 
            print("MSCI Request Info - Response Code {0}".format(response.status_code))
                                                                                
        return response

####################################### 1] GET ESG Issuers Detail Call ####################################### 

    def get_esgissuers_detail(self, msciids):
        '''
            Get Issuer Details.
            Accepts dictionary <IdentifierType>,<IdentifierValue> of msci recognized identifiers.
            Returns json.
        '''
        # create necessary fields for the endpoint
        fields = {
            "category_path_list": "ESG Ratings:Company Summary",
            "coverage": "esg_ratings",
            "issuer_identifier_type": list(msciids.keys())[0],
            "issuer_identifier_list": list(msciids.values())[0]
        }

        # pass endpoint to the __call_api function and return the json response
        result = self.__call_api('data/#version/issuers', fields)

        # get issuer count, if no or more than one issuer returned then raise an exception
        issuercount = len(json.loads(result)['result']['issuers'])
        print('The count of issuers : ' + str(issuercount))
        if issuercount != 1:
            raise Exception('MSCI get_esgissuers_detail - Issuer count is not 1. Please check the response from API hit')
        
        jsonreturn = result.text
        return jsonreturn


################################### 2] GET ESG Ratings Report and Save Call ################################### 

    def save_msci_esg_report(self, msciid, savingdir):
        '''
            Downloads and Saves ESG Full Ratings Report.
            Accepts dictionary <K_IdentifierType>,<V_IdentifierValue> of a msci recognized identifier.
            Accepts the saving directory location.
            Returns a tuple with complete filepath and then json response if any. 
        '''
        # create necessary fields for the endpoint
        fields = {
            "idtype": list(msciid.keys())[0],
            "format": "pdf"
        }

        # pass endpoint to the __call_api function, then save the file and return the json response
        result = self.__call_api('report/#version/reports/esgRatingsReport/{0}'.format(list(msciid.values())[0]),fields)

        try:
            # get file name from the response headers using regular expression
            cd = result.headers['content-disposition']
            filename = re.findall("filename=(.+)", cd)[0]

            # Pending : 1] Add date wise folder 2] Create the folder if not exist.
            savingdir += filename.replace('"','')

            # save the file from the response
            with open(savingdir, 'wb') as out_file:
                out_file.write(result.content)
        except IOError as ex:
            # print / log the file saving exception
            #print('MSCI Wrapper : Error while saving ESG Ratings Report file locally.')
            raise ex
        finally:
            #return response if any, check for response and then 
            jsonresponse = {}

        return (savingdir, jsonresponse)


################### Test Code ###################  
'''
url = 'https://api.msci.com/esg/'
apikey = '8219e618-2e24-42d9-9fc2-e53864cfa659'
secretkey = 'SkFCclNwejcreExyLVc3cFdraTljWUNHN1M5WlNEaTlfKjglNn09JA=='
msci = MSCIWrapper(apikey, secretkey, baseuri=url, version='v1.0')

#returnval = msci.get_esgissuers_detail({"ISIN": "KYG017191142"})
#returnval = msci.get_esgissuers_detail({"ISIN": "JP3756600007,KYG017191142"})
#print(returnval)

#returnval = msci.save_msci_esg_report({"ISIN": "KYG017191142"},'C:\\dev\\Tamale\\work\\03272020\\')
#print("Saving Dir : " + returnval[0])
'''