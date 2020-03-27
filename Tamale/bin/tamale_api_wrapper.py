"""
"""
# Import Namespaces
import json
import ntpath
import datetime
from requests import request, HTTPError, RequestException

# Import Libraries (from lib and Common as required)


class TamaleWrapper:

    def __init__(self, access_key, secret_key, baseuri="", version=""):
        self.access_key = access_key
        self.secret_key = secret_key
        self.version = version
        self.uri = baseuri

    def __call_api(self, method, endpoint, additionalfields = {}, additionalheaders = {}, file = None):        
        '''
            This is a private generic method to make calls to Tamale API.\n
            Accepts endpoint name as parameter.
            Returns json response.
        '''
        # prepare the url        
        url = '{0}/{1}'.format(self.uri, self.version) + endpoint

        # print / log url
        #print(url)

        # prepare the request headers
        headers = {
            #"Content-Type":"application/x-www-form-urlencoded"
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
                               auth = (self.access_key, self.secret_key),
                               files = file,
                               verify = False)
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

################################### 1] GET Entity by alias Call ################################### 

    def get_entity_by_alias(self, secid):
        '''

        '''
        # create necessary fields for the endpoint
        fields = {
            "outputformat":"json",
            "filterby":"alias",
            "filterstring":secid
        }

        # pass endpoint to the __call_api function and return the json response
        result = self.__call_api('GET', '/entity/', fields)
        
        jsonreturn = result.text
        return jsonreturn

################################### 2] POST Entry by Tamale Id with attachment Call ################################### 

    def create_entry_with_attachment(self, tamaleids, fullfilepath, extradata = {}):
        '''
        '''
        # get filename from the filepath
        #filename = ntpath.basename(fullfilepath)

        # create necessary fields for the endpoint
        fields = {
            "outputformat":"json",
            "entities":tamaleids,
        }

        # update the extradata
        fields.update(extradata)

        try:
            # create files object
            files = [('attachment', open(fullfilepath,'rb'))]
        except IOError as ex:
            print('Tamale create_entry_with_attachment - Unable to read the file.')
            raise ex

        # pass endpoint to the __call_api function and return the json response
        result = self.__call_api('POST', '/entry/', additionalfields=fields, file=files)
        
        jsonreturn = result.text
        return jsonreturn

############# Test ###############
'''
url = 'https://seg-ct-tamaletest2/restapi'
apikey = 'localIT'
secretkey = 'l0calIT'
tamale = TamaleWrapper(apikey,secretkey,url,'2.0')
#resp = tamale.get_entity_by_alias('US01609W1027')
#print(resp)

fullfilepath = 'C:\\dev\\Tamale\\work\\03272020\\ESG Ratings Report - ALIBABA GROUP HOLDING LIMITED.pdf'
extradata = {"entry-type":"Proxy Review"}
resp = tamale.create_entry_with_attachment(tamaleids='BABA', fullfilepath= fullfilepath, extradata=extradata)
print(resp)
'''