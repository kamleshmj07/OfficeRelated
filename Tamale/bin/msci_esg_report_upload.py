"""  
    # Setup boiler plate code 
    # i.e. parser Arg, Setup Logger, Setup Database Connection, Setup Config File
    # Get the MSCI SOI List -- Get the SOI List verified from ALex... have see many duplicate  
    # Loop through the List
        # Check if the ISIN is blank.. if Blank raise an exception and continue with the next one
        # Take the ISIN and get the Issuer details from MSCI Data API
        # If we dont get the response raise an exception and continue with the next one
        # if there is a proper response then 
            # If the Debug flag is on then download the json response as a file and save it to the work folder
            # get the IVA_RATING DATE and send it to a function to detect if there is a change in report               
            # if there is no change write a log and move to next one 
            # if there is a change then download the report using MSCI Report API
            # if the Rating_Date is null then add a record to MSCI_RATING_Master table
            # Add a record in the Audit table
            # Get the Tamale Short Name from the SOI List. 
            # If there is a missing one check for the ISIN value as Alias in the Tamale -- Needs to verify from ALEX
            # Upload the report and note in Tamale.. 
	    
    # Step 1  : Get MSCI-Tamale Mappings for SOI using DataUtil.py
    # Step 2  : Start looping over the Mappings one by one
        
        # Step 2a : Call MSCI API using Wrapper to get report detail and store json
        # Step 2b : Pass the json from Step 2a to detect_rating_change function to detect a change in rating
        # Step 3  : If YES there is a change in rating then 
        #               - Call MSCI API using Wrapper to download + save ESG report, returns fullpath of file and success message
        #           If NO change in rating then
        #               - Log the Security information and store in a separate list
        # Step 4  : If report download successful then 
        #           Call the Tamale API using Wrapper to post the ESG reports as Note using Tamale Id and filepath 
        #           Get Response
    # Step 5  : Notify the Security information for which Tamale Id mapping was not found, no change of ratings and/or report was not downloaded     
Author: Gravitas Team
"""
# Import Namespaces
import logging
import logging.config
import os
import sys
import json
import argparse
from requests import HTTPError, RequestException
from datetime import datetime as dtt
import numpy
import pandas

# Import Libraries (from bin, lib and Common as required)
from SEG.utils.SEGUtils import get_db_conn_name, get_log_config, get_app_work_dir
from SEG.DB.DBConn import DBConn as db_util
from msci_api_wrapper import MSCIWrapper
from tamale_api_wrapper import TamaleWrapper

################################################ DB function Calls ####################################################################

def generateSOITickerList(con):
    """
    The function get the SOI list for the given date
    """
    log = logging.getLogger(__name__)
    try:
        log.info("In generate SOI Ticker Function")
        #sql = "[dbo].[sp_tamaleMSCISOI] '@firmwide','{0}',null".format(dtt.now().strftime("%Y-%m-%d")) check how to get the last bussiness date
        sql = "[dbo].[sp_tamaleMSCISOI] '@firmwide','2020-03-17',null"
        results = db_util().return_ordered_resultset(connection=con, sql=sql)
        return results
    except Exception as e:
        raise e

#
def get_msci_rating_master(con):
    """
    """
    sql = "select tamaleId ,securitysymbol ,securityname ,isin ,cusip ,sedol ,ticker ,id_bbg_global ,convert(date, rating_date) rating_date ,current_rating ,previous_rating_date ,previous_rating ,rating_trend ,rating_analysis ,db_insert_time ,db_update_time ,tamale_shortname from [dbo].[MSCI_Rating_Master]"
    df = pandas.read_sql(sql=sql, con=con)
    return df


# def insert_update_msci_rating_master(con, securityname, isin, cusip, sedol, ticker, id_bbg_global, rating_date, current_rating, rating_trend, rating_analysis, tamale_shortname):
def insert_update_msci_rating_master(insertFlag, con, soiTicker, msciissuerdetails):
    """
    This function add a record to MSCI Rating Master Table.. Need to check the primary key for the tables after discussing with Alex.
    """
    log = logging.getLogger(__name__)
    try:
        tamaleid = soiTicker["Tamale_ID"]
        securitysymbol = soiTicker["Security_Symbol"]
        securityname =soiTicker["Security_Name"]
        isin = soiTicker["id_isin"]
        cusip = soiTicker["Cusip"]
        sedol = soiTicker["id_sedol1"]
        ticker = soiTicker["axysSymbol"]
        id_bbg_global = soiTicker["BB_ID"]
        rating_date = msciissuerdetails['result']['issuers'][0]['IVA_RATING_DATE']
        current_rating = msciissuerdetails['result']['issuers'][0]['IVA_COMPANY_RATING']
        previous_rating = msciissuerdetails['result']['issuers'][0]['IVA_PREVIOUS_RATING']
        rating_trend = msciissuerdetails['result']['issuers'][0]['IVA_RATING_TREND']
        rating_analysis = msciissuerdetails['result']['issuers'][0]['IVA_RATING_ANALYSIS'] 

        if insertFlag:
            log.info("Inserting data to MSCI_Rating_Master")
            sql = """INSERT INTO [dbo].[MSCI_Rating_Master] (tamaleid, securitysymbol, securityname, isin, cusip, sedol, ticker, id_bbg_global, rating_date, current_rating, previous_rating, rating_trend, rating_analysis, db_insert_time)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""            
            param = (tamaleid, securitysymbol, securityname, isin, cusip, sedol, ticker, id_bbg_global, rating_date, current_rating, previous_rating, rating_trend, rating_analysis, dtt.now().strftime("%Y-%m-%d"))
        else:
            log.info("Update data to MSCI_Rating_Master")
            if tamaleid == None:
                sql = """UPDATE [dbo].[MSCI_Rating_Master] set rating_date = ?, current_rating = ?, previous_rating = ?, rating_trend = ?, rating_analysis = ?, db_update_time = ? where securitysymbol = ?"""
                param = (rating_date, current_rating, previous_rating, rating_trend, rating_analysis, dtt.now().strftime("%Y-%m-%d"), securitysymbol)
            else:
                sql = """UPDATE [dbo].[MSCI_Rating_Master] set rating_date = ?, current_rating = ?, previous_rating = ?, rating_trend = ?, rating_analysis = ?, db_update_time = ? where tamaleid = ? and securitysymbol = ?"""
                param = (rating_date, current_rating, previous_rating, rating_trend, rating_analysis, dtt.now().strftime("%Y-%m-%d"),tamaleid, securitysymbol)
        
        log.info("Executing sql %s with %s", sql, param)
        cursor=con.cursor()    
        cursor.execute(sql,param)
        cursor.commit()
        cursor.close()
        
        log.info("Data inserted successfully to the MSCI_Rating_Master table")

    except Exception as e:
        raise e
####################################################################################################################

def main():
    # read the command line agrs
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--ini_file",
                            help="location of db config json file")

    arg_parser.add_argument("--section_name",
                            help="database connection name from json file",
                            default="QA_TamaleMarketData")
    arg_parser.add_argument("--log_level", help="level of logging",
                            default="INFO")
    arg_parser.add_argument("--process_date",
                            help="Date to process for, if required",
                            default=dtt.now().strftime("%Y-%m-%d"))

    options = arg_parser.parse_args()

    # Setting logger Info
    logging.config.fileConfig(get_log_config(),
                              defaults=({'filename_append': "",
                                         'run_id': get_db_conn_name(options.section_name)}))

    logging.basicConfig(format='%(asctime)s|%(module)s|%(levelname)s|' +
                        '%(lineno)d|%(message)s', level=options.log_level)

    log = logging.getLogger(__name__)

    log.info("--------MSCI ESG Report Python Script has started----")

    # Pending : Create the folders if not present and intorduce date wise folder structure
    # Need to fetch below values from config 
    savingdir = get_app_work_dir()
    responsedir = get_app_work_dir() + 'jsonresponses\\'
    
    # Common Database Connection Object
    con = db_util().get_connection(conn_name=options.section_name,
                                   conn_info_file=options.ini_file)
    # Get MSCI Rating Master List from Database
    dfMSCI = get_msci_rating_master(con)

    # Get SOI List from Database
    soiList = generateSOITickerList(con)

    for soiTicker in soiList:
        # check None for security identifiers
        if soiTicker["id_isin"] != None:
            soiid = soiTicker["id_isin"]
            soiidtype = 'isin'
        elif soiTicker["id_cusip"] != None:
            soiid = soiTicker["id_cusip"]
            soiidtype = 'cusip'
        elif soiTicker["id_sedol1"] != None:
            soiid = soiTicker["id_sedol1"]
            soiidtype = 'sedol'
        elif soiTicker["axysSymbol"] != None:
            soiid = soiTicker["axysSymbol"]
            soiidtype = 'ticker'
        else:
            # Pending : print / log the security information (without id use Tamaleid of other identifier)
            # Pending : add security to exception list, mark as incomplete information
            continue

        #print('Inside for loop - soiid {0} soiidtype {1} '.format(str(soiid), str(soiidtype)))
        log.debug('Inside for loop - soiid {0} soiidtype {1} '.format(str(soiid), str(soiidtype)))

        log.info('Inside for loop - Start call to MSCI Data API')    
        dictmsciid = {soiidtype : soiid}
        msciobj = MSCIWrapper()    
        
        # get msci issuer data
        try:
            jsonissuers = msciobj.get_esgissuers_detail(dictmsciid)
        except (HTTPError, RequestException) as ex1:
            log.info('Inside for loop - MSCI Request Exception with code ' + ex1.response.status_code)
            # Pending : add security to exception list, mark as process error
            continue
        except:
            # Pending : add security to exception list, mark as process error
            log.info('Inside for loop - MSCI Wrapper general exception with ' + sys.exc_info()[0])
            continue

        if options.log_level == 'DEBUG':
            fn = 'jsonissuerdetails' + soiid + '-'+ dtt.now().strftime("%Y-%m-%d-%H-%M-%S-%f")+ '.txt'
            jsondir = ''
            jsondir = responsedir + fn
            record_jsonresponse(jsondir, jsonissuers)
            log.debug('Inside for loop - Issuer response will be saved to directory ' + jsondir)
            
        log.info('Inside for loop - End call to MSCI Data API')

        # download msci esg ratings report
        msciissuerdetailsobj = json.loads(jsonissuers)

        '''
        # Check te response output # Pending : This should be handled at API call level
        if (msciissuerdetailsobj['code'] != 200):
            log.error ('Error occured while fetching the data from MSCI Data API for security with {0} as {1}'.format(str(soiid),soiidtype))
            log.info('Moving to next ticker')
            continue
      
        # Pending : This should be handled at API call level
        resultList = msciissuerdetailsobj['result']
        if(len(resultList['issuers'])==0):
            log.error ('Blank Issuers details fetched from MSCI Data API for security with {0} as {1}'.format(str(soiid),soiidtype))
            log.info('Moving to next ticker')
            continue
        '''

        log.info('Inside for loop - Start call to MSCI Report API')

        # getting the MSCI Rating Date for the Security..
        tamale_id = soiTicker['Tamale_ID']
        security_symbol = soiTicker['Security_Symbol']

        ratingDate = None
        if dfMSCI.empty == False:
            #dfMSCIRow = dfMSCI[dfMSCI['tamaleId']== tamale_id   & dfMSCI['securitysymbol']=='security_symbol']
            dfMSCIRow = dfMSCI[dfMSCI['securitysymbol']==security_symbol]
            if dfMSCIRow.empty == False:
                ratingDate = str(dfMSCIRow.iloc[0]['rating_date'])
		       
        # check if there is a change in rating
        changeflag = True
        insertFlag = True
        if ratingDate != None:
            changeflag = detect_rating_change(ratingDate, msciissuerdetailsobj)  # detect_rating_change(str(soiTicker[RATING_DATE]), msciissuerdetailsobj)
            insertFlag = False        
    
        if changeflag:
            log.info('Inside for loop - Downloading MSCI ESG Report')
            # download msci esg report
            try:
                resptuple = msciobj.save_msci_esg_report(dictmsciid, savingdir)
            except (HTTPError, RequestException) as ex1:
                log.info('Inside for loop - MSCI Request Exception with code ' + ex1.response.status_code)
                # Pending : add security to exception list, mark as process error
                continue
            except:
                # Pending : add security to exception list, mark as process error
                log.info('Inside for loop - MSCI Wrapper general exception with ' + sys.exc_info()[0])
                continue
        else:
            # Pending add security to the exception list, mark as no change detected
            log.info('Inside for loop - No change in rating detected for security ' + str(soiid))
            # continue with the next security
            continue

        fullfilename = resptuple[0]         # returns full path with filename
        jsonrespmsciesg = resptuple[1]

        if options.log_level == 'DEBUG' and jsonrespmsciesg != '':
            fn = 'jsonesgreports' + soiid + '.txt'
            jsondir = ''
            jsondir = responsedir + fn
            record_jsonresponse(responsedir, jsonrespmsciesg)
            log.debug('Inside for loop - Report response will be saved to directory ' + jsondir)
         
        # Add/Update record to MSCI Rating Table 
        insert_update_msci_rating_master(insertFlag, con, soiTicker, msciissuerdetailsobj)
        # for loop ends
    
    log.info("--------MSCI ESG Report Python Script has ended----")


###### Utility ######
def record_jsonresponse(fullfilepath, strjsonresponse):
    '''
        This routine records json response on the location provided.
        Accepts fullfilepath i.e. filename with full path.
        Accepts strjsonresponse as string json.
    '''
    try:
        with open(fullfilepath, 'w') as out_file:
            out_file.write(strjsonresponse)
    except IOError as e:
        # log the file name and log the exception message
        print(e)


def detect_rating_change(ratingsdateold, msciissuerdetails):
    ''' 
        This function detects rating change.
        Accepts ratings_date old as last ratings date from the mappings dataframe.
        Accepts mscireportdetailobj as json response from MSCI Data API for an issuer.
        Accepts fieldpath as Json path to the ratings date field from the json response.
        Returns boolean based on whether rating is changed or not.
    '''
    log = logging.getLogger(__name__)
    log.info('Inside detect_rating_change - Start')
    changedetected = False

    # get the new ratings date from json
    if msciissuerdetails['result']['issuers'][0]['IVA_RATING_DATE'] != '':
        ratingsdatenew = msciissuerdetails['result']['issuers'][0]['IVA_RATING_DATE']
        # print / log the new rating date
        #print('Inside detect_rating_change - New Rating Date ' + ratingsdatenew)
        log.info('Inside detect_rating_change - New Rating Date ' + ratingsdatenew)
    else:
        log.info('Inside detect_rating_change - Could not find a New rating date')
        return changedetected

    # format both old and new dates
    olddate = dtt.strptime(ratingsdateold, '%Y-%m-%d').date()
    newdate = dtt.strptime(ratingsdatenew, '%Y-%m-%d').date()

    # compare the dates and return a value
    if olddate < newdate or olddate == None:
        changedetected = True

    log.info('Inside detect_rating_change - End')

    return changedetected


#if __name__ == "__main__":
#    main()

main()