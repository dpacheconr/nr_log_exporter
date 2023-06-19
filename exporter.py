import datetime
import time
import pandas as pd
import logging
import asyncio
from global_variables import *
from api_calls import *
from checkpoints import *
from s3_file_exporter import *

def main():
    global fatal_errors
    global timestamps_processed  
    global rolling_total
    global requests_queue
    global q
    global retry
    global num_retries
    
    #Clear queues and counters
    retry=False
    rolling_total=0
    timestamps_processed.clear()
    requests_queue.queue.clear()
    q.queue.clear()
    panda_frames_lst.clear()
    
    # Start timer
    start_time = time.time()

    # Output information regarding current configuration
    logging.info("Processing data from "+date_since+" to "+date_to)  
    logging.info("Total amount time to export data for is "+str(datetime.timedelta(seconds=duration))+" hours")
    
    
    total_number_records=make_request_total(str(int(unix_time_since)),str(int(unix_time_to)))
    logging.info("Obtaining timeseries data and calculating timestamps")
        
    logging.info("Finished calculating timestamps")
    logging.info("Requesting timeseries data for the calculated timestamps")


    for i in range(1,num_retries+1):
        asyncio.run(obtain_time_series_data())
        check_point_lst=checkpoint(timestamps_processed,total_number_records)
        result = check_point_lst[0]
        if result:
            break
        else:
            logging.error("Number records on timeseries data parsed does not match total number records we needed to obtain, this is what we have "+ str(check_point_lst[1])+" this is what we needed "+str(total_number_records))
            logging.error("Attempt number "+str(i))
            logging.error("Will backoff for 5 seconds first, before proceeding")
            time.sleep(5)
            i+=1
        if i == num_retries+1:
            logging.error("FATAL: Unable to obtain timeseries data without errors, exiting")
            fatal_errors+=1
            retry=True
            return
    
    global timestamps_to_fecth_data_for
    if len(timestamps_to_fecth_data_for)>0:
        timestamps_to_fecth_data_for.clear()
    final_sorted_timestamps=sort_timestamps(check_point_lst[1],timestamps_to_fecth_data_for)
    check_point_lst=checkpoint(final_sorted_timestamps,total_number_records)
    if check_point_lst[0] == False:
        logging.error("Number records on sorted timeseries data parsed does not match total number records we needed to obtain, this is what we have "+ str(check_point_lst[1])+" this is what we needed "+str(total_number_records))
        retry=True
        return
        
    logging.info("Number records on timeseries data parsed "+ str(check_point_lst[2])+" matches total number records "+str(total_number_records))
    logging.info("Total number records expected in final sorted list, will request the raw data now")  


    final_sorted_timestamps_to_fecth_data=check_point_lst[1] 
    for i in range(1,num_retries+1):
        result = asyncio.run(process_raw_data(final_sorted_timestamps_to_fecth_data))
        if result:
            break
        else:
            with q.mutex:
                q.queue.clear()
            logging.error("Will attempt to obtain raw data again, this was attempt number "+str(i))
            logging.error("Will backoff for 5 seconds first, before requesting more data from API")
            time.sleep(5)
            i+=1
        if i == 4:
            logging.error("FATAL: Unable to obtain raw data from API")
            retry=True
            return

    # Parse the data received from API
    logging.info("Processed data between "+ str(datetime.datetime.fromtimestamp(int(unix_time_since))) + " and " + str(datetime.datetime.fromtimestamp(int(unix_time_to))))
    logging.info("Data obtained from New Relic API in "+str(datetime.timedelta(seconds=(time.time() - start_time)))+ " minutes")

    logging.info("Exporting to CSV")
    generate_csv()
        
    if number_errors_occured <=0 or fatal_errors<=1:
        logging.info("No errors ocurred")
    else:
        if total_number_records == len(df.index):
            logging.info("Number of errors occured: "+str(number_errors_occured))
            logging.info("Errors ocurred " +str(number_errors_occured)+ ", but we were able to obtain all records on retries")
            logging.info("Errors occured:  "+str(number_errors_occured)+" and fatal errors:  "+str(fatal_errors))
        else:
            logging.error("FATAL ERROR OCURRED EXIT")
            retry=True
            return
  
    logging.info("The expected number records was "+str(total_number_records)+" and this is what we got!")
    logging.info("Total amount time it took to run script "+str(datetime.timedelta(seconds=(time.time() - start_time)))+ " minutes")

if __name__ == "__main__":
    for i in range(1,num_retries+1):
        main()
        if not retry:
           break         
       
        if i == num_retries:
            logging.exception("FATAL ERROR OCURRED, UNABLE TO OBTAIN DATA FROM NEW RELIC, TRY AGAIN LATER")
