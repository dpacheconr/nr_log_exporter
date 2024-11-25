import json
import requests
import logging
import humanize
from global_variables import *
import aiohttp
import asyncio
import concurrent.futures
from checkpoints import *
from s3_file_exporter import *
import pandas as pd
import time


def parse_response(value):
    global total_number_records_from_temp_df
    global panda_frames_lst
    global number_errors_occured
    global fatal_errors
    global df
    response = value[0]
    s_since = value[1]
    s_until= value[2]
    count= value[3]
    temp_df = pd.DataFrame.from_dict(json.loads(json.dumps(response['data']['actor']['account']['nrql']['results'])))
    try:
        panda_frames_lst.append(temp_df)
        if int(count) == int(len(temp_df.index)):
            with lock:
                total_number_records_from_temp_df+=len(temp_df.index)
            logging.info("Processed data for timestamps between "+ str(datetime.datetime.fromtimestamp(int(s_since))) + " and " + str(datetime.datetime.fromtimestamp(int(s_until))) + " added " + str(len(temp_df.index)) + " records to dataframe")
    except Exception as e:
        with lock:
            number_errors_occured+=1
        logging.error("Failed to add dataframe to list -> "+str(e))
        
    return len(temp_df.index)


def make_request_total(s_since,s_until):  
    global total_number_records
    response = None
    global query_total
    logging.info("Querying total number records with query: "+query_total+" since "+s_since+" until "+s_until+" LIMIT MAX")
    while response is None:
        try:
            headers = {
            'API-Key': NEW_RELIC_API_KEY,
            }

            json_data = {
            'query': '{\n actor {\n account(id: '+NEW_ACCOUNT_ID+') {\n nrql(query: "'+query_total+' since '+s_since+' until '+s_until+' LIMIT MAX") {\ntotalResult\n }\n }\n }\n}\n',
            'variables': '',
            }
            response = requests.post('https://api.newrelic.com/graphql', headers=headers, json=json_data, timeout=30)
            data = json.loads(json.dumps(response.json()['data']['actor']['account']['nrql']['totalResult']['count']))
            total_number_records=data
            logging.info("Total number records in select period is: "+ str(total_number_records))
            
        except:
            logging.error("Unable to obtain total number records from API, will retry")
            response=None
            continue
    return total_number_records

async def make_request(session,i):
    global q
    global make_request_errors
    s_since = str(i["beginTimeSeconds"])
    s_until= str(i["endTimeSeconds"])
    count= i["count"]
    global query
    data = None
    logging.debug("Obtaining raw data with query: "+query+" since "+s_since+" until "+s_until+" LIMIT MAX")
    logging.info("Obtaining raw data from "+str(datetime.datetime.fromtimestamp(int(s_since))) + " to " + str(datetime.datetime.fromtimestamp(int(s_until))))
    while data is None:
        try:
            headers = {
            'API-Key': NEW_RELIC_API_KEY,
            }      
            json_data = {
            'query': '{\n actor {\n account(id: '+NEW_ACCOUNT_ID+') {\n nrql(query: "'+query+' since '+s_since+' until '+s_until+' LIMIT MAX") {\n results\n nrql\n }\n }\n }\n}\n',
            'variables': '',
            }      
            async with session.post('https://api.newrelic.com/graphql',headers=headers,json=json_data, timeout=30) as resp:
                response = await resp.json()
                if response['data']['actor']['account']['nrql']['results']:
                    data = response['data']['actor']['account']['nrql']['results']
                    q.put([response,s_since,s_until,count])
                    return [response,s_since,s_until,count]
                else:
                    data = None
        
        except:
            logging.error("Unable to obtain raw data for this iteration, will retry")
            data = None
            continue

async def make_request_timeseries_async(session,unix_time_since,unix_time_to,series,iteration):
    global requests_queue
    global lock
    c_since =int(unix_time_since)
    c_until=int(unix_time_to)
    c_iteration=iteration
    response = None
    global query_total
    logging.info("Obtaining timeseries data with query: "+query_total+" since "+str(int(c_since))+" until "+str(int(c_until))+" TIMESERIES "+series)
    while response is None:
        try:
            headers = {
            'API-Key': NEW_RELIC_API_KEY,
            }
            json_data = {
            'query': '{\n actor {\n account(id: '+NEW_ACCOUNT_ID+') {\n nrql(query: "'+query_total+' since '+str(int(c_since))+' until '+str(int(c_until))+' TIMESERIES '+series+'") {\n totalResult\n }\n }\n }\n}\n',
            'variables': '',
            }
            
            async with session.post('https://api.newrelic.com/graphql',headers=headers,json=json_data, timeout=30) as resp:
                response = await resp.json()
                data = response['data']['actor']['account']['nrql']['totalResult']
                c_iteration+=1
                for item in data:
                    c_data=item
                    c_data
                    requests_queue.put([c_data,c_iteration,series,iteration])        
        except:
            logging.error("Unable to obtain data from API for this timeseries -> "+str(datetime.datetime.fromtimestamp(int(c_since))) + " and " + str(datetime.datetime.fromtimestamp(int(c_until)))+ ", will retry.")
     
async def worker():
    global timestamps_processed
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
        current_total=0
        tasks = []
        while True:
            data = requests_queue.get()
            if data is None:
                break
            c_data=data[0]
            c_iteration=data[1]
            c_since =c_data["beginTimeSeconds"]
            c_until=c_data["endTimeSeconds"]
            if c_data["count"] > 1999:
                if duration >= 1296000: # check if it's more than 15 days, more than 366 buckets
                    tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,c_since,c_until,"1 hour",0)))
                else:
                    if c_iteration == 1:
                        tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,c_since,c_until,"1 minute",1)))
                    elif c_iteration == 2:
                        tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,c_since,c_until,"1 second",2)))
                    else:
                        tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,c_since,c_until,"MAX",3))) # still needs testing as never hit this on any query, so may fail
            else:
                if c_data["count"] != 0:
                    current_total+=c_data["count"]
                    timestamps_processed.append(c_data)
            
            await asyncio.gather(*tasks)
            if requests_queue.empty():
                requests_queue.put(None)

    logging.info("Worker processed records: "+str(current_total))  

# another coroutine that cancels a task
async def task_cancel(other_task):
    global requests_queue
    # wait a moment
    await asyncio.sleep(0.3)
    print(requests_queue.qsize())
    if requests_queue.qsize() > 0:
        # cancel the other task
        other_task.cancel()
       
# entry point coroutine
async def obtain_time_series_data():
    global duration
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
        tasks = []
        logging.info("Duration: "+str(duration))
        if duration >= 1296000: # check if it's more than 15 days
            tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,unix_time_since,unix_time_to,"1 day",0)))
        elif duration > 60 and duration <= 3600: # check if it's less than 1 hour
            if duration%60==0:
                tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,unix_time_since,unix_time_to,"1 minute",1)))
            else:
                tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,unix_time_since,unix_time_to,"1 second",2)))
        elif duration <= 60: # check if it's less than 1 minute
            tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,unix_time_since,unix_time_to,"1 second",2)))
        else: # that means all other requests we will be requesting in 1 hour series
            tasks.append(asyncio.ensure_future(make_request_timeseries_async(session,unix_time_since,unix_time_to,"1 hour",0)))
        await asyncio.gather(*tasks)
            
    # create a task
    task = asyncio.create_task(worker())
    # create the wait for coroutine
    wait_coro = asyncio.wait_for(task, timeout=500)
    # wrap the wait coroutine in a task and execute
    wait_task = asyncio.create_task(wait_coro)
    # await the wait-for task
    try:
        await wait_task
    except asyncio.TimeoutError:
        asyncio.create_task(task_cancel(wait_task))
        logging.error('Gave up waiting, task canceled')
    except asyncio.CancelledError:
        asyncio.create_task(task_cancel(wait_task))
        logging.error('Wait for task was canceled externally')

    
    logging.info('Finished obtaining timeseries data')
    

    
async def process_raw_data(final_sorted_timestamps_to_fecth_data): 
    logging.info("Requesting raw data")  
    tasks = [] 
    batchsize = max_workers
    logging.info("Requesting batches of "+str(max_workers))  
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
        for i in range(0, len(final_sorted_timestamps_to_fecth_data), batchsize):
            batch = final_sorted_timestamps_to_fecth_data[i:i+batchsize] 
            for item in batch:
                tasks.append(asyncio.ensure_future(make_request(session,item)))
            await asyncio.gather(*tasks)
        
    logging.info("All items added to queue")
    logging.info("Will consume data in queue")

    rolling_total=0
    global df
    li = list(q.queue)
    count=0
    for i in li:
        count+=i[3]
    if count != total_number_records:
        logging.error("Unexpected number items in converted list, will request new data from New Relic")
        return False
    
    if len(final_sorted_timestamps_to_fecth_data) != len(li):
        logging.error("Unexpected number items in queue, will request new data from New Relic")
        logging.error("Queue size was "+str(q.qsize())+" and converted list " + str(len(li)))
        return False
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in  executor.map(parse_response, li):
                rolling_total+=result
                
        logging.info("All items in queue processed")
        while rolling_total != total_number_records:
            logging.error("Total number logs received from New Relic API " +str(rolling_total)+ " does not match the expected total records "+str(total_number_records))
            return False
        
        logging.info("Total number logs received from New Relic API " +str(rolling_total)+ " matches the expected total records "+str(total_number_records))
        logging.info("Total number records expected after parsing raw data")       
        return True
    

def divide_chunks(l, n):
      
    # looping till length l
    for i in range(0, len(l), n): 
        yield l[i:i + n]
  
def generate_csv():
    global retry
    global s3_client
    global panda_frames_lst
    df = pd.DataFrame()
    timestr = time.strftime("%Y_%m_%d_%H_%M_%S")
    try:
        df=pd.concat(panda_frames_lst, ignore_index=True)
        x = list(divide_chunks(df,100000))
        for idx,i in enumerate(x):
            filename="exported_data_"+timestr+"_"+str(idx)+".csv"
            i.to_csv(filename, encoding='utf-8')
        # Enable to push CSV to AWS S3
        # bucket = os.getenv("bucket")
        # with open(filename, "rb") as f:
        #     s3_client.upload_fileobj(f, bucket, filename)
        # os.remove(filename)
        logging.info("Number of records in CSV "+str(len(df. index)))
    except Exception as e:
        logging.error("Unable to convert dataframe to CSV due to "+str(e))
        retry=True
        return