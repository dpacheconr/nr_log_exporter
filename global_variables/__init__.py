import datetime
import logging
import threading
from queue import Queue
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

#Ensure that mandatory variables are configured before starting
def check_env_vars():
    keys = ("NEW_ACCOUNT_ID","NEW_RELIC_API_KEY","QUERY","DATE_SINCE","DATE_TO")

    keys_not_set = []

    for key in keys:
        if key not in os.environ:
            keys_not_set.append(key)
    else:
        pass

    if len(keys_not_set) > 0: 
        for key in keys_not_set:
            print(key + " not set")
        exit(1)
    else:
        pass # All required environment variables set

check_env_vars()

# filename="log.txt",filemode='a'
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=logging.INFO,datefmt='%Y-%m-%d %H:%M:%S')
  
# Initializing a queue
q = Queue()

# Configure variables
num_retries=3
run_number=0
timestamps_to_fecth_data_for=[]
current_count=0
queue_empty=False
timestamps_processed=[]
calculated_timestamps=[]
final_request_list_queue=Queue()
requests_queue=Queue()
date_since = os.getenv("DATE_SINCE")
date_to = os.getenv("DATE_TO")
query=os.getenv("QUERY")
query_total= query.replace("*","count(*)")
unix_time_since = datetime.datetime.timestamp(datetime.datetime.strptime(date_since,"%Y-%m-%d %H:%M:%S"))
unix_time_to = datetime.datetime.timestamp(datetime.datetime.strptime(date_to,"%Y-%m-%d %H:%M:%S"))
duration= int((unix_time_to-unix_time_since))
number_errors_occured=0
total_number_records=0
total_number_records_from_temp_df=0
rolling_total=0
fatal_errors=0
make_request_errors=[]
timestamps_already_processed=[]
status = True
panda_frames_lst=[]
check_point=0
lock = threading.Lock()
# None max_workers will default number processors on the machine
max_workers=500
NEW_RELIC_API_KEY = os.getenv("NEW_RELIC_API_KEY")
NEW_ACCOUNT_ID = os.getenv("NEW_ACCOUNT_ID")
retry=False