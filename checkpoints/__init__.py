from global_variables import *

def checkpoint(lst,total_number_records):
    new_lst = []
    check_point=0
    for item in timestamps_processed:
        check_point+=int(item["count"])
    if check_point != total_number_records:
        return False,check_point
    else:
        parsed_lst = sort_list(lst)
        new_check_point=0
        for item in parsed_lst:
            new_lst.append(item)
            new_check_point+=int(item["count"])
        if new_check_point != total_number_records:
            return False,new_check_point
        else:
            return True,new_lst,new_check_point
        
def sort_list(lst):
     sorted_lst = sorted(lst, key=lambda d: d['beginTimeSeconds']) 
     return sorted_lst
 

def sort_timestamps(lst,out_lst):
    global run_number
    global last_idx_processed
    global current_count
    new_start=""
    new_end=""
    last_idx_processed=0
    run_number+=1
    for idx,item in enumerate(lst):
        if idx != (len(lst)-1):
            if new_start == "":
                    new_start=str(item["beginTimeSeconds"])
            if current_count+item["count"] < 1999:
                current_count=current_count+item['count']
            else:
                if new_start == "":
                    new_start=item["beginTimeSeconds"]
                new_end=str(item["endTimeSeconds"])
                new_end=str(lst[idx-1]['endTimeSeconds'])
                out_lst.append({'beginTimeSeconds': str(new_start), 'endTimeSeconds': str(new_end), 'count': current_count})
                current_count=item['count']
                new_start=str(lst[idx-1]['endTimeSeconds'])
                last_idx_processed=idx
        else:
            if current_count+item["count"] < 1999:
                current_count=current_count+item['count']
                new_end=str(item["endTimeSeconds"])
                out_lst.append({'beginTimeSeconds': str(new_start), 'endTimeSeconds': str(new_end), 'count': current_count})
            else:
                new_end=str(lst[idx-1]['endTimeSeconds'])
                out_lst.append({'beginTimeSeconds': str(new_start), 'endTimeSeconds': str(new_end), 'count': current_count})
                new_start=str(lst[idx-1]['endTimeSeconds'])
                new_end=str(item["endTimeSeconds"])
                current_count=item['count']
                out_lst.append({'beginTimeSeconds': str(new_start), 'endTimeSeconds': str(new_end), 'count': current_count})
    
    current_count=0
    new_start=""
    new_end=""
    last_idx_processed=0
    return out_lst
    
    
    
