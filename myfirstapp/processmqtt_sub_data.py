from django.core.cache import cache
import pymongo
import json
from django.conf import settings
# from datetime import datetime
from datetime import datetime, timedelta
my_client = pymongo.MongoClient('mongodb://localhost:27017/mydb')
from django.utils import timezone
import pytz
import re

utc_time = timezone.now()  # Assuming you have a UTC time stored in the database
indian_timezone = pytz.timezone('Asia/Kolkata')
indian_time = utc_time.astimezone(indian_timezone)
formatted_datetime = indian_time.strftime("%Y-%m-%d %H:%M:%S")
formatted_date = indian_time.strftime("%Y-%m-%d")
formatted_hour = indian_time.strftime("%H")

lowerTime = formatted_date+" "+formatted_hour+":00:00"
upperTime = formatted_date+" "+formatted_hour+":59:59"
print("lower time ",lowerTime)
print("upperTime time ",upperTime)
# First define the database name
dbname = my_client['water_initiative']
# Now get/create collection name (remember that you will see the database in your mongodb cluster only after you create a collection)
history = dbname["history"]
latestdata = dbname["repo_latestdata"]
cnd_sen_data = dbname["treat_cnd_sen"]
cnd_sen_hourly=dbname["treat_cnd_sen_hourly"]
cnd_sen_daily=dbname["treat_cnd_sen_daily"]
cnd_sen_monthly=dbname["treat_cnd_sen_monthly"]
cnd_sen_yearly=dbname["treat_cnd_sen_yearly"]

def processmqtt_sub_data(msg):
    cache.clear()
    global msgo
    hmq=msg.topic
    hmqm_split=hmq.split('/')
    device_id=hmqm_split[1]
    msg_type=hmqm_split[2]
    components=hmqm_split[3]
    sums=counts=avgs=0
    messege_new = msg.payload
    print(messege_new)
    decoded_data = messege_new.decode('utf-8')  # Decode bytes to string
    fixed_data = decoded_data.replace("\'", "\"")  # Replace single quotes with double quotes
    final_mqtt_msg = json.loads(fixed_data)  # Parse string as JSON
    print(final_mqtt_msg)

   

    history_data = {
        "device_id": device_id,
        "message_type" : msg_type,
        "component_name" : components,
        "msg_json" : final_mqtt_msg,
        "created_at": formatted_datetime,
        "updated_at": formatted_datetime
    }
    history.insert_one(history_data)
   
    try:
        print("components:",components)
        if 'cnd_tds'== components:
            existing_dict = final_mqtt_msg
            existing_dict['device_id'] = device_id
            existing_dict['components'] = components
            existing_dict['created_at'] = formatted_datetime
            existing_dict['updated_at'] = formatted_datetime
            cnd_sen_data.insert_one(existing_dict)

            criteria = {"created_at": {"$gte": lowerTime,"$lte": upperTime }}

            cnd_sen_data_for_hour = cnd_sen_data.find(criteria)
            count = cnd_sen_data.count_documents(criteria)

            print(count)
            cnd_hour_sum = 0
            asp_hour_sum = 0
            tsp_hour_sum = 0
            spn_hour_sum = 0
            for all_data in cnd_sen_data_for_hour:
                for k,v in all_data.items():
                    print(k,v)
                    if k == 'cnd':
                        cnd_hour_sum = cnd_hour_sum + int(v)
                    if k == 'asp':
                        asp_hour_sum = asp_hour_sum + int(v)
                    if k == 'tsp':
                        tsp_hour_sum = tsp_hour_sum + int(v)
                    if k == 'spn':
                        spn_hour_sum = spn_hour_sum + int(v)

            cnd_hour_avg=cnd_hour_sum/count
            asp_hour_avg=asp_hour_sum/count
            tsp_hour_avg=tsp_hour_sum/count
            spn_hour_avg=spn_hour_sum/count

            

            collection_names = dbname.list_collection_names()
            if cnd_sen_hourly in collection_names:
                cnd_sen_hourly_data = cnd_sen_hourly.find(criteria)
                if cnd_sen_hourly_data:
                    for all_data in cnd_sen_hourly_data:
                        print(all_data)
                    print("in if")

                    cnd_hour_data = {
                        "device_id": device_id,
                        "message_type" : msg_type,
                        "component_name" : components,
                        "cnd":{"sum": cnd_hour_sum, "avg":cnd_hour_avg,'count':count},
                        "asp":{"sum": asp_hour_sum, "avg":asp_hour_avg,'count':count},
                        "tsp":{"sum": tsp_hour_sum, "avg":tsp_hour_avg,'count':count},
                        "spn":{"sum": spn_hour_sum, "avg":spn_hour_avg,'count':count},
                        "created_at": formatted_datetime,
                        "updated_at": formatted_datetime
                    }
                    cnd_sen_hourly.insert_one(cnd_hour_data)

                else:
                print("in else")
                cnd_hour_data = {
                    "device_id": device_id,
                    "message_type" : msg_type,
                    "component_name" : components,
                    "cnd":{"sum": cnd_hour_sum, "avg":cnd_hour_avg,'count':count},
                    "asp":{"sum": asp_hour_sum, "avg":asp_hour_avg,'count':count},
                    "tsp":{"sum": tsp_hour_sum, "avg":tsp_hour_avg,'count':count},
                    "spn":{"sum": spn_hour_sum, "avg":spn_hour_avg,'count':count},
                    "created_at": formatted_datetime,
                    "updated_at": formatted_datetime
                }
                cnd_sen_hourly.insert_one(cnd_hour_data)

        #         # Print the aggregation result
        #         if len(result) > 0:
        #             print("Sum:", result[0]["total"])
        #             print("Average:", result[0]["average"])
        #             print("Count:", result[0]["count"])
        #         else:
        #             print("No documents found in the last hour.")
                    
        #         # # {"sum": 0, "avg": 0.0, "count": 1}
        
            
            

    except Exception as e:
        print(e)
   