from django.core.cache import cache
import pymongo
import json
from django.conf import settings
# from datetime import datetime
from datetime import datetime, timedelta
my_client = pymongo.MongoClient('mongodb://localhost:27017/mydb')
from django.utils import timezone
import pytz

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
    history_data = {
        "device_id": device_id,
        "message_type" : msg_type,
        "component_name" : components,
        "msg_json" : msg.payload.decode('utf-8'),
        "created_at": formatted_datetime,
        "updated_at": formatted_datetime
    }
    history.insert_one(history_data)
   
    try:
        print("components:",components)
        if 'cnd_tds'== components:
            existing_dict = json.loads(msg.payload)
            existing_dict['device_id'] = device_id
            existing_dict['components'] = components
            existing_dict['created_at'] = formatted_datetime
            existing_dict['updated_at'] = formatted_datetime
            cnd_sen_data.insert_one(existing_dict)

            criteria = {"created_at": {"$gte": lowerTime,"$lte": upperTime }}
            criteria_cnd = {"created_at": {"$gte": lowerTime,"$lte": upperTime },
                            'cnd': { '$exists': True }}
            cnd_sen_hour_data = cnd_sen_data.find(criteria)
            count = cnd_sen_data.count_documents(criteria)
            print("count ",count)

            criteria_new = {"$match": criteria_cnd}
            pipelines = [
                criteria_new,
                {"$group": {"_id": None, "total": {"$sum": "$value"}, "average": {"$avg": "$value"}, "count": {"$sum": 1}}}
            ]

            result = list(cnd_sen_data.aggregate(pipelines))

            # Print the aggregation result
            if len(result) > 0:
                print("Sum:", result[0]["total"])
                print("Average:", result[0]["average"])
                print("Count:", result[0]["count"])
            else:
                print("No documents found in the last hour.")
                
            # {"sum": 0, "avg": 0.0, "count": 1}

            # for cnd_data in cnd_sen_hour_data:
            #     print(cnd_data)
            

    except Exception as e:
        print(e)
   