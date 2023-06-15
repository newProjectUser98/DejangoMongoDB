import pymongo
import json
from django.conf import settings
# from datetime import datetime
from datetime import datetime, timedelta
my_client = pymongo.MongoClient('mongodb://localhost:27017/mydb')
import time
# First define the database name
dbname = my_client['watero']

# Now get/create collection name (remember that you will see the database in your mongodb cluster only after you create a collection)
history = dbname["history"]
latestdata = dbname["repo_latestdata"]
cnd_sen_data = dbname["treat_cnd_sen"]
cnd_sen_hourly=dbname["treat_cnd_sen_hourly"]
cnd_sen_daily=dbname["treat_cnd_sen_daily"]
cnd_sen_monthly=dbname["treat_cnd_sen_monthly"]
cnd_sen_yearly=dbname["treat_cnd_sen_yearly"]
def dateandtime():
    '''this function used for  date and time for database records'''
    year=datetime.today().strftime('%Y')
    month=datetime.today().strftime('%m')
    # day=datetime.today().strftime('%d')
    day=13
    # hour=datetime.now().strftime('%H')
    hour='10'
    minit=datetime.now().strftime('%M')
    second=datetime.now().strftime('%S')
    return year,month,day,hour,minit,second
def processmqtt_sub_data(msg):
    print("satish")
    print(msg)
    print(f'Received message on topic: {msg.topic} with payload: {msg.payload}')
    global msgo
    print(msg.payload)
    jstring=msg.payload
    dict_str = jstring.decode("UTF-8")
    rep1=dict_str.replace("}",'')
    rep2=rep1.replace("{",'')
    array_dat = rep2.split(',')
    mydata ={}
    insert_data = {}
    calculation_data ={}
    for loop_data in array_dat:
        removed_col = loop_data.split(':')
        mydata[removed_col[0]] =removed_col[1]
        if removed_col[0]=='cnd':
            if removed_col[1] !="":
                insert_data['cnd']=removed_col[1]
                #tds=removed_col[1]
        elif removed_col[0]=='spn':
            if removed_col[1] !="":
                insert_data['spn']=removed_col[1]
                #tds=removed_col[1]
        elif removed_col[0]=='tds':
            if removed_col[1] !="":
                insert_data['tds']=removed_col[1]
                #tds=removed_col[1]
        elif removed_col[0]=='tsp':
            if removed_col[1] !="":
                insert_data['tsp']=removed_col[1]
                #tsp=removed_col[1]
        elif removed_col[0]=='asp':
            if removed_col[1] !="":
                insert_data['asp']=removed_col[1]
                #asp=removed_col[1]
        elif removed_col[0]=='sts':
            if removed_col[1] !="":
                insert_data['sts']=removed_col[1]
                #sts=removed_col[1]
        elif removed_col[0]=='crt':
            if removed_col[1] !="":
                insert_data['crt']=removed_col[1]
                #crt=removed_col[1]
        elif removed_col[0]=='olc':
            if removed_col[1] !="":
                insert_data['olc']=removed_col[1]
                #olc=removed_col[1]
        elif removed_col[0]=='drc':
            if removed_col[1] !="":
                insert_data['drc']=removed_col[1]
                #drc=removed_col[1]
        elif removed_col[0]=='rtl':
            if removed_col[1] !="":
                insert_data['rtl']=removed_col[1]
                #rtl=removed_col[1]
        elif removed_col[0]=='ttl':
            if removed_col[1] !="":
                insert_data['ttl']=removed_col[1]
                #ttl=removed_col[1]
        elif removed_col[0]=='lps':
            if removed_col[1] !="":
                insert_data['lps']=removed_col[1]
                #lps=removed_col[1]
        elif removed_col[0]=='hps':
            if removed_col[1] !="":
                insert_data['hps']=removed_col[1]
                #hps=removed_col[1]
        elif removed_col[0]=='dgp':
            if removed_col[1] !="":
                insert_data['dgp']=removed_col[1]
                #dgp=removed_col[1]
        elif removed_col[0]=='mod':
            if removed_col[1] !="":
                insert_data['mod']=removed_col[1]
                #mod=removed_col[1]
        elif removed_col[0]=='ipv':
            if removed_col[1] !="":
                insert_data['ipv']=removed_col[1]
                #ipv=removed_col[1]
        elif removed_col[0]=='unv':
            if removed_col[1] !="":
                insert_data['unv']=removed_col[1]
                #unv=removed_col[1]
        elif removed_col[0]=='ovv':
            if removed_col[1] !="":
                insert_data['ovv']=removed_col[1]
                #ovv=removed_col[1]
        elif removed_col[0]=='nmv':
            if removed_col[1] !="":
                insert_data['nmv']=removed_col[1]
                #nmv=removed_col[1]
        elif removed_col[0]=='stp':
            if removed_col[1] !="":
                insert_data['stp']=removed_col[1]
                #stp=removed_col[1]
        elif removed_col[0]=='bkt':
            if removed_col[1] !="":
                insert_data['bkt']=removed_col[1]
                #bkt=removed_col[1]
        elif removed_col[0]=='rst':
            if removed_col[1] !="":
                insert_data['rst']=removed_col[1]
                #rst=removed_col[1]
        elif removed_col[0]=='err':
            if removed_col[1] !="":
                insert_data['err']=removed_col[1]
                #err=removed_col[1]
        elif removed_col[0]=='fr1':
            if removed_col[1] !="":
                insert_data['fr1']=removed_col[1]
                #fr1=removed_col[1]
        elif removed_col[0]=='fr2':
            if removed_col[1] !="":
                insert_data['fr2']=removed_col[1]
                #fr2=removed_col[1]
        elif removed_col[0]=='ff1':
            if removed_col[1] !="":
                insert_data['ff1']=removed_col[1]
                #ff1=removed_col[1]
        elif removed_col[0]=='ff2':
            if removed_col[1] !="":
                insert_data['ff2']=removed_col[1]
                #ff2=removed_col[1]
        elif removed_col[0]=='pos':
            if removed_col[1] !="":
                insert_data['pos']=removed_col[1]
                #pos=removed_col[1]
        elif removed_col[0]=='rmt':
            if removed_col[1] !="":
                insert_data['rmt']=removed_col[1]
                #rmt=removed_col[1]
        elif removed_col[0]=='cct':
            if removed_col[1] !="":
                insert_data['cct']=removed_col[1]
                #cct=removed_col[1]
        elif removed_col[0]=='srt':
            if removed_col[1] !="":
                insert_data['srt1']=removed_col[1]
                #srt1=removed_col[1]
            srt2=removed_col[2]
            srt=srt1+':'+srt2
        elif removed_col[0]=='bkt':
            if removed_col[1] !="":
                insert_data['bkt']=removed_col[1]
                #bkt=removed_col[1]
        elif removed_col[0]=='mot':
            if removed_col[1] !="":
                insert_data['mot']=removed_col[1]
                #mot=removed_col[1]
        elif removed_col[0]=='stp':
            if removed_col[1] !="":
                insert_data['stp']=removed_col[1]
                #stp=removed_col[1]
        elif removed_col[0]=='op1':
            if removed_col[1] !="":
                insert_data['op1']=removed_col[1]
                #op1=removed_col[1]
        elif removed_col[0]=='op2':
            if removed_col[1] !="":
                insert_data['op2']=removed_col[1]
                #op2=removed_col[1]
        elif removed_col[0]=='op3':
            if removed_col[1] !="":
                insert_data['op3']=removed_col[1]
                #op3=removed_col[1]
        elif removed_col[0]=='ip1':
            if removed_col[1] !="":
                insert_data['ip1']=removed_col[1]
                #ip1=removed_col[1]
        elif removed_col[0]=='ip2':
            if removed_col[1] !="":
                insert_data['ip2']=removed_col[1]
                #ip2=removed_col[1]
        elif removed_col[0]=='ip3':
            if removed_col[1] !="":
                insert_data['ip3']=removed_col[1]
                #ip3=removed_col[1]
        elif removed_col[0]=='psi':
            if removed_col[1] !="":
                insert_data['psi']=removed_col[1]
                #psi=removed_col[1]
        elif removed_col[0]=='ndv':
            if removed_col[1] !="":
                insert_data['ndv']=removed_col[1]
                #ndv=removed_col[1]
        elif removed_col[0]=='ntt':
            if removed_col[1] !="":
                insert_data['ntt']=removed_col[1]
                #ntt=removed_col[1]
        elif removed_col[0]=='nta':
            if removed_col[1] !="":
                insert_data['nta']=removed_col[1]
                #nta=removed_col[1]
        elif removed_col[0]=='tmp':
            if removed_col[1] !="":
                insert_data['tmp']=removed_col[1]
                #tmp=removed_col[1]
        elif removed_col[0]=='ntp':
            if removed_col[1] !="":
                insert_data['ntp']=removed_col[1]
                #ntp=removed_col[1]
        elif removed_col[0]=='nov':
            if removed_col[1] !="":
                insert_data['nov']=removed_col[1]
                #nov=removed_col[1]
        elif removed_col[0]=='vl1':
            if removed_col[1] !="":
                insert_data['vl1']=removed_col[1]
                #vl1=removed_col[1]
        elif removed_col[0]=='vl2':
            if removed_col[1] !="":
                insert_data['vl2']=removed_col[1]
                #vl2=removed_col[1]
        elif removed_col[0]=='vl3':
            if removed_col[1] !="":
                insert_data['vl3']=removed_col[1]
                #vl3=removed_col[1]
        elif removed_col[0]=='vl4':
            if removed_col[1] !="":
                insert_data['vl4']=removed_col[1]
                #vl4=removed_col[1]
        elif removed_col[0]=='re1':
            if removed_col[1] !="":
                insert_data['re1']=removed_col[1]
                #re1=removed_col[1]
        elif removed_col[0]=='re2':
            if removed_col[1] !="":
                insert_data['re2']=removed_col[1]
                #re2=removed_col[1]
        elif removed_col[0]=='re3':
            if removed_col[1] !="":
                insert_data['re3']=removed_col[1]
                #re3=removed_col[1]
        elif removed_col[0]=='re4':
            if removed_col[1] !="":
                insert_data['re4']=removed_col[1]
                #re4=removed_col[1]
        elif removed_col[0]=='p1':
            if removed_col[1] !="":
                insert_data['p1']=removed_col[1]
                #p1=removed_col[1]
        elif removed_col[0]=='p2':
            if removed_col[1] !="":
                insert_data['p2']=removed_col[1]
                #p2=removed_col[1]
        elif removed_col[0]=='p3':
            if removed_col[1] !="":
                insert_data['p3']=removed_col[1]
                #p3=removed_col[1]
        elif removed_col[0]=='p4':
            if removed_col[1] !="":
                insert_data['p4']=removed_col[1]
                #p4=removed_col[1]
        elif removed_col[0]=='fr':
            if removed_col[1] !="":
                insert_data['fr']=removed_col[1]
                #fr=removed_col[1]
    mydata1=mydata      
    mydata = json.dumps(mydata, indent = 4) 
    mydatadict=json.loads(mydata)
    hmq=msg.topic
    hmqm_split=hmq.split('/')
    device_id=hmqm_split[1]
    msg_type=hmqm_split[2]
    sums=counts=avgs=0
    if(msg_type == 'updset'):
        print("msg_type",msg_type)
        components=hmqm_split[3]
        od=mydata.strip()
        history_data = {
            "device_id": device_id,
            "message_type" : msg_type,
            "component_name" : components,
            "msg_json" : mydata1
            }
        history.insert(history_data)
        dd=dateandtime() 
        all_deviceids=[]
        try:
            print("components:",components)
            if 'cnd_sen'== components:
                dd=dateandtime() 
                insert_data['device_id'] = device_id
                insert_data['message_type'] = msg_type
                insert_data['year']= dd[0]
                insert_data['month']= dd[1]
                insert_data['day']= dd[2]
                insert_data['hour']= dd[3]
                insert_data['minit']= dd[4]
                insert_data['second']=dd[5]
                cnd_sen_data.insert(insert_data)

                # Access or create the destination collection
                destination_collection = dbname["treat_cnd_sen_hourly"]
                source_collection = dbname["treat_cnd_sen"]
                hourly_cocument=cnd_sen_data.find()
                print("hourly document:",hourly_cocument)
                time.sleep(1)
                # Pipeline stages for aggregation
                pipeline = [
                    {
                        "$group": {
                            "_id": {
                                "device_id": "$device_id",
                                "hour": "$hour",
                                "year": "$year",
                                "month": "$month",
                                "day": "$day"
                            },
                            "cnd_sum": {"$sum": {"$toInt": "$cnd"}},
                            "cnd_avg": {"$avg": {"$toInt": "$cnd"}},
                            "cnd_count": {"$sum": 1},
                            "spn_sum": {"$sum": {"$toInt": "$spn"}},
                            "spn_avg": {"$avg": {"$toInt": "$spn"}},
                            "spn_count": {"$sum": 1},
                            "tsp_sum": {"$sum": {"$toInt": "$tsp"}},
                            "tsp_avg": {"$avg": {"$toInt": "$tsp"}},
                            "tsp_count": {"$sum": 1},
                            "asp_sum": {"$sum": {"$toInt": "$asp"}},
                            "asp_avg": {"$avg": {"$toInt": "$asp"}},
                            "asp_count": {"$sum": 1}
                        }
                    }
                ]

                # Perform aggregation on the source collection
                result = source_collection.aggregate(pipeline)

                # Iterate over the aggregation result
                for item in result:
                    device_id = item["_id"]["device_id"]
                    hour = item["_id"]["hour"]
                    year = item["_id"]["year"]
                    month = item["_id"]["month"]
                    day = item["_id"]["day"]
                    cnd_sum = item["cnd_sum"]
                    cnd_avg = item["cnd_avg"]
                    cnd_count = item["cnd_count"]
                    spn_sum = item["spn_sum"]
                    spn_avg = item["spn_avg"]
                    spn_count = item["spn_count"]
                    tsp_sum = item["tsp_sum"]
                    tsp_avg = item["tsp_avg"]
                    tsp_count = item["tsp_count"]
                    asp_sum = item["asp_sum"]
                    asp_avg = item["asp_avg"]
                    asp_count = item["asp_count"]

                    # Create the new document for the destination collection
                    new_document = {
                        "device_id": device_id,
                        "service": "cnd",
                        "cnd": {"sum": cnd_sum, "avg": cnd_avg, "count": cnd_count},
                        "spn": {"sum": spn_sum, "avg": spn_avg, "count": spn_count},
                        "tsp": {"sum": tsp_sum, "avg": tsp_avg, "count": tsp_count},
                        "asp": {"sum": asp_sum, "avg": asp_avg, "count": asp_count},
                        "month": month,  # Placeholder for month value, can be set later
                        "year": year,  # Placeholder for year value, can be set later
                        "day": day,   # Placeholder for day value, can be set later
                        "hour": hour
                    }


                    # Update or insert the document into the destination collection
                    filter_query = {
                        "device_id": device_id,
                        "hour": hour
                    }
                    filter_daily = {
                        "device_id": device_id,
                        "day": day
                    }
                    update_query = {
                        "$set": {
                            "cnd": {"sum": cnd_sum, "avg": cnd_avg, "count": cnd_count},
                            "spn": {"sum": spn_sum, "avg": spn_avg, "count": spn_count},
                            "tsp": {"sum": tsp_sum, "avg": tsp_avg, "count": tsp_count},
                            "asp": {"sum": asp_sum, "avg": asp_avg, "count": asp_count},
                            "year": year,
                            "month": month,
                            "day": day
                        }
                    }
                    cnd_sen_data_finder=cnd_sen_data.find({'device_id': device_id},{'hour': hour})
                    if cnd_sen_data_finder:
                        destination_collection.update_one(filter_query, update_query)
                    else:
                        cnd_sen_hourly.insert(new_document)
                    # cnd_sen_daily.insert(new_document)
                    cnd_sen_data_daily=cnd_sen_hourly.find({'device_id': device_id},{'day': day})
                    if cnd_sen_data_daily:
                        cnd_sen_daily.update_one(filter_daily, update_query)
                    else:
                        cnd_sen_daily.insert(new_document)
                print("Data inserteeeed",new_document)


                # # Pipeline stages for aggregation
                # pipeline = [
                #     {
                #         "$group": {
                #             "_id": {
                #                 "device_id": "$device_id",
                #                 "hour": "$hour"
                #             },
                #             "cnd_sum": {"$sum": {"$toInt": {"$ifNull": ["$cnd", 0]}}},
                #             "cnd_avg": {"$avg": {"$toDouble": {"$ifNull": ["$cnd", 0]}}},
                #             "cnd_count": {"$sum": {"$cond": [{"$ifNull": ["$cnd", False]}, 1, 0]}},
                            
                #             "tsp_sum": {"$sum": {"$toInt": {"$ifNull": ["$tsp", 0]}}},
                #             "tsp_avg": {"$avg": {"$toDouble": {"$ifNull": ["$tsp", 0]}}},
                #             "tsp_count": {"$sum": {"$cond": [{"$ifNull": ["$tsp", False]}, 1, 0]}},
                #             "spn_sum": {"$sum": {"$toInt": {"$ifNull": ["$spn", 0]}}},
                #             "spn_avg": {"$avg": {"$toDouble": {"$ifNull": ["$spn", 0]}}},
                #             "spn_count": {"$sum": {"$cond": [{"$ifNull": ["$spn", False]}, 1, 0]}},
                #             "asp_sum": {"$sum": {"$toInt": {"$ifNull": ["$asp", 0]}}},
                #             "asp_avg": {"$avg": {"$toDouble": {"$ifNull": ["$asp", 0]}}},
                #             "asp_count": {"$sum": {"$cond": [{"$ifNull": ["$asp", False]}, 1, 0]}}
                #         }
                #     },
                #     {
                #         "$set": {
                #             "cnd_avg": {"$cond": [{"$eq": ["$cnd_count", 0]}, 0, "$cnd_avg"]},
                #             "spn_avg": {"$cond": [{"$eq": ["$spn_count", 0]}, 0, "$spn_avg"]},
                #             "tsp_avg": {"$cond": [{"$eq": ["$tsp_count", 0]}, 0, "$tsp_avg"]},
                #             "asp_avg": {"$cond": [{"$eq": ["$asp_count", 0]}, 0, "$asp_avg"]}
                #         }
                #     }
                # ]

                # # Perform aggregation on the source collection
                # result = cnd_sen_data.aggregate(pipeline)

                # # Iterate over the aggregation result
                # for item in result:
                #     device_id = item["_id"]["device_id"]
                #     hour = item["_id"]["hour"]
                #     cnd_sum = item["cnd_sum"]
                #     cnd_avg = item["cnd_avg"]
                #     cnd_count = item["cnd_count"]
                #     spn_sum = item["spn_sum"]
                #     spn_avg = item["spn_avg"]
                #     spn_count = item["spn_count"]
                #     tsp_sum = item["tsp_sum"]
                #     tsp_avg = item["tsp_avg"]
                #     tsp_count = item["tsp_count"]
                #     asp_sum = item["asp_sum"]
                #     asp_avg = item["asp_avg"]
                #     asp_count = item["asp_count"]

                #     # Create the new document for the destination collection
                #     new_document = {
                #         "device_id": device_id,
                #         "service": "cnd",
                #         "cnd": {"sum": cnd_sum, "avg": cnd_avg, "count": cnd_count},
                #         "spn": {"sum": spn_sum, "avg": spn_avg, "count": spn_count},
                #         "tsp": {"sum": tsp_sum, "avg": tsp_avg, "count": tsp_count},
                #         "asp": {"sum": asp_sum, "avg": asp_avg, "count": asp_count},
                #         "month": last_document["_id"]["month"],  # Placeholder for month value, can be set later
                #         "year": last_document["_id"]["year"],  # Placeholder for year value, can be set later
                #         "day": last_document["_id"]["day"],   # Placeholder for day value, can be set later
                #         "hour": hour
                #     }

                #     # Insert the new document into the destination collection
                #     #hour
                #     cnd_sen_hourly.insert_one(new_document)

                # all_cnd_sen_data=cnd_sen_data.find(query)
                # for c in all_cnd_sen_data:
                #     print("all cnd_sen_data:",c)
                # last_document = cnd_sen_data.find_one({}, sort=[('_id', pymongo.DESCENDING)])
                # # last_document = cnd_sen_data.find()
                # cnd_hourlast_document = cnd_sen_hourly.find_one({}, sort=[('_id', pymongo.DESCENDING)])
                # cnd_monthlylast_document = cnd_sen_daily.find_one({}, sort=[('_id', pymongo.DESCENDING)])
                # cnd_yearlylast_document = cnd_sen_monthly.find_one({}, sort=[('_id', pymongo.DESCENDING)])
            
                # for document in all_cnd_sen_data:
                #         timestamp = document.get('device_id')
                #         all_deviceids.append(timestamp)
                # if last_document:
                #     print('device id:', all_cnd_sen_data)
                #     hour_data = last_document.get('hour')
                #     print("Last doc:",last_document)
                    
                #     if hour_data !=dd[3] or device_id not in all_deviceids:
                #         # Hour
                #         # cnd_sen_hourly.insert(insert_data)
                #         pass
                #     else:
                #         pass
                #         # cnd_sen_hourly.update(insert_data)
                # if cnd_hourlast_document:
                #     daily_data = cnd_hourlast_document.get('day')
                #     if daily_data !=dd[2] or device_id not in all_deviceids:
                #         #daily
                #         cnd_sen_daily.insert(insert_data)
                # if cnd_monthlylast_document:
                #     monthly_data = cnd_monthlylast_document.get('month')
                #     if monthly_data !=dd[1] or device_id not in all_deviceids:
                #         #monthly
                #         cnd_sen_monthly.insert(insert_data)
                # if cnd_yearlylast_document:
                #     monthly_data = cnd_yearlylast_document.get('year')
                #     if monthly_data !=dd[0] or device_id not in all_deviceids:
                #         #yearly
                #         cnd_sen_yearly.insert(insert_data)
                #minut table        
                print("datada inserted")
        except Exception as e:
            print(e)
            print("sss")

