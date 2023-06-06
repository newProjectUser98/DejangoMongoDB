from django.shortcuts import render
from django.http import HttpResponse
import paho.mqtt.client as mqtt
import json
from django.http import JsonResponse
from myfirstapp.mqtt import client as mqtt_client


# Create your views here.
def index(request):
    return HttpResponse("<h1>Hello and welcome to my first <u>Django App</u> project!</h1>")

import pymongo
from django.conf import settings
my_client = pymongo.MongoClient('mongodb://localhost:27017/mydb')

# First define the database name
dbname = my_client['sample_medicines']

# Now get/create collection name (remember that you will see the database in your mongodb cluster only after you create a collection)
collection_name = dbname["medicinedetails"]

#let's create two documents
medicine_1 = {
    "medicine_id": "RR000123456",
    "common_name" : "Paracetamol",
    "scientific_name" : "",
    "available" : "Y",
    "category": "fever"
}
medicine_2 = {
    "medicine_id": "RR000342522",
    "common_name" : "Metformin",
    "scientific_name" : "",
    "available" : "Y",
    "category" : "type 2 diabetes"
}

collection_name.insert_many([medicine_1,medicine_2])

med_details = collection_name.find({})

for r in med_details:
	print(r["common_name"])

update_data = collection_name.update_one({'medicine_id':'RR000123456'}, {'$set':{'common_name':'Paracetamol 500'}})
delete_data = collection_name.delete_one({'medicine_id':'RR000123456'})
# print(collection_name.count())




def publish_message(request):
    request_data = json.loads(request.body)
    rc, mid = mqtt_client.publish(request_data['topic'], request_data['msg'])
    return JsonResponse({'code': rc})







