
import paho.mqtt.client as mqtt
from django.conf import settings
from .processmqtt_sub_data import processmqtt_sub_data

def on_connect(mqtt_client, userdata, flags, rc):
    if rc == 0:
        # print('Connected successfully')
        mqtt_client.subscribe('wc/#')
    else:
        # print('Bad connection. Code:', rc)
        pass


def on_message(mqtt_client, userdata, msg):
    processmqtt_sub_data(msg)
    print(f'Received message on topic: {msg.topic} with payload: {msg.payload}')


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(settings.MQTT_USER, settings.MQTT_PASSWORD)
client.connect(
    host=settings.MQTT_SERVER,
    port=settings.MQTT_PORT,
    keepalive=settings.MQTT_KEEPALIVE
)
