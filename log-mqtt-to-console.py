#!/usr/bin/python3
 
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import json
import ssl
import os
import datetime
import pyvcloud.vcd.client
import pyvcloud.vcd.vdc
 
load_dotenv()
vcdHost = os.environ.get('vcdHost')
vcdPort = os.environ.get('vcdPort')
path = "/messaging/mqtt"
logFile = 'vcd_log.log'
 
#org admin credentials
user = os.environ.get('user')
password = os.environ.get('password')
org = os.environ.get('org')


credentials = pyvcloud.vcd.client.BasicLoginCredentials(user, org, password)
vcdClient = pyvcloud.vcd.client.Client(vcdHost+":"+str(vcdPort),None,False,logFile)
vcdClient.set_credentials(credentials)
accessToken = vcdClient.get_access_token()
headers = {"Authorization": "Bearer "+ accessToken}
 
if max(vcdClient.get_supported_versions_list()) < "34.0":
    exit('VMware Cloud Director 10.1 or newer is required')
 
org = vcdClient.get_org_list()
orgId = (org[0].attrib).get('id').split('org:',1)[1]



# channel = "publish/"+orgId+"/*"
channel = "publish/#"

 
def on_message(client, userdata, message):
    topic = message.topic
    event = message.payload.decode('utf-8')
    event = event.replace('\\','')
    eventPayload = event.split('payload":"',1)[1]
    eventPayload = eventPayload[:-2]
    event_json = json.loads(eventPayload)
    print("\n" + datetime.datetime.now().replace(microsecond=0).isoformat() + " " + topic)
    print (event_json)
 
# Enable for logging
# def on_log(client, userdata, level, buf):
#     print("log: ",buf)
 
client = mqtt.Client(client_id = "PythonMQTT",transport = "websockets")
client.ws_set_options(path = path, headers = headers)
# client.tls_set()
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.on_message=on_message
# client.on_log=on_log  #enable for logging
client.connect(host = vcdHost, port = int(vcdPort) , keepalive = 60)
print('Connected')
client.subscribe(channel)
client.loop_forever()