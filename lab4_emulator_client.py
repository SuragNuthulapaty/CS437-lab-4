# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#Path to the dataset, modify this
data_path = "./data2/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "./certs/{}_cert.pem"
key_formatter = "./certs/{}_private.key"

# Get certs
thing_names = []
with open("./certs/thing_names.txt") as thing_f:
    thing_names = thing_f.read().splitlines()
    print(f"Found {len(thing_names)} certificates.")

thing_cert_map = {name: {"cert": certificate_formatter.format(name), "key": key_formatter.format(name)} for name in thing_names}

# Define what subset of clients we want to spin up:
device_st = 0
device_end = 103

assert abs(device_end - device_st) < len(thing_names), "Trying to start up more clients than there are certs for!"

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a3uj80vbbb57mr-ats.iot.us-east-2.amazonaws.com", 8883)
        self.client.configureCredentials("./keys/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5) # 5 sec

        self.client.subscribe(f"iot/Vehicle_veh{self.device_id}", 0, self.green_grass_return)

        self.df = pd.read_csv(data_path.format(np.random.randint(0, 5)))
        self.cur_idx = 0
        

    def green_grass_return(self, client, userdata, message):
        print(f"device {self.device_id} received payload {message.payload} from topic {message.topic}")


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, topic="vehicle/emission/data"):
        # Load the vehicle's emission data
        if self.cur_idx < 0 or self.cur_idx >= len(self.df):
            return

        row = self.df.iloc[self.cur_idx]
        # Create a JSON payload from the row data
        pload = row.to_dict()
        pload["vehicle_device_id"] = self.device_id
        payload = json.dumps(pload)

        print(payload)
        
        # Publish the payload to the specified topic
        print(f"[{self.device_id}] {payload} Publishing to {topic}")
        self.client.publishAsync(topic, payload, 0, ackCallback=self.customPubackCallback)
            
            # Sleep to simulate real-time data publishing
            # time.sleep(1) # the csv timesteps are 1s apart
        self.cur_idx += 1



# print("Loading vehicle data...")
# data = []
# for i in range(5):
#     a = pd.read_csv(data_path.format(i))
#     data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    device_name = thing_names[device_id]
    device_certificate = thing_cert_map[device_name]["cert"]
    device_key = thing_cert_map[device_name]["key"]
    print(f"Starting client [{device_id}]:{device_name}  (Certfiles: {device_certificate}, {device_key})")
    client = MQTTClient(device_id, device_certificate, device_key)
    client.client.connect()
    clients.append(client)
 

# while True:
#     print("send now?")
#     x = input()
#     if x == "s":
#         for i,c in enumerate(clients):
#             c.publish()

#     elif x == "d":
#         for c in clients:
#             c.client.disconnect()
#         print("All devices disconnected")
#         exit()
#     else:
#         print("wrong key pressed")

#     time.sleep(3)

while True:
    for i,c in enumerate(clients):
        c.publish()

    time.sleep(1)





