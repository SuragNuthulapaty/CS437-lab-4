import json
import os
import logging
import sys
import boto3

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# connect to the iot-data stream
REGION = "us-east-2"
iot_client = boto3.client("iot-data", region_name=REGION)

# file for persistent storage between function calls
LOCAL_FILE = "/tmp/max_co2.json"

def load_local_data():
    if os.path.exists(LOCAL_FILE):
        with open(LOCAL_FILE, "r") as f:
            return json.load(f)
    return {}

def save_local_data(data):
    with open(LOCAL_FILE, "w") as f:
        json.dump(data, f)

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    # extract vehicle id and emissions from the inbound packet
    vehicle_stat = event['vehicle_device_id']
    CO2_val = float(event['vehicle_CO2'])

    # load the local file into a dict
    max_data = load_local_data()

    # update the dict if necessary with the max data for the specific vehicle
    if vehicle_stat not in max_data or CO2_val > max_data[vehicle_stat]:
        max_data[vehicle_stat] = CO2_val
        save_local_data(max_data)

    # send the maximum emissions back to the vehicle on the specific MQTT topic
    iot_client.publish(
        topic=f"iot/Vehicle_veh{vehicle_stat}",
        qos=1,
        payload=json.dumps({"max_CO2": max_data[vehicle_stat]}),
    )

    # output status (only used for debugging)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "vehicle_id": vehicle_stat,
            "max_CO2": max_data[vehicle_stat]
        })
    }
