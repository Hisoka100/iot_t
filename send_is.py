import paho.mqtt.client as mqtt
import json
from ISStreamer.Streamer import Streamer
# DF

# Set up your Initial State bucket key and access key
BUCKET_NAME = "DEVICE_1"
BUCKET_KEY = "XEKT3YPHKX8F"
# ACCESS_KEY = "ist_yc9wtnOYvumLsVG9dsh0ybztajzT3M47"
ACCESS_KEY = "ist_EUPMYty24UaBVm60PEJSI83pTybC2oo6"

# Initialize Initial State Streamer
# streamer = Streamer(bucket_name=BUCKET_NAME, bucket_key=BUCKET_KEY, access_key=ACCESS_KEY)

# MQTT parameters
MQTT_BROKER = "broker.hivemq.com" 
MQTT_PORT = 1883                 
MQTT_TOPIC = "Chapisha/Mtandaoni"      

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    print("Connected to broker with result code: " + str(rc))
    client.subscribe(MQTT_TOPIC)

# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    try:
        # Decode the JSON payload
        payload = msg.payload.decode('utf-8')
        sensor_data = json.loads(payload)

        # Extract the sensor data
        soil_moisture = sensor_data.get("soil_moisture", None)
        irrigation_amount = sensor_data.get("irrigation_amount", None)
        valve_state = sensor_data.get("valve_state", None)
        humidity = sensor_data.get("Humidity", None)
        temperature = sensor_data.get("Temperature", None)
        device_id = sensor_data.get("DeviceId", None)
        streamer = Streamer(bucket_name=BUCKET_NAME, bucket_key=device_id, access_key=ACCESS_KEY)
        # Print the received data
        print(f"Received Data: Soil Moisture: {soil_moisture}, Irrigation Amount: {irrigation_amount}, Valve State: {valve_state}, Temp: {temperature}, humidity{humidity}, device id{device_id}")

        # Send the data to Initial State
        if soil_moisture is not None:
            streamer.log("Soil Moisture", soil_moisture)
        if irrigation_amount is not None:
            streamer.log("Irrigation Amount", irrigation_amount)
        if valve_state is not None:
            streamer.log("Valve State", valve_state)
        if humidity is not None:
            streamer.log("Humidity", humidity)
        if temperature is not None:
            streamer.log("Temperature", temperature)          
        # Ensure data is pushed to Initial State
        streamer.flush()

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Setup MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Start the MQTT loop to process messages and reconnect automatically
client.loop_forever()