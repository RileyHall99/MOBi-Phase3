"""
File: Mobi.py
Description: This code processes data from the scale and mill/loading site and uploads it to AWS and the OPCUA server
Author: Zion Chong
Created: 2024-07-4
Detail: Has variance and old for OPCUA location 1/0 Changes from 02_26 test 
"""

# a3hf0azjmdr1le-ats.iot.us-east-2.amazonaws.com
# Website URL https://main.d21g68q1z0ru64.amplifyapp.com/

import datetime
import ssl
import json
import threading
import serial
import re
import time
import csv
import requests
import os
import paho.mqtt.client as mqtt
from opcua import Client, ua
import atexit
import socket
import serial.tools
import serial.tools.list_ports
from threading import Timer

# Serial connection baudrate
baudrate_scale = 9600
baudrate_loc = 115200



FORMAT = r"^(\-?\d+\.?\d*)"
SLEEP_TIME = 1

last_known_location : str = "0"
location_status : bool = False
first_load : bool = False
c_t1 = None

def get_correct_ports()-> list: 
    print("Searching for available COM ports...")
    ports = list(serial.tools.list_ports.comports())
    return ports

ports = get_correct_ports()
# ports = None
port_scale : str = ""
port_loc : str = ""
print(f"ports!!! ==>> {ports}")
if(ports):
    for i in ports: 
        print(f"ports {i.description}")
        if("Silicon Labs CP210x USB to UART Bridge" in i.description ):
            port_loc = i.device
            print(f"PORT LOC ==>>{port_loc}")
        elif("USB Serial Port" in i.description):
            port_scale = i.device
else:
    port_scale = "COM7" #Default ports ==> Scale Receiver 
    port_loc = "COM3" #Default ports ==> LoRa 
# Default Values
DEFAULTS = {
    "BUCKET_WEIGHT": 1.3, #INFO ==>> Old Value 1710
    "MAX_RESIDUAL_WEIGHT": .2,  # kg, maximum deviation from bucket weight OLD Value ==>> 100
    "MIN_MATERIAL_WEIGHT": 3,  # kg, minimum material loaded ==>> MIN_MATERIAL_WEIGHT ==>> 1500
    "MIN_WEIGHT_DROP": 3,     # kg, minimum material unloaded ==>> MIN WEIGHT DROP ==>> 1500
    "STABILITY_VARIANCE": 20,    # kg², variance threshold ==>> 50
    "TIMEOUT": 600,             # seconds
    "WEIGHT_BUFFER_COUNT": 5    # Size of weight buffer
}

# Initialize with defaults
BUCKET_WEIGHT = DEFAULTS["BUCKET_WEIGHT"]
MAX_RESIDUAL_WEIGHT = DEFAULTS["MAX_RESIDUAL_WEIGHT"]
MIN_MATERIAL_WEIGHT = DEFAULTS["MIN_MATERIAL_WEIGHT"]
MIN_WEIGHT_DROP = DEFAULTS["MIN_WEIGHT_DROP"]
STABILITY_VARIANCE = DEFAULTS["STABILITY_VARIANCE"]
TIMEOUT = DEFAULTS["TIMEOUT"]
WEIGHT_BUFFER_COUNT = DEFAULTS["WEIGHT_BUFFER_COUNT"]

def get_valid_input(prompt, default):
    """Prompt for an integer input, return default if Enter is pressed."""
    while True:
        value = input(prompt)
        if value == "":  # Enter pressed
            return default
        try:
            return int(value)
        except ValueError:
            print("Invalid input. Please enter an integer or press Enter for default.")

# Ask user if they want to customize parameters
cont = input("Press Enter to use default parameters or any key to customize: ")
if cont == "":
    print("Using default parameters:")
else:
    print("Enter new values (press Enter to keep default):")
    BUCKET_WEIGHT = get_valid_input(
        f"Enter bucket weight (kg) [default: {DEFAULTS['BUCKET_WEIGHT']}]: ",
        DEFAULTS["BUCKET_WEIGHT"]
    )
    MAX_RESIDUAL_WEIGHT = get_valid_input(
        f"Enter max residual weight (kg) [default: {DEFAULTS['MAX_RESIDUAL_WEIGHT']}]: ",
        DEFAULTS["MAX_RESIDUAL_WEIGHT"]
    )
    MIN_MATERIAL_WEIGHT = get_valid_input(
        f"Enter min material weight (kg) [default: {DEFAULTS['MIN_MATERIAL_WEIGHT']}]: ",
        DEFAULTS["MIN_MATERIAL_WEIGHT"]
    )
    MIN_WEIGHT_DROP = get_valid_input(
        f"Enter min weight drop (kg) [default: {DEFAULTS['MIN_WEIGHT_DROP']}]: ",
        DEFAULTS["MIN_WEIGHT_DROP"]
    )
    STABILITY_VARIANCE = get_valid_input(
        f"Enter stability variance (kg²) [default: {DEFAULTS['STABILITY_VARIANCE']}]: ",
        DEFAULTS["STABILITY_VARIANCE"]
    )
    TIMEOUT = get_valid_input(
        f"Enter timeout (seconds) [default: {DEFAULTS['TIMEOUT']}]: ",
        DEFAULTS["TIMEOUT"]
    )
    WEIGHT_BUFFER_COUNT = get_valid_input(
        f"Enter weight buffer (kg) [default: {DEFAULTS['WEIGHT_BUFFER_COUNT']}]: ",
        DEFAULTS["WEIGHT_BUFFER_COUNT"]
    )
    print()  # Newline for readability

# Display the final parameters
print("Final Parameters:")
print(f"  Bucket Weight: {BUCKET_WEIGHT} kg")
print(f"  Max Residual Weight: {MAX_RESIDUAL_WEIGHT} kg")
print(f"  Min Material Weight: {MIN_MATERIAL_WEIGHT} kg")
print(f"  Min Weight Drop: {MIN_WEIGHT_DROP} kg")
print(f"  Stability Variance: {STABILITY_VARIANCE} kg²")
print(f"  Timeout: {TIMEOUT} seconds")
print(f"  Weight Buffer: {WEIGHT_BUFFER_COUNT} kg")
print()

# OPCUA Server URL
OPCUA_Server_URL = "opc.tcp://admin@localhost:4840/freeopcua/server/"

# API URL
location_api_url = "https://hvmo05uh9d.execute-api.us-east-2.amazonaws.com/dev/"

# MQTT Host
Mqtt_host = "a3hf0azjmdr1le-ats.iot.us-east-2.amazonaws.com"

# Certificate paths using relative directory
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")
CA_CERT = os.path.join(CERT_DIR, "rootCA.pem")
CLIENT_CERT = os.path.join(CERT_DIR, "certificate.pem.crt")
PRIVATE_KEY = os.path.join(CERT_DIR, "private.pem.key")

# Initialize Serial Connections with Locks
serLoc = None
serScale = None

try:
    serScale = serial.Serial(port_scale, baudrate_scale)
    scale_lock = threading.Lock()
    print("Serial Scale opened", serScale.name)
except serial.SerialException as e:
    print(f"Scale Serial Exception: {e}")
    print("Please check if scale receiver is connected exiting...\n")
    exit()

try:
    serLoc = serial.Serial(port_loc, baudrate_loc)
    loc_lock = threading.Lock()
    print("Serial Location opened", serLoc.name)
except serial.SerialException as e:
    print(f"Location Serial Exception: {e}")
    print("Please check if Lora is connected exiting...\n")
    exit()

serLoc.write(b"AT+PARAMETER=10,7,2,7\r\n")
time.sleep(1)
serLoc.write(b"AT+NETWORKID=4\r\n")
time.sleep(1)
serLoc.write(b"AT+ADDRESS=100\r\n")

try:
    data_loc = serLoc.readline().decode().strip()
    print("Location receiver setup: " + data_loc)
except serial.SerialException as e:
    print(f"Location Serial Exception: {e} \ndata_loc: {data_loc}")
    print("Please check if Lora is connected exiting...\n")
    exit()

# Global flag to signal thread termination
stop_threads = False

# MQTT Client Setup
def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to AWS IoT: " + str(rc))

# Global flag to signal test mode 
# 0 = Production mode
# 1 = test mode By pass AWS / internet connection but still send to OPCUA
# 2 = test mode By pass AWS / internet connection and bypass OPCUA
testmode = 0

is_connected_internet_AWS = False

if testmode == 0:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    
    # Verify certificate files exist
    cert_files = [CA_CERT, CLIENT_CERT, PRIVATE_KEY]
    for cert_file in cert_files:
        if not os.path.exists(cert_file):
            print(f"Error: Certificate file not found: {cert_file}")
            exit()
            
    client.tls_set(
        ca_certs=CA_CERT,
        certfile=CLIENT_CERT,
        keyfile=PRIVATE_KEY,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )

    #Can remove for production
    #client.tls_insecure_set(True)

    # Connect to AWS IoT with 3 retries
    num_retries = 3
    for attempt in range(1, num_retries + 1):
        try:
            msg = client.connect(Mqtt_host, 8883, 60)
            client.loop_start()
            is_connected_internet_AWS = True
            if msg == 0:
                print("AWS Connection established.")
                break  # Exit the loop on successful connection
            else:
                print("Connection attempt", attempt, "failed:", msg)
        except Exception as e:
            print("Error in connection attempt", attempt, ":", e)

        if attempt == num_retries:  # Check if this is the last attempt
            print(
                "Connection failed after",
                num_retries,
                "retries. Please ensure the device is connected to the internet and try again. Exiting...",
            )
            exit()

        print("Retrying connection in 5 seconds...")
        time.sleep(5)

    # Sleep for 10 seconds to let MQTT connect (might need adjustment)
    time.sleep(10)

# check both Mqtt and API
def check_network_connection():
    if testmode == 1 or testmode == 2:
        return
    try:
        global is_connected_internet_AWS
        res = socket.gethostbyname(Mqtt_host)
        response = requests.post(location_api_url)

        # Check if the request was successful
        if response.status_code == 200 and res is not None:
            is_connected_internet_AWS = True
            # print("Network Connection OK")
        else:
            print("Failed to retrieve data. Status code:")
            is_connected_internet_AWS = False

    except socket.gaierror as e:
        is_connected_internet_AWS = False
        print("Unable to resolve the hostname No Internet: ", e)
    except requests.ConnectionError as e:
        is_connected_internet_AWS = False
        print("No internet connection. Please check your network.", e)
    except requests.Timeout as e:
        is_connected_internet_AWS = False
        print("Request timed out. The server took too long to respond.", e)
    except requests.RequestException as e:
        is_connected_internet_AWS = False
        print("An unexpected error occurred:", e)
    except requests.RequestException as e:
        print("Error occurred:", e)

# Capture weights from scale about 1 second per execution  
def scaleWeight():
    t1 = time.time()
    count = 0
    while count < 3:
        with scale_lock:
            try:
                serScale.timeout = 2  # Set a 1-second timeout
                serScale.reset_input_buffer()
                
                data_scale = (
                    serScale.readline().decode("utf-8", errors="ignore").strip()
                )
                # serScale.timeout = None  # Reset the timeout

                if data_scale:
                    match = re.search(FORMAT, data_scale)
                    if match:
                        t2 = time.time()
                        weight = float(match.group(1))
                        return weight
                    else:
                        print("Scale is NOT ON")
                        count += 1
                else:
                    print(
                        "Timeout: No data received from scale. PLEASE TURN ON SCALE RECEIVER"
                    )
                    count+=1
                    
            except serial.SerialException as e:
                print(f"Serial Exception: {e}")
                break
            except Exception as e:
                print(f"An error occurred: {e}")
                break
    return 0
# Heartbeat Recive from location
def heartbeat_recive(location):
    if testmode == 1 or testmode == 2:
        return
    Time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    d = "D" + location
    data = {"Device": d, "Status": "Online", "LastTimestamp": Time}
    
    try:
        payload = json.dumps(data)
        client.publish(
            "raspi/mobi_heartbeat",
            payload=payload,
            qos=0,
            retain=False,
        )
        print(f"Heartbeat sent to AWS from {d} at {Time}")
    except Exception as e:  # Catch other potential exceptions during publishing
        print(f"An error occurred: {e}")
        print("Heartbeat Recived but not sent\n")
    

def process_input_data(data_loc : str):
    print(f"PROCESSING DATA ==>> {data_loc}")
    try:
        if data_loc == "+OK" or data_loc == "" or "ERR" in data_loc.upper():
        #If returning +Ok it could be the LoRa power cycling and might need the parameter rerun 
            print("RETURNING ERROR!")
            return ["error"]
        else:
                t1 = time.time()
                print(f"THIS IS DATA RECEIVED ==>> {data_loc} ")
                data = (
                    data_loc.split(",")[2]
                    .replace("#", ",")
                    .replace(" : ", ",")
                    .replace("'", "")
                    .replace("{", "")
                    .replace("}", "")
                    .split(",")
                )
                type = data[1].strip()
                mill = data[3].strip()
    except (IndexError):
        print(f"INDEX OUT OF RANGE! THIS IS DATA ==>> {data_loc}")
        return ["error"]
    except (AttributeError):
        print(f"LIST HAS BEEN SENT NOT STRING! THIS IS DATA ==>> {data_loc} ")
        return ['error']

    return[type,mill]

# Recives data via Lora from the location
def mill_recive():
    try:
        #Change 2 readline waits until there is a new line character read in. So if that never comes it can take a while for it to finish
        #Also changing so that the data is processed in another function to deal exit the threading.lock quickly 
        serLoc.timeout = 2
        data_loc = serLoc.readline().decode().strip()
        serLoc.reset_input_buffer()
        serLoc.reset_output_buffer()
        return data_loc
    except serial.SerialException as e:
        print(f"Location Serial Exception: {e} \ndata_loc: {data_loc}")
        return ["error"]
    except IndexError as e:
        print(f"Index Exception: {e} \ndata_loc: {data_loc}")
        return ["error"]
    except Exception as e:
        print(f"An error occurred: {e}")
        return ["error"]

# To verify the connection to AWS
def Connection_Verification(location, Leavetime, outweight):
    # Headers
    headers = {"Content-Type": "application/json"}

    # Payload (request body)
    payload = {"recloc": location, "systemno": "1"}

    # Making the POST request
    try:
        response = requests.post(
            location_api_url, headers=headers, data=json.dumps(payload)
        )
        print(f"RESPONSE STATUS ==>> {response.status_code} RESPONSE MESSAGE ==>> {response.json()}")
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            result = response.json()
            data = result.get(
                "body", result
            )  # `body` if it's nested, otherwise the entire result

            # Convert `data` if it is a JSON string
            if isinstance(data, str):
                data = json.loads(data)

            print(data)
            if Leavetime == data["leavetime"] and outweight == data["outweight"]:
                print("Connection Verification PASSED")
                return True
            else:
                print(
                    "Data does not match uploaded data. "
                    + "Recorded data: "
                    + str(Leavetime)
                    + " "
                    + str(outweight)
                    + " Received data: "
                    + str(data["leavetime"])
                    + str(data["outweight"])
                )
                return False

        else:
            print("Failed to retrieve data. Status code:", response.status_code)
            print("Response:", response.text)
            return False

    except requests.RequestException as e:
        print("Error occurred:", e)
        return False
    except Exception as e:
        print("An error occurred:", e)
        return False

# sends data to OPCUA
def OPCUA_Upload(location, leavetime, outweight, arrivetime, inweight):
    def _get_variant_type(value):
        if isinstance(value, bool):
            return ua.VariantType.Boolean
        elif isinstance(value, (int, float)):
            return ua.VariantType.Double
        elif isinstance(value, str):
            return ua.VariantType.String
        return None

    if testmode == 1:
        pass
    elif testmode == 2:
        return
    elif not is_connected_internet_AWS:
        print("Not connected to the internet or can't reach AWS")
        return False

    try:
        client = Client(OPCUA_Server_URL)
        client.connect()
        root = client.get_root_node()
        objects = root.get_children()[0]
        print(f"THIS IS LOCATION AT LINE #462 ==> {location}")
        if location == "0":
            loading_zone = objects.get_child(["2:Loading Zone"])
            loading = loading_zone.get_children()[0]
            loading_vars = {var.get_browse_name().Name: var for var in loading.get_children()}
            print(f"Vars in loading: {list(loading_vars.keys())}")
            data = {
                "Loading": {
                    "Loading Arrive Time": arrivetime,
                    "Loading Arrive Weight": inweight,
                    "Loading Leave Time": leavetime,
                    "Loading Leave Weight": outweight,
                }
            }
            for var_name, value in data["Loading"].items():
                try:
                    if var_name in loading_vars:
                        variant_type = _get_variant_type(value)
                        print(f"setting{var_name} to {value} type {variant_type}")
                        if variant_type:
                            loading_vars[var_name].set_value(value, varianttype=variant_type)
                        else:
                            loading_vars[var_name].set_value(value)
                except Exception as e:
                    print(f"Error updating {var_name} in Loading: {e}")
                    return False
            time.sleep(1)    
            data = {
                "Loading": {
                    "Loading Arrive Weight": 0.0,
                    "Loading Leave Weight": 0.0,
                }
            }
            for var_name, value in data["Loading"].items():
                try:
                    if var_name in loading_vars:
                        variant_type = _get_variant_type(value)
                        if variant_type:
                            loading_vars[var_name].set_value(value, varianttype=variant_type)
                            print(f"Reset {var_name} tp 0.0")
                        else:
                            loading_vars[var_name].set_value(value)
                            print(f"Reset {var_name} tp 0.0")
                except Exception as e:
                    print(f"Error updating {var_name} in Loading: {e}")
                    return False    
            return True

        elif location in ["1", "2", "3", "4", "5", "6"]:
            mill_name = f"Mill {location}"
            mills_folder = objects.get_child(["2:Mills"])
            mill_folder = None
            for child in mills_folder.get_children():
                if child.get_browse_name().Name == mill_name:
                    mill_folder = child
                    break
            if mill_folder is None:
                print(f"Error: Mill folder {mill_name} not found")
                return False

            mill_vars = {var.get_browse_name().Name: var for var in mill_folder.get_children()}
            data = {
                mill_name: {
                    f"{mill_name} Arrive Time": arrivetime,
                    f"{mill_name} Arrive Weight": inweight,
                    f"{mill_name} Leave Time": leavetime,
                    f"{mill_name} Leave Weight": outweight,
                }
            }
            for var_name, value in data[mill_name].items():
                try:
                    if var_name in mill_vars:
                        variant_type = _get_variant_type(value)
                        if variant_type:
                            mill_vars[var_name].set_value(value, varianttype=variant_type)
                        else:
                            mill_vars[var_name].set_value(value)
                except Exception as e:
                    print(f"Error updating {var_name} in {mill_name}: {e}")
                    return False
            time.sleep(1)
            data = {
                mill_name: {
                    f"{mill_name} Arrive Weight": 0.0,
                    f"{mill_name} Leave Weight": 0.0,
                }
            }
            for var_name, value in data[mill_name].items():
                try:
                    if var_name in mill_vars:
                        variant_type = _get_variant_type(value)
                        if variant_type:
                            mill_vars[var_name].set_value(value, varianttype=variant_type)
                        else:
                            mill_vars[var_name].set_value(value)
                except Exception as e:
                    print(f"Error updating {var_name} in {mill_name}: {e}")
                    return False
            return True

        else:
            print(f"Invalid Location: {location}")
            return False

    except Exception as e:
        print(f"OPCUA server not started\nError: {e}")
        return False
    finally:
        try:
            client.disconnect()
        except Exception as e:
            print("Failed to disconnect from OPCUA", e)
            return False
    

# Updates the heartbeat in OPCUA current heartbeat frecuency from Mill/Loading is every 10 minutes OPCUA HB resets every 1000 counts should be around 7 days
def OPCUA_Heartbeat(location):

    print(f"OPCUA HEARTBEAT ==>> LOCATION ==>> {location}")
    def _get_variant_type(value):
        if isinstance(value, int):
            return ua.VariantType.Int64
        return None

    if testmode == 1:
        pass
    elif testmode == 2:
        return
    elif not is_connected_internet_AWS:
        print("Not connected to the internet or can't reach AWS")
        return

    try:
        client = Client(OPCUA_Server_URL)
        client.connect()
        root = client.get_root_node()
        objects = root.get_children()[0]

        if location == "0":
            loading_zone = objects.get_child(["2:Loading Zone"])
            loading = loading_zone.get_children()[
                0
            ]  # Get the Loading folder inside Loading Zone
            loading_vars = {
                var.get_browse_name().Name: var for var in loading.get_children()
            }

            try:
                HB_value = loading_vars["Loading Heartbeat"].get_value()
                variant_type = _get_variant_type(HB_value)
                HB_value += 1
                if HB_value > 1000:
                    HB_value = 0
                if variant_type:
                    loading_vars["Loading Heartbeat"].set_value(
                        HB_value, varianttype=variant_type
                    )
                else:
                    loading_vars["Loading Heartbeat"].set_value(HB_value)
            except Exception as e:
                print(f"Error updating Loading Heartbeat to OPCUA in Loading: {e}")

        elif location in ["1", "2", "3", "4", "5", "6"]:
            mill_name = f"Mill {location}"
            mills_folder = objects.get_child(["2:Mills"])

            # Dynamically find the correct mill folder
            mill_folder = None
            for child in mills_folder.get_children():
                if child.get_browse_name().Name == mill_name:
                    mill_folder = child
                    break

            if mill_folder is None:  # Handle case where mill is not found
                print(f"Error: Mill folder {mill_name} not found")
                return

            mill_vars = {
                var.get_browse_name().Name: var for var in mill_folder.get_children()
            }

            try:
                HB_value = mill_vars[f"{mill_name} Heartbeat"].get_value()
                variant_type = _get_variant_type(HB_value)
                HB_value += 1
                #This value is changing from a 10000 to 1000
                if HB_value > 1000:
                    HB_value = 0
                if variant_type:
                    mill_vars[f"{mill_name} Heartbeat"].set_value(
                        HB_value, varianttype=variant_type
                    )
                else:
                    mill_vars[f"{mill_name} Heartbeat"].set_value(HB_value)
            except Exception as e:
                print(f"Error updating {mill_name} Heartbeat to OPCUA: {e}")
        elif location == "7":
            loading_zone = objects.get_child(["2:PC"])
            loading = loading_zone.get_children()[
                0
            ]  # Get the Loading folder inside Loading Zone
            loading_vars = {
                var.get_browse_name().Name: var for var in loading.get_children()
            }

            try:
                HB_value = loading_vars["PC_Heartbeat"].get_value()
                variant_type = _get_variant_type(HB_value)
                HB_value += 1
                if HB_value > 1000:
                    HB_value = 0
                if variant_type:
                    loading_vars["PC_Heartbeat"].set_value(
                        HB_value, varianttype=variant_type
                    )
                else:
                    loading_vars["PC_Heartbeat"].set_value(HB_value)
            except Exception as e:
                print(f"Error updating PC_Heartbeat to OPCUA in Loading: {e}")
        else:
            print(f"Invalid Location: {location}")

    except Exception as e:
        print(f"OPCUA server not started\nError: {e}")
    finally:
        try:
            client.disconnect()
        except Exception as e:
            print("Failed to disconnect from OPCUA", e)

# Updates the *live* weight in OPCUA
def OPCUA_Raw_weight(time, weight):
    if testmode == 1:
        pass
    elif testmode == 2:
        return
    elif not is_connected_internet_AWS:
        print("Not connected to the internet or can't reach AWS")
        return 

    try:
        client = Client(OPCUA_Server_URL)
        client.connect()
        root = client.get_root_node()
        objects = root.get_children()[0]

        RW_zone = objects.get_child(["2:Raw_Weight"])
        RW = RW_zone.get_children()[0]  # Get the Loading folder inside Loading Zone
        RW_vars = {var.get_browse_name().Name: var for var in RW.get_children()}
        try:
            RW_vars["RW_Weight"].set_value(weight, varianttype=ua.VariantType.Double)
            RW_vars["RW_Time"].set_value(time, varianttype=ua.VariantType.String)
        except Exception as e:
            print(f"Error updating Raw Weight to OPCUA in Loading: {e}")
            print(weight)

    except Exception as e:
        print(f"OPCUA server not started\nError: {e}")
    finally:
        try:
            client.disconnect()
        except Exception as e:
            print("Failed to disconnect from OPCUA", e)

def call_back_timer(): 
    global location_status
    location_status = False
    print(f"CALL BACK TIMER!!!!!!! ==>> {last_known_location}")
    client = Client(OPCUA_Server_URL)
    client.connect()
    root = client.get_root_node()
    objects = root.get_children()[0]
    if(last_known_location == "0"):

        loading_zone = objects.get_child(["2:Loading Zone"])
        loading = loading_zone.get_children()[
            0
        ]  # Get the Loading folder inside Loading Zone
        loading_vars = {
            var.get_browse_name().Name: var for var in loading.get_children()
        }
        loading_vars["Loading Status"].set_value(
        False, varianttype=ua.VariantType.Boolean
        )
    else:
        mill_name = f"Mill {last_known_location}"
        mills_folder = objects.get_child(["2:Mills"])

        # Dynamically find the correct mill folder
        mill_folder = None
        for child in mills_folder.get_children():
            if child.get_browse_name().Name == mill_name:
                mill_folder = child
                break

        if mill_folder is None:  # Handle case where mill is not found
            print(f"Error: Mill folder {mill_name} not found")
            exit()

        mill_vars = {
            var.get_browse_name().Name: var for var in mill_folder.get_children()
        }
        mill_vars[f"Mill {last_known_location} Status"].set_value(False, varianttype=ua.VariantType.Boolean)
        print("VALUE SWITCHED TO NEGATIVE")
    client.disconnect()
        
#Switching function to deal with constant positives negatives. Trying to nomalize the line. 
#TODO CLOSE THE THREADS!!!!

def OPCUA_Location_Status(location, status):
    global last_known_location
    global c_t1
    print(f"SETTING LOCATION STATUS {location}")
    if testmode == 1:
        pass
    elif testmode == 2:
        return
    elif not is_connected_internet_AWS:
        print("Not connected to the internet or can't reach AWS")
        return
    try:
        client = Client(OPCUA_Server_URL)
        client.connect()
        root = client.get_root_node()
        objects = root.get_children()[0]
        if(c_t1 is not None):
            c_t1.cancel()
        c_t1 = Timer(20.0,call_back_timer )
        print("TIMER CREATED!!!!!")
        if location == "0": 
            
            loading_zone = objects.get_child(["2:Loading Zone"])
            loading = loading_zone.get_children()[
                0
            ]  # Get the Loading folder inside Loading Zone
            loading_vars = {
                var.get_browse_name().Name: var for var in loading.get_children()
            }
            try:
            
                if(last_known_location == location):
                    if status == 0:
                        loading_vars["Loading Status"].set_value(
                            False, varianttype=ua.VariantType.Boolean
                        )
                    elif status == 1:
                        loading_vars["Loading Status"].set_value(
                            True, varianttype=ua.VariantType.Boolean
                        )
                    elif status == 2:
                        loading_vars["Loading Status"].set_value(
                            True, varianttype=ua.VariantType.Boolean
                        )
                        # time.sleep(1)
                        # loading_vars["Loading Status"].set_value(
                        #     False, varianttype=ua.VariantType.Boolean
                        # )
                        #TODO Implement a callback timer here ~10 seconds 
                        
                        c_t1.start()
                        print("TIMER STARTED!!!!")
                else:
                    c_t1.start()
                    print("TIMER STARTED ")
                    if(last_known_location != "0"):
                        mill_name = f"Mill {last_known_location}"
                        mills_folder = objects.get_child(["2:Mills"])
                        mill_folder = None
                        for child in mills_folder.get_children():
                            if child.get_browse_name().Name == mill_name:
                                mill_folder = child
                                break    
                            
                        print(f"hereeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee{mill_name}")  
                        mill_vars = {
                            var.get_browse_name().Name: var for var in mill_folder.get_children()
                        }
                        mill_vars[f"Mill {last_known_location} Status"].set_value(False , varianttype=ua.VariantType.Boolean)
                        last_known_location = location
                        print(f"LOCATION CHANGED ==>> {last_known_location}")
                    else:
                        loading_zone = objects.get_child(["2:Loading Zone"])
                        loading = loading_zone.get_children()[
                            0
                        ]  # Get the Loading folder inside Loading Zone
                        loading_vars = {
                            var.get_browse_name().Name: var for var in loading.get_children()
                        }
                        loading_vars["Loading Status"].set_value(
                            False, varianttype=ua.VariantType.Boolean
                        )
                    last_known_location = location

            except Exception as e:
                print(f"Error updating Loading Status to OPCUA in Loading: {e}")
        elif location in ["1", "2", "3", "4", "5", "6"]:
            mill_name = f"Mill {location}"
            mills_folder = objects.get_child(["2:Mills"])

            # Dynamically find the correct mill folder
            mill_folder = None
            for child in mills_folder.get_children():
                if child.get_browse_name().Name == mill_name:
                    mill_folder = child
                    break

            if mill_folder is None:  # Handle case where mill is not found
                print(f"Error: Mill folder {mill_name} not found")
                return

            mill_vars = {
                var.get_browse_name().Name: var for var in mill_folder.get_children()
            }
            if(last_known_location == location):
                try:
                    
                    if status == 0:
                        mill_vars[f"{mill_name} Status"].set_value(
                            True, varianttype=ua.VariantType.Boolean
                        )
                    elif status == 1:
                        mill_vars[f"{mill_name} Status"].set_value(
                            False, varianttype=ua.VariantType.Boolean
                        )
                    elif status == 2:
                        mill_vars[f"{mill_name} Status"].set_value(
                            True, varianttype=ua.VariantType.Boolean
                        )
                        # time.sleep(1)
                        # mill_vars[f"{mill_name} Status"].set_value(
                        #     False, varianttype=ua.VariantType.Boolean
                        # )a
                    
                        #TODO Implement a callback timer here ~10 seconds 
                        
                        c_t1.start()
                        print("TIMER STARTED LCATION IS THE SAME")
                except Exception as e:
                    print(f"Error updating {mill_name} Status to OPCUA: {e}")
            else:
                    if(last_known_location != "0"):
                        mill_name = f"Mill {last_known_location}"
                        mills_folder = objects.get_child(["2:Mills"])
                        mill_folder = None
                        for child in mills_folder.get_children():
                            if child.get_browse_name().Name == mill_name:
                                mill_folder = child
                                break    
                            
                        print(f"hereeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee{mill_name}")  
                        mill_vars = {
                            var.get_browse_name().Name: var for var in mill_folder.get_children()
                        }
                        mill_vars[f"Mill {last_known_location} Status"].set_value(False , varianttype=ua.VariantType.Boolean)
                        print(f"LOCATION CHANGED ==>> {last_known_location}")
                    else:
                        loading_zone = objects.get_child(["2:Loading Zone"])
                        loading = loading_zone.get_children()[
                            0
                        ]  # Get the Loading folder inside Loading Zone
                        loading_vars = {
                            var.get_browse_name().Name: var for var in loading.get_children()
                        }
                        loading_vars["Loading Status"].set_value(
                            False, varianttype=ua.VariantType.Boolean
                        )
                    last_known_location = location
                    
    except Exception as e:
        print(f"OPCUA server not started\nError: {e}")
    finally:
        try:
            client.disconnect()
        except Exception as e:
            print("Failed to disconnect from OPCUA", e)


def AWS_upload(data , etime , weight):
    
    print(f"DATA ==>> {data}")
    if testmode == 0:
        client.publish("raspi/mobi_loc", payload=json.dumps(data), qos=0, retain=False)
        print(f"IS CONNECTED?????? ==>> {client.is_connected()}")
        connectionstatus = False
        for i in range(3):
            if(data['location'] == "Loading"):
                connectionstatus = Connection_Verification("Loading", data['leavetime'], data['outweight'])
            else:
                connectionstatus = Connection_Verification(data['location'], data['leavetime'], data['outweight'])
                
            if connectionstatus:
                break
            time.sleep(5)
    else:
        connectionstatus = True


# Loading location fill up logic
def loadingStart(sysno):
    print("STARTING LOADING")
    global location_status
    global first_load
    heartbeat_recive("0")
    sTime = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    print(f"SYSTEM {sysno} Start Loading at {sTime}\n")
    
    if(first_load):
        inweight = 1.3 #Chnage for HVC to 1510
        first_load = False
    else:
        inweight = scaleWeight()
        
    print(f"Location status {location_status}------------------------------------------------")
        
    # Initial location check
    #INFO The Variable location_status is set to true when OPCUA_Location_Status is called and set to false when call_back_timer is called. 
    #INFO The reason for this to to only track the load when it has left the location  
    #INFO Logic ==>> Once the location_status variable changes to False it will leave the loop 
    while location_status:
        data_loc = mill_recive()
        data_loc = process_input_data(data_loc)
        print(f"Location status thingy  {location_status} ------------------------------------------------")
        if data_loc[0] == "Location" and data_loc[1] == "0":
            OPCUA_Location_Status("0", 2)
            # break
        elif data_loc[0] == "Heartbeat":
            heartbeat_recive(data_loc[1])
            OPCUA_Heartbeat(data_loc[1])
        elif data_loc[0] == "error":
            continue
        else:
            print(f"Wrong location: {data_loc}. Expected Loading (0).")
            if data_loc[1] in ["1", "2", "3", "4", "5", "6"]:
                OPCUA_Location_Status(data_loc[1], 2)
            return
        print("End of While Statement in loading start")
    

    weight = scaleWeight()
    print(f"Current Weight: {weight} kg")
    # variable to signifiy 10 less wieght on the bucket
    Dweight = inweight * 1.10
    #if the arrival wieght is greater than the current weight it means its getting loaded
    if(inweight < 0 ):
        print("Scale is not zeroed")    

    else:
        if (weight > Dweight):
            eTime = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            print("\n---------------------------------------------------------")
            print(f"Here is the last weight={weight}")
            print(f"Here is the inweight={inweight}")
            print(f"substraction={(weight-inweight)}")
            print(f"Loading complete at {eTime}, Weight: {weight-inweight} kg")
            print("---------------------------------------------------------\n")
            
        else:
            print("Still at loading zone")
            return
    data = {
        "arrivetime": sTime,
        "leavetime": eTime,
        "inweight": inweight,
        "outweight": weight,
        "location": "Loading",
        "systemno": sysno,
    }
    if (testmode == 0):
        cloud_upload = threading.Thread(target=AWS_upload , args=(data , eTime , weight,))
        cloud_upload.start()


    
    if testmode in ( 0 , 1, 2):
        with open("location_data.csv", mode="a", newline="") as csv_file:
            fieldnames = ["arrivetime", "leavetime", "inweight", "outweight", "location", "systemno"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if csv_file.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        #uploading net weight into leave weight at loading
        status = OPCUA_Upload("0", eTime, (weight-inweight), sTime, inweight)
        print("Data uploaded to OPCUA Server\n" if status else "OPCUA upload failed\n")
        print("Loading Complete\n")
    else:
        with open("location_data_Backup.csv", mode="a", newline="") as csv_file:
            fieldnames = ["arrivetime", "leavetime", "inweight", "outweight", "location", "systemno"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if csv_file.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        print("Connection to AWS failed. Data saved to Backup file\n")
    # Prepare and upload data
    
    # data = {
    #     "arrivetime": sTime,
    #     "leavetime": eTime,
    #     "inweight": inweight,
    #     "outweight": weight,
    #     "location": "Loading",
    #     "systemno": sysno,
    # }

    # if testmode == 0:
    #     client.publish("raspi/mobi_loc", payload=json.dumps(data), qos=0, retain=False)
    #     connectionstatus = False
    #     for i in range(3):
    #         connectionstatus = Connection_Verification("Loading", eTime, weight)
    #         if connectionstatus:
    #             break
    #         time.sleep(5)
    # else:
    #     connectionstatus = True
    cloud_upload.join()



# start unload process
def unloadStart(tag, sysno):
    heartbeat_recive(str(tag))
    global location_status
    sTime = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    print(f"SYSTEM {sysno} Start Monitoring for Mill {tag} at {sTime}\n")
    inweight = scaleWeight()

    # Buffer for stability check (5 seconds)
    weight_buffer = []
    start_time = time.time()
    leave_counter = 0

    # Initial location check
    while location_status:
        data_loc = mill_recive()
        data_loc = process_input_data(data_loc)
        print(f"THIS IS UNLOAD START ==>> {data_loc}")
        if data_loc[0] == "Location" and data_loc[1] == tag:  # Fixed condition
            OPCUA_Location_Status(tag, 2)
            # break
        elif data_loc[0] == "Heartbeat":
            heartbeat_recive(data_loc[1])
            OPCUA_Heartbeat(data_loc[1])
        elif data_loc[0] == "error":
            continue
        else:
            print(f"Wrong location: {data_loc}. Expected Mill {tag}.")
            if data_loc[1] in ["0", "1", "2", "3", "4", "5", "6"]:
                OPCUA_Location_Status(data_loc[1], 2)
            return

    # Monitor weight for unloading
    print("Ater call back timer loop=----------------------------------")
    
    weight = scaleWeight()
    
    print(f"Current Weight: {weight} kg")
            
        # variable to signifiy 10 less wieght on the bucket
    Dweight = inweight * .9
    
    #if the arrival weight is less than the leave weight then a unload is counted
    if(inweight < 0 ):
        print("Issue with scala wight is less tha 0")    
        
    else:
        if (Dweight > weight):
            eTime = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            print("\n---------------------------------------------------------")
            print(f"Here is the last weight={weight}")
            print(f"Here is the inweight={inweight}")
            print(f"substraction={(weight-inweight)}")
            print(f"unLoaded complete at {eTime}, Weight: {(inweight-weight)} kg")
            print("---------------------------------------------------------\n")
            
            
        else:
            print(f"Still unloading at mill {tag} ")
            return
    data = {
        "arrivetime": sTime,
        "leavetime": eTime,
        "inweight": inweight,
        "outweight": weight,
        "location": f"Mill {tag}",
        "systemno": sysno,
    }
    # Prepare and upload data
    cloud_upload = threading.Thread(target=AWS_upload , args=(data , eTime , weight,))
    cloud_upload.start()

    if testmode in ( 0 , 1, 2):
        with open("location_data.csv", mode="a", newline="") as csv_file:
            fieldnames = ["arrivetime", "leavetime", "inweight", "outweight", "location", "systemno"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if csv_file.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        #uploading net weight into leave weight at loading
        status = OPCUA_Upload(tag, eTime, weight, sTime, (inweight-weight))
        print("Data uploaded to OPCUA Server\n" if status else "OPCUA upload failed\n")
        print("Unloading Complete\n")
    else:
        with open("location_data_Backup.csv", mode="a", newline="") as csv_file:
            fieldnames = ["arrivetime", "leavetime", "inweight", "outweight", "location", "systemno"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if csv_file.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        print("Connection to AWS failed. Data saved to Backup file\n")
    # data = {
    #     "arrivetime": sTime,
    #     "leavetime": eTime,
    #     "inweight": inweight,
    #     "outweight": weight,
    #     "location": f"Mill {tag}",
    #     "systemno": sysno,
    # }
    # #AWS backup 
    # if testmode == 0:
    #     client.publish("raspi/mobi_loc", payload=json.dumps(data), qos=0, retain=False)
    #     connectionstatus = False
    #     for i in range(3):
    #         connectionstatus = Connection_Verification(f"Mill {tag}", eTime, weight)
    #         if connectionstatus:
    #             break
    #         time.sleep(5)
    # else:
    #     connectionstatus = True
    # #local backups of data 
    # if connectionstatus or testmode in (1, 2):
        
    #     with open("location_data.csv", mode="a", newline="") as csv_file:
    #         fieldnames = ["arrivetime", "leavetime", "inweight", "outweight", "location", "systemno"]
    #         writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    #         if csv_file.tell() == 0:
    #             writer.writeheader()
    #         writer.writerow(data)
    #     #upload net weight into arrival weight
    #     status = OPCUA_Upload(tag, eTime, weight, sTime, (inweight-weight))
    #     print("Data uploaded to OPCUA Server\n" if status else "OPCUA upload failed\n")
    #     print("Unloading Complete\n")
    # else:
    #     with open("location_data_Backup.csv", mode="a", newline="") as csv_file:
    #         fieldnames = ["arrivetime", "leavetime", "inweight", "outweight", "location", "systemno"]
    #         writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    #         if csv_file.tell() == 0:
    #             writer.writeheader()
    #         writer.writerow(data)
    #     print("Connection to AWS failed. Data saved to Backup file\n")
    

def task1():
    print("Starting Task 1")
    print("Resetting values to false")
    on_start()
    global location_status
    global stop_threads  # Access the global flag
    while not stop_threads:  # Check the flag in the loop
        response = None
        with loc_lock:
            response = mill_recive()
            print(f"RESPONSE==>>{response}")
        #Change 1 ==>> Process the data outside of the thread lock. Allow other threads to execute 
        response = process_input_data(response)

        if response == ["error"]:
            continue
        
        data_type = response[0]
        data_location = response[1]
        heart_thread = threading.Thread(target=heartbeat_recive , args=(data_location,))
        opcua_heart = threading.Thread(target=OPCUA_Heartbeat , args=(data_location,))
        

        if data_type == "Heartbeat":

            # heartbeat_recive(data_location)
            # OPCUA_Heartbeat(data_location)
            heart_thread.start()
            opcua_heart.start()
            # heart_thread.join()
            # opcua_heart.join()

            
            continue
        elif data_type == "Location":
            location_status = True
            if data_location == "0":
                opcua_location = threading.Thread(target=OPCUA_Location_Status , args=("0" , 2))
                opcua_location.start()
                # OPCUA_Location_Status("0", 2)
                weight = scaleWeight()
                
                    #OPCUA_Location_Status("0", 1)
                loadingStart("1")

                    # heartbeat_recive(data_location)
                heart_thread.start()
                    
            elif data_location in ("1", "2", "3", "4", "5", "6"):
                opcua_location = threading.Thread(target=OPCUA_Location_Status , args=(data_location , 2))
                opcua_location.start()
                # OPCUA_Location_Status(data_location, 2)
                weight = scaleWeight()
                if weight > BUCKET_WEIGHT + MIN_MATERIAL_WEIGHT:
                    #OPCUA_Location_Status(data_location, 1)
                    unloadStart(data_location, "1")
                    
                else:
                    # heartbeat_recive(data_location)
                    heart_thread.start()
                    print(f"Bucket at Mill {data_location}, Weight: {weight} kg - No unload triggered (TASK 1)")
                    continue
        else:
            print(
                "No reading or reading different system data type:"
                + data_type
                + "data location:"
                + data_location
            )

    print("ENd of task 1")
# RAW WEIGHT for OPCUA and AWS also used for checking network connection this is fine because it operates on another thread 
def task2():
    print("Starting Task 2")
    global stop_threads  # Access the global flag
    while not stop_threads:  # Check the flag in the loop
        time.sleep(2)  # Or 3, depends on how often you want to publish.
        print("OPCUA-RAW-WEIGHT")
        weight = scaleWeight()
        Time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        if testmode == 1 or testmode == 2:
            pass
        else:
            client.publish(
                "raspi/mobi",
                payload=json.dumps({"timestamp": Time, "weight": weight}),
                qos=0,
                retain=False,
            )
        # with open("scale_data.csv", mode="a", newline="") as csv_file:
        #     fieldnames = ["timestamp", "weight"]
        #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        #     if csv_file.tell() == 0:
        #         writer.writeheader()
        #     writer.writerow({"timestamp": Time, "weight": weight})
        try:
            OPCUA_Raw_weight(Time, weight)
        except Exception as e:
            print(f"Error in task2 OPCUA_Raw_weight : {e}")
        check_network_connection()

# PC OPCUA Heartbeat
def task3():
    print("Starting Task 3")
    global stop_threads  # Access the global flag
    while not stop_threads:  # Check the flag in the loop
        try:
            OPCUA_Heartbeat("7")
        except Exception as e:
            print(f"Error in task3 OPCUA_Heartbeat : {e}")
        heartbeat_recive("7")
        time.sleep(120)

def close_connections():
    
    global stop_threads
    stop_threads = True  # set flag to true to stop threads
    print("Closing all connections and exiting...")

    if serLoc:
        serLoc.close()
        print("Serial Location Closed")
    if serScale:
        serScale.close()
        print("Serial Scale Closed")

    client.disconnect()
    print("Disconnecting from AWS IoT...")
    client.loop_stop()  # Stop the MQTT client's loop
    print("AWS IoT Disconnected")

# Register the exit handler
atexit.register(close_connections)


#This method is used to switch all locations statuses to false on start. To clean up if it shutdown during use
def on_start():

    client = Client(OPCUA_Server_URL)
    client.connect()
    root = client.get_root_node()
    objects = root.get_children()[0]
    global first_load
    first_load = True

    #Switch Loading Zone to false on start
    loading_zone = objects.get_child(["2:Loading Zone"])
    loading = loading_zone.get_children()[0]  # Get the Loading folder inside Loading Zone
    loading_vars = {
        var.get_browse_name().Name: var for var in loading.get_children()
    }

    loading_vars["Loading Status"].set_value(
        False, varianttype=ua.VariantType.Boolean
    )

    #Switching Mills to false
    mills_folder = objects.get_child(["2:Mills"])
    mill_folder = None

    for i in range(1,6):
        try:
            mill_name = f"Mill {i}"
            for child in mills_folder.get_children():
                if child.get_browse_name().Name == mill_name:
                    mill_folder = child
                    break  
            mill_vars = {
            var.get_browse_name().Name: var for var in mill_folder.get_children()
        }
            mill_vars[f"{mill_name} Status"].set_value(False , varianttype=ua.VariantType.Boolean)
        except Exception as e:
            print(f"There seems to be an error with on_start ==>> {e.args[0]}")
    client.disconnect()
    print("Finished setup-----------------------------------------------")



def reset_all_historic(sysno):
    """Resets historical data for all mills in a system."""
    if sysno in ("1", "2"):
        for i in range(6):
            client.publish(
                "raspi/historic_set",
                payload=json.dumps(
                    {"location": f"Mill {i+1}", "eventCode": "4", "systemno": sysno}
                ),
                qos=0,
                retain=False,
            )
    elif sysno == "3":
        for i in range(6):
            for j in range(2):
                client.publish(
                    "raspi/historic_set",
                    payload=json.dumps(
                        {
                            "location": f"Mill {i+1}",
                            "eventCode": "4",
                            "systemno": str(j + 1),
                        }
                    ),
                    qos=0,
                    retain=False,
                )
    print("Resetting Complete\n")

print(
    "\nSystem start for individual system\n=====================================================\n"
)

while True:  # Input loop for error handling
    start_command = "0"
    # start_command = input("Enter:\n"
    #                     "  0 for normal start\n"
    #                     "  1 for reset shift\n"
    #                     "  2 for reset day\n"
    #                     "  3 for reset month\n"
    #                     "  4 for reset All\n"
    #                     "  E To close\n"
    #                     "Enter your choice: ")

    if start_command == "0":
        th1 = threading.Thread(target=task1, daemon=True)
        th2 = threading.Thread(target=task2, daemon=True)
        th3 = threading.Thread(target=task3, daemon=True)

        th1.start()
        th2.start()
        th3.start()

        break
    elif start_command in ("1", "2", "3", "4"):
        mill_no = input("Enter Mill Number: ")
        while True:  # Loop until a valid sysno is entered
            sysno = input("Enter System Number (1, 2 or 3 for both): ")

            if sysno == "3":
                systems_to_reset = ["1", "2"]  # Reset both system 1 and 2
                break  # Exit the loop
            elif sysno in ("1", "2"):
                systems_to_reset = [sysno]  # Reset only the specified system
                break  # Exit the loop
            else:
                print("Invalid input. Please try again.")

        for sys in systems_to_reset:
            client.publish(
                "raspi/historic_set",
                payload=json.dumps(
                    {
                        "location": f"Mill {mill_no}",
                        "eventCode": start_command,
                        "systemno": sys,
                    }
                ),
                qos=0,
                retain=False,
            )
        break  # Exit the loop after sending the reset command(s)

    elif start_command.upper() == "E":
        exit()
    else:
        print("Invalid input. Please try again.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Keyboard interrupt detected")
    close_connections()