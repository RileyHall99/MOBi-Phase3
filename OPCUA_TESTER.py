import time
import datetime
import random
from opcua import Client, ua

OPCUA_Server_URL = "opc.tcp://admin@localhost:4840/freeopcua/server/"
testmode = 1
is_connected_internet_AWS = False

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

        if location == "0":
            loading_zone = objects.get_child(["2:Loading Zone"])
            loading = loading_zone.get_children()[0]
            loading_vars = {var.get_browse_name().Name: var for var in loading.get_children()}

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
                        if variant_type:
                            loading_vars[var_name].set_value(value, varianttype=variant_type)
                        else:
                            loading_vars[var_name].set_value(value)
                except Exception as e:
                    print(f"Error updating {var_name} in Loading: {e}")
                    return False
            time.sleep(3)    
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
                        else:
                            loading_vars[var_name].set_value(value)
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
            time.sleep(3)
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
                if HB_value == 10000:
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
                if HB_value > 10000:
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

def OPCUA_Location_Status(location, status):
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
                    time.sleep(1)
                    loading_vars["Loading Status"].set_value(
                        False, varianttype=ua.VariantType.Boolean
                    )
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
                    time.sleep(1)
                    mill_vars[f"{mill_name} Status"].set_value(
                        False, varianttype=ua.VariantType.Boolean
                    )
            except Exception as e:
                print(f"Error updating {mill_name} Status to OPCUA: {e}")
    except Exception as e:
        print(f"OPCUA server not started\nError: {e}")
    finally:
        try:
            client.disconnect()
        except Exception as e:
            print("Failed to disconnect from OPCUA", e)


while True:
    Time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    current_time = datetime.datetime.now()
    new_time = current_time - datetime.timedelta(minutes=10)
    formatted_new_time = new_time.strftime("%m/%d/%Y, %H:%M:%S")

    print("Upload Location 0, 1, 2,")
    OPCUA_Upload("0", formatted_new_time, random.uniform(0, 2), Time, random.uniform(0, 100)) 
    OPCUA_Upload("1", formatted_new_time, random.uniform(0, 2), Time, random.uniform(0, 100)) 
    OPCUA_Upload("2", formatted_new_time, random.uniform(0, 2), Time, random.uniform(0, 100)) 
    # OPCUA_Upload("3", formatted_new_time, random.uniform(0, 2), Time, random.uniform(0, 100)) 
    # OPCUA_Upload("4", formatted_new_time, random.uniform(0, 2), Time, random.uniform(0, 100)) 
    # OPCUA_Upload("5", formatted_new_time, random.uniform(0, 2), Time, random.uniform(0, 100))
    # OPCUA_Upload("6", formatted_new_time, random.uniform(0, 2), Time, random.uniform(0, 100))

    print("Heartbeat Location 0, 1, 2, ")
    OPCUA_Heartbeat("0")
    OPCUA_Heartbeat("1")   
    OPCUA_Heartbeat("2")
    # OPCUA_Heartbeat("3")
    # OPCUA_Heartbeat("4")
    # OPCUA_Heartbeat("5")
    # OPCUA_Heartbeat("6")
    OPCUA_Heartbeat("7")
    time.sleep(2)

    print("Raw Weight")
    OPCUA_Raw_weight(Time, random.uniform(0, 2))

    print("Location Status")
    OPCUA_Location_Status("0", 1)
    OPCUA_Location_Status("1", 1)
    OPCUA_Location_Status("2", 1)
    # OPCUA_Location_Status("3")
    # OPCUA_Location_Status("4")
    # OPCUA_Location_Status("5")
    # OPCUA_Location_Status("6")

