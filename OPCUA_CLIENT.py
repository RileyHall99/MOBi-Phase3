import asyncio
import logging
from asyncua import Client , ua
import seaborn as sns
import matplotlib.pyplot as plt
import time
import threading 
import pandas as pd
from queue import Queue
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Client")

# SERVER_URL = "opc.tcp://localhost:4840/mobi/server/"  # Local connection
SERVER_URL = "opc.tcp://10.0.0.221:4840/mobi/server/"  # Local connection
NAMESPACE_URI = "http://mobi.opcua_server"
data = {'Timestamp' : [] , 'Raw_Weight' : [] , 'Mill1' : [] , 'Mill2' : [] , 'Mill3' : [] , 'Mill4' : [], 'Mill5' : [], 'Mill6' : [], 'Loading' : [] , 'Mill1-Heartbeat' : [] , 'Mill2-Heartbeat' : []}
weight = 0
mill1_location = False
mill2_location = False
mill3_location = False
mill4_location = False
mill5_location = False
mill6_location = False
loading_location = False
mill1_heartbeat = 0
mill2_heartbeat = 0
data_queue = Queue()

async def browse_and_read(node, depth=0):
    global weight
    global mill1_location
    global mill2_location
    global mill3_location 
    global mill4_location
    global mill5_location
    global mill6_location
    global loading_location
    global mill1_heartbeat
    global mill2_heartbeat

    value_changed = False
    """
    Recursively browses OPC UA nodes and reads values if they are Variables.
    """
    indent = "  " * depth  # Just for nice indentation in logs
    browse_name = await node.read_browse_name()
    node_class = await node.read_node_class()
    # logger.info(f"{indent}Node: {browse_name} ({node_class})")

    if node_class == ua.NodeClass.Variable:
        value = await node.read_value()
        logger.info(f"{indent}  Node Name ==>> {browse_name}\n_________________\nDATA Value ==>>  {value} \n _______________")
        if('Mill 1 Status' in browse_name.Name ):
            mill1_location = value
            value_changed = True
        elif('Mill 2 Status' in browse_name.Name):
            mill2_location = value
            value_changed = True

        elif('Mill 3 Status' in browse_name.Name ):
            mill3_location = value
            value_changed = True
        
        elif('Mill 4 Status' in browse_name.Name ):
            mill4_location = value
            value_changed = True
        
        elif('Mill 5 Status' in browse_name.Name ):
            mill5_location = value
            value_changed = True
        
        elif('Mill 6 Status' in browse_name.Name ):
            mill6_location = value
            value_changed = True
    
        elif("Loading Status" in browse_name.Name):
            loading_location = value
            value_changed = True
        elif('RW_Weight' in browse_name.Name):
            weight = value
            value_changed = True
        elif('Mill 1 Heartbeat' in browse_name.Name):
            mill1_heartbeat = value
            value_changed = True
        elif('Mill 2 Heartbeat' in browse_name.Name):
            mill2_heartbeat = value
            value_changed = True
            # data['Raw_Weight'].append(weight)
            # data['Timestamp'].append(time.time())
        if(value_changed):
            data["Mill1"].append(mill1_location)
            data["Mill2"].append(mill2_location)
            data["Mill3"].append(mill3_location)
            data["Mill4"].append(mill4_location)
            data["Mill5"].append(mill5_location)
            data["Mill6"].append(mill6_location)
            data["Loading"].append(loading_location)
            data['Raw_Weight'].append(weight)
            data['Timestamp'].append(time.time())
            data['Mill1-Heartbeat'].append(mill1_heartbeat)
            data['Mill2-Heartbeat'].append(mill2_heartbeat)
    else:
        children = await node.get_children()
        for child in children:
            await browse_and_read(child, depth + 1)
    return data


def update_graphs(data_queue):
    """Live-updating Seaborn line plot"""
    plt.ion()
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.set_style("darkgrid")
    
    while True:
        data_dict = data_queue.get()
        df = pd.DataFrame(data_dict)
        
        if not df.empty:
            df_long = df.melt(
                id_vars=['Timestamp'],
                value_vars=['Raw_Weight', 'Mill1', 'Mill2', 'Mill3','Mill4','Mill5','Mill6' ,'Loading' , 'Mill1-Heartbeat' ],
                var_name='Variable',
                value_name='Value'
            )
            #, 'Mill2-Heartbeat'
            df_long = df_long.dropna()
            
            if not df_long.empty:
                ax.clear()
                sns.lineplot(
                    data=df_long,
                    x='Timestamp',
                    y='Value',
                    hue='Variable',
                    marker='o',
                    ax=ax
                )
                ax.set_title('OPC UA Server Data')
                ax.set_xlabel('Time')
                ax.set_ylabel('Values')
                ax.tick_params(axis='x', rotation=45)
                ax.legend(title='Variables')
                fig.tight_layout()
                plt.pause(0.1)

async def main():
    url = "opc.tcp://localhost:4840/mobi/server/" # Replace with your actual server URL
    logger.info(f"Attempting to connect to: {url}")
    async with Client(url=url) as client:
        try:
            if client is None:
                raise ValueError("Failed to initialize OPC UA Client")
            
            # Set a timeout (optional, adjust as needed)
            # 10 seconds timeout
            
            # Connect to the server
            await client.connect()
            logger.info("Successfully connected to OPC UA server")
            
            objects = client.get_objects_node()
            
            # Start graph update thread
            t1 = threading.Thread(target=update_graphs, args=(data_queue,))
            t1.start()
            
            # Collect data continuously
            while True:
                await browse_and_read(objects)
                data_queue.put(data.copy())
                print(data)
                # await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in main: {str(e)}")
            raise
        finally:
            try:
                await client.disconnect()
                logger.info("Disconnected from OPC UA server")
            except:
                pass  # Ignore disconnect errors if already disconnected

if __name__ == "__main__":
    asyncio.run(main())