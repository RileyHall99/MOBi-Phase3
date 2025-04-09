import asyncio
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from asyncua import Client
from collections import defaultdict
from matplotlib.animation import FuncAnimation
import threading

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OPCUA_Client")

# Data storage for real-time plotting
data_history = defaultdict(list)  # {node_id: [values]}
time_history = []  # Timestamps

# Subscription handler to process data changes
class SubHandler:
    def datachange_notification(self, node, val, data):
        """Called when a monitored item's value changes."""
        node_id = str(node)
        logger.info(f"Node {node_id} changed to: {val}")
        # Store the new value
        data_history[node_id].append(val)
        if len(data_history[node_id]) > 50:
            data_history[node_id].pop(0)

# Async function to run the OPC UA client
async def opcua_client_task():
    url = "opc.tcp://localhost:4840/mobi/server/"
    client = Client(url=url, timeout=30)
    
    try:
        logger.info(f"Attempting to connect to {url}")
        await client.connect()
        logger.info("Connected to OPC UA server")

        # Specify nodes to monitor
        node_ids = [
            "ns=2;i=18",  # Example node ID
            "ns=2;i=22",
            "ns=2;i=23",  # Replace with actual node IDs
        ]

        # Create subscription
        handler = SubHandler()
        subscription = await client.create_subscription(
            period=500,  # Sampling interval in milliseconds
            handler=handler
        )
        logger.info("Subscription created")

        # Subscribe to nodes
        monitored_items = []
        for node_id in node_ids:
            node = client.get_node(node_id)
            mon_item = await subscription.subscribe_data_change(node)
            monitored_items.append(mon_item)
            logger.info(f"Subscribed to {node_id}")

        # Keep the client running
        while True:
            await asyncio.sleep(1)

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        await client.disconnect()
        logger.info("Disconnected from server")

# Function to update the plot
def update_plot(frame, ax, node_ids):
    logger.debug("update_plot called")  # Debug to confirm this runs
    global time_history
    ax.clear()  # Clear the previous plot
    
    # Update time history
    if len(time_history) < len(data_history[node_ids[0]]):
        time_history.append(len(time_history) + 1)
    if len(time_history) > 50:
        time_history.pop(0)

    # Plot each node's data
    for node_id in node_ids:
        if data_history[node_id]:
            logger.info(f"Plotting {node_id} with data: {data_history[node_id][-5:]}")  # Show last 5 values
            sns.lineplot(x=time_history, y=data_history[node_id], label=node_id, ax=ax)
        else:
            logger.warning(f"No data for {node_id}")
    
    ax.set_title("Real-Time OPC UA Data")
    ax.set_xlabel("Time")
    ax.set_ylabel("Value")
    ax.legend()
    plt.tight_layout()

# Function to run the async client in a thread
def run_opcua_client_in_thread(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(opcua_client_task())

# Main function
def main():
    # Set up the asyncio loop for the OPC UA client
    loop = asyncio.new_event_loop()
    client_thread = threading.Thread(target=run_opcua_client_in_thread, args=(loop,), daemon=True)
    client_thread.start()

    # Set up the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.set(style="darkgrid")
    node_ids = [
            "ns=2;i=18",  # Example node ID
            "ns=2;i=22",
            "ns=2;i=23",  # Replace with actual node IDs
        ]  # Your node ID

    # Animate the plot
    ani = FuncAnimation(fig, update_plot, fargs=(ax, node_ids), interval=1000)  # Update every 1s

    # Show the plot (blocking)
    plt.show()  # This will block until the window is closed

    # Stop the asyncio loop when the plot is closed
    loop.call_soon_threadsafe(loop.stop)
    client_thread.join()

if __name__ == "__main__":
    main()


