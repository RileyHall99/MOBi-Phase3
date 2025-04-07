import asyncio
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from asyncua import Client
from collections import defaultdict
import numpy as np
from matplotlib.animation import FuncAnimation

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
        # Keep only the last 50 data points (adjust as needed)
        if len(data_history[node_id]) > 50:
            data_history[node_id].pop(0)

# Async function to run the OPC UA client
async def opcua_client_task():
    url = "opc.tcp://10.0.0.221:4840/mobi/server/"
    client = Client(url=url, timeout=30)
    
    try:
        # Connect to the server
        logger.info(f"Attempting to connect to {url}")
        await client.connect()
        logger.info("Connected to OPC UA server")

        # Specify nodes to monitor (replace with your actual node IDs)
        node_ids = [
            "ns=2;s=Channel1.Device1.Tag1",  # Example node ID
            "ns=2;s=Channel1.Device1.Tag2",  # Replace with actual node IDs
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
        try:
            while True:
                await asyncio.sleep(1)  # Keep the event loop alive
        except KeyboardInterrupt:
            logger.info("Shutting down subscription...")
            await subscription.unsubscribe(monitored_items)
            await subscription.delete()

    except Exception as e:
        logger.error(f"Error: {e}")
    
    finally:
        await client.disconnect()
        logger.info("Disconnected from server")

# Function to update the plot
def update_plot(frame, ax, node_ids):
    global time_history
    ax.clear()  # Clear the previous plot
    
    # Update time history
    if len(time_history) < len(data_history[node_ids[0]]):
        time_history.append(len(time_history) + 1)
    if len(time_history) > 50:  # Match data_history limit
        time_history.pop(0)

    # Plot each node's data
    for node_id in node_ids:
        if data_history[node_id]:  # Only plot if there's data
            sns.lineplot(x=time_history, y=data_history[node_id], label=node_id, ax=ax)
    
    # Customize the plot
    ax.set_title("Real-Time OPC UA Data")
    ax.set_xlabel("Time (samples)")
    ax.set_ylabel("Value")
    ax.legend()
    plt.tight_layout()

# Main function to run both OPC UA client and plot
async def main():
    # Start the OPC UA client in the background
    client_task = asyncio.create_task(opcua_client_task())

    # Set up the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.set(style="darkgrid")  # Seaborn style

    # Node IDs for plotting (same as subscribed nodes)
    node_ids = [
        "ns=2;s=Channel1.Device1.Tag1",
        "ns=2;s=Channel1.Device1.Tag2",
    ]

    # Animate the plot
    ani = FuncAnimation(fig, update_plot, fargs=(ax, node_ids), interval=1000)  # Update every 1 second

    # Show the plot (non-blocking)
    plt.show(block=False)

    # Keep the event loop running
    try:
        await client_task
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    finally:
        plt.close(fig)  # Close the plot when done

if __name__ == "__main__":
    asyncio.run(main())