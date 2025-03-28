import asyncio
import logging
from asyncua import Client , ua
import seaborn 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Client")

SERVER_URL = "opc.tcp://localhost:4840/mobi/server/"  # Local connection
NAMESPACE_URI = "http://mobi.opcua_server"

def getData():
    client = Client(SERVER_URL)
    client.connect()

    root = client.get_root_node()

    objects = root.get_children[0]

    loading_zone = objects.get_child(["2:Loading Zone"])
    loading = loading_zone.get_children()[0] 

    data = {var.get_browse_name().Name: var for var in loading.get_children}

    logger.info(f"THIS IS DATA ==>> {data}")

async def browse_and_read(node, depth=0):
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
    else:
        children = await node.get_children()
        for child in children:
            await browse_and_read(child, depth + 1)


async def main():

    # async with Client(url=SERVER_URL) as client:
    #     logger.info("Client connected to server")

    #     # Get namespace index
    #     idx = await client.get_namespace_index(NAMESPACE_URI)
    #     logger.info(f"Namespace index: {idx}")

    #     # Access 'Loading Zone -> Loading -> Status'
    #     # node_id = f"ns={idx};s=Loading Zone"
    #     # node = client.get_node(node_id)
    #     # value = await node.read_value()
    #     # logger.info(f"Current Status Value: {value}")

    #     # You can also browse the Objects folder
    #     logger.info("Browsing objects:")
    #     objects = client.nodes.objects
    #     children = await objects.get_children()
    #     # root = client.get_root_node()
    #     # obj = root.get_children()[0]
    #     for child in children:
    #         browse_name = await child.read_browse_name()
    #         logger.info(f"Found node: {browse_name}")
    #         node_class = await child.read_node_class()
    #         if(node_class == "Variable"):
    #             data = await child.read_value()
    #             logger.info(f"DATA ==>> {data}")
    #         else:
    #             print(f"CHILDREN {children}")
    #             # mills = await children.get_child(["2:Mills"])

    #             for child in children:
    #                 print(f"Child ==>> {}")
    async with Client(url=SERVER_URL) as client:
        logger.info("Connected to OPC UA Server")

        # Get namespace index
        idx = await client.get_namespace_index(NAMESPACE_URI)
        logger.info(f"Namespace Index: {idx}")

        # Start browsing from Objects node
        objects = client.nodes.objects
        await browse_and_read(objects)

        # OPTIONAL: Write example
        # await node.write_value(True)
        # logger.info("Updated Status value to True")


if __name__ == "__main__":
    asyncio.run(main())
    # getData()
