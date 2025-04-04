
"""
File: Mobi.py
Description: This code is the OPCUA server
Author: Zion Chong
Created: 2024-11-13
"""

import asyncio
import logging
from asyncua import Server, ua
from asyncua.common.methods import uamethod
from typing import Dict, List, Optional
from asyncua.server.user_managers import CertificateUserManager
from asyncua.crypto.permission_rules import SimpleRoleRuleset
from asyncua.server.users import UserRole


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RawWeightObject:
    def __init__(self, server: Server, idx: int):
        self.server = server
        self.idx = idx
        self.node = None
        self.variables: Dict[str, Optional[ua.NodeId]] = {}

    async def setup(self, parent_node):
        """Asynchronously set up the Raw Weight Object"""
        try:
            # Create the Raw Weight object
            self.node = await parent_node.add_object(self.idx, "RW")
            
            # Add Raw_Weight_time variable (string)
            raw_weight_time = await self.node.add_variable(
                self.idx, 
                "RW_Time", 
                ""  # Empty string as initial value
            )
            await raw_weight_time.set_writable()
            self.variables["time"] = raw_weight_time

            # Add Raw_Weight_weight variable (double)
            raw_weight_value = await self.node.add_variable(
                self.idx, 
                "RW_Weight", 
                0.0  # 0.0 as initial value
            )
            await raw_weight_value.set_writable()
            self.variables["weight"] = raw_weight_value

            #logger.info("Raw Weight object setup complete")
        except Exception as e:
            logger.error(f"Error setting up Raw Weight object: {e}")
            raise

class PCObject:
    def __init__(self, server: Server, idx: int):
        self.server = server
        self.idx = idx
        self.node = None
        self.variables: Dict[str, Optional[ua.NodeId]] = {}

    async def setup(self, parent_node):
        """Asynchronously set up the PC Object"""
        try:
            # Create the PC object
            self.node = await parent_node.add_object(self.idx, "PC")
            
            # Add PC_time variable (string)
            pc_heartbeat = await self.node.add_variable(
                self.idx, 
                "PC_Heartbeat", 
                0  
            )
            await pc_heartbeat.set_writable()    
            self.variables["time"] = pc_heartbeat


            #logger.info("PC object setup complete")
        except Exception as e:    
            logger.error(f"Error setting up PC object: {e}")
            raise   

class MillObject:
    def __init__(self, server: Server, idx: int, name: str):
        self.server = server
        self.idx = idx
        self.name = name
        self.node = None
        self.variables: Dict[str, Optional[ua.NodeId]] = {}

    async def setup(self, parent_node):
        """Asynchronously set up the Mill Object"""
        try:
            self.node = await parent_node.add_object(self.idx, self.name)
            
            # Define variables
            variable_specs = [
                ("Arrive Time", ""),
                ("Arrive Weight", 0.0),
                ("Leave Time", ""),
                ("Leave Weight", 0.0),
                ("Status", False),
                ("Heartbeat", 0)
            ]

            # Add variables to the mill object
            for var_name, initial_value in variable_specs:
                prefixed_name = f"{self.name} {var_name}"
                var_node = await self.node.add_variable(self.idx, prefixed_name, initial_value)
                await var_node.set_writable()
                self.variables[var_name] = var_node

            #logger.info(f"Mill object {self.name} setup complete")
        except Exception as e:
            logger.error(f"Error setting up mill object {self.name}: {e}")
            raise

class AsyncOPCUAServer:
    def __init__(self, endpoint: str = "opc.tcp://0.0.0.0:4840/mobi/server/"):
        # Server setup
        self.server: Optional[Server] = None
        self.endpoint = endpoint
        
        # Namespace setup
        self.uri = "http://mobi.opcua_server"
        self.idx = None
        
        # Nodes and objects
        self.objects_node = None
        self.mills: List[MillObject] = []
        self.raw_weight = None

    async def init_server(self):
        print("HERE IN INIT SERVER")
        """Initialize the OPC UA server"""
        try:

            self.server = Server()
            self.server.set_server_name("MOBI_OPCUA_Server")
            print("SET NAMESPACE")
            # Initialize server
            await self.server.init()
            print("END OF SERVER SETUP")
            self.server.set_endpoint(self.endpoint)
            self.idx = await self.server.register_namespace(self.uri)

            # Get objects node
            self.objects_node = self.server.nodes.objects

            #logger.info("Server initialized successfully")
        except Exception as e:
            logger.error(f"Server initialization error: {e}")
            raise
        print("END OF SERVER SETUP")
    async def setup_nodes(self):
        print("HELP")
        """Set up server nodes"""
        if not self.server or not self.objects_node:
            print("HERE IN CATCH RAISE")
            raise RuntimeError("Server not initialized")

        try:
            print("FIRST TRY")
            # Create main objects
            loading_zone = await self.objects_node.add_object(self.idx, "Loading Zone")
            mills_folder = await self.objects_node.add_object(self.idx, "Mills")
            
            # Create Raw_Weight object
            raw_weight_folder = await self.objects_node.add_object(self.idx, "Raw_Weight")

            # Create PC object
            PC_folder = await self.objects_node.add_object(self.idx, "PC")
            print("AFTER A FEW")
            # Create loading zone mill object
            loading_mill = MillObject(self.server, self.idx, "Loading")
            await loading_mill.setup(loading_zone)

            # Create Raw Weight object
            self.raw_weight = RawWeightObject(self.server, self.idx)
            await self.raw_weight.setup(raw_weight_folder)

            # Create PC object
            self.PC = PCObject(self.server, self.idx)
            await self.PC.setup(PC_folder)

            # Create multiple mill objects
            for i in range(1, 7):
                mill = MillObject(self.server, self.idx, f"Mill {i}")
                await mill.setup(mills_folder)
                self.mills.append(mill)

            logger.info("Nodes setup complete")
        except Exception as e:
            logger.error(f"Node setup error: {e}")
            raise

    async def run(self):
        """Run the OPC UA server"""
        try:
            # Initialize server
            await self.init_server()

            # Setup nodes
            await self.setup_nodes()

            logger.info(f"Server started on {self.endpoint}")

            # Keep server running
            async with self.server:
                while True:
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Server run error: {e}")
            raise
        finally:
            if self.server:
                await self.server.stop()

async def main():
    """Main async entry point"""
    opcua_server = AsyncOPCUAServer()
    try:
        await opcua_server.run()
    except KeyboardInterrupt:
        logger.info("Server shutdown initiated by user")
    except Exception as e:
        logger.error(f"Unhandled server error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main(), debug=True)
    except Exception as e:
        logger.error(f"Asyncio run error: {e}")