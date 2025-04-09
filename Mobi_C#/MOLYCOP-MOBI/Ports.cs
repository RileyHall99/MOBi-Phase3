using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.Ports;
using System.Management; 

namespace MOLYCOP_MOBI
{
    internal class COMPORTS
    {
        String[] deviceNames;
        String[] ports;
        String scalePort = "";
        String LoRaPort = ""; 
        public COMPORTS(String [] device_names) {
            this.deviceNames = device_names;
            this.ports = new string[device_names.Length];
        }

        public COMPORTS(String s_port, String l_port)
        {
            this.scalePort = s_port;
            this.LoRaPort = l_port;
        }

        public void getPorts()
        {
            String[] results = new String[this.deviceNames.Length];

            try
            {
                Console.WriteLine("IN GET PORTS");

                String[] ports = SerialPort.GetPortNames();
                int index = 0; 

                    foreach (String port in ports)
                {
                    Console.WriteLine("This is ports ==>> " + port);
                    using (var searcher = new ManagementObjectSearcher($"SELECT * FROM Win32_PnPEntity WHERE Caption LIKE '%({port})%'")) {
                        foreach (var device in searcher.Get())
                        {
                            Console.WriteLine(device["Caption"].ToString());
                            //Add in device name for scale
                            if (device["Caption"].ToString().Contains("Silicon Labs CP210x USB to UART Bridge") || device["Caption"].ToString().Contains(""))
                            {
                                Console.WriteLine("FOUND!!!!! ==>> " + port);

                                results[index] = port;
                                index++; 
                                break;
                            }
                        }
                    }; 
                    
                }

            }
            catch (Exception ex){
                Console.WriteLine("ERROR ==>> " + ex.Message);
            }
            this.ports = results;

        }
    }
}
