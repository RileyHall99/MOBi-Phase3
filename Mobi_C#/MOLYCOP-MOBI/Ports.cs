/*
Scale Read 
LoRa Read
 */



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
        SerialPort scalePort = null;
        SerialPort LoRaPort = null; 
        public COMPORTS(String [] device_names) {
            this.deviceNames = device_names;
            this.ports = new string[device_names.Length];
        }

        public COMPORTS(String s_port, String l_port)
        {
            this.scalePort = new SerialPort(s_port , 9600 , Parity.None , 8 , StopBits.One);
            this.LoRaPort = new SerialPort(l_port , 112500 , Parity.None, 8 , StopBits.One);
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
                            if (device["Caption"].ToString().Contains("Silicon Labs CP210x USB to UART Bridge") )
                            {
                                Console.WriteLine("FOUND!!!!! ==>> " + port);

                                results[index] = port;
                                index++;
                                this.LoRaPort = new SerialPort(port , 112500 , Parity.None , 8, StopBits.One);
                           
                            }
                            else if (device["Caption"].ToString().Contains(""))
                            {
                                this.scalePort = new SerialPort(port, 9600 , Parity.None , 8 , StopBits.One);
                                results[index] = port;
                                index++; 
                             
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


        private void LoRaDataReceived(object sender, SerialDataReceivedEventArgs e)
        {
            Console.WriteLine(this.LoRaPort.ReadExisting()); 
        }
        public void LoRaReceive()
        {
            this.LoRaPort.DataReceived += new SerialDataReceivedEventHandler(LoRaDataReceived); 
        }


        private void scaleDataReceived(object sender, SerialDataReceivedEventArgs e) { 
            Console.WriteLine(this.scalePort.ReadExisting());
        }
        public void scaleRecieve()
        {
            this.scalePort.DataReceived += new SerialDataReceivedEventHandler(scaleDataReceived);
        }

        public void LoRaTransmit(String msg)
        {
            this.LoRaPort.Write(msg);
        }
    }
}
