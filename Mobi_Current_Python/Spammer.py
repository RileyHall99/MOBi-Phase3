import serial.tools
import serial.tools.list_ports
import random
import time
import threading

def to_txt(data : str = "")-> None:
    with open("./pc_monitoring.txt" , "a")as file:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # Format timestamp
        print(f"here in save {data}")
        output = f"TIME ==>> {timestamp} DATA ==>> {data}\n"
        file.write(output)
        # file.flush()
        # file.close()



def connect_port():
        port_index = 0
        print("Searching for available COM ports...")
        ports = list(serial.tools.list_ports.comports())
        port_found = False
        while(not port_found):
            try:
                if(len(ports) > port_index):
                    print(f"Trying this port {ports[port_index]}")
                    if("Silicon Labs CP210x USB to UART Bridge" in ports[port_index].description):
                        ser = serial.Serial(ports[port_index].device , 115200 , timeout=1)
                        print(f"Port Info ==>> {ports[port_index]} port device {ports[port_index].device}")

                        port_found = True
                        return ser
            except Exception as e:
                print(e)
                port_index+=1
                port_found = False

        print("Cant find port")
        exit()

#Use a thread call back timer to call this function every two minutes 
def heartbeats(ser : serial.Serial) -> None:
    hearbeat_0 = f"{{'Type' : 'Heartbeat' # 'Value' : 0}}"
    hearbeat_2 = f"{{'Type' : 'Heartbeat' # 'Value' : 2}}"
    print("HEARTBEAT!!!")
    while(True):
        time.sleep(120)
        ser.write(f"AT+SEND=100,{len(hearbeat_0)},{hearbeat_0}\r\n".encode())
        time.sleep(1)
        ser.write(f"AT+SEND=100,{len(hearbeat_2)},{hearbeat_2}\r\n".encode())
        

def site_simulation_Mill0(ser : serial.Serial) -> None: 
    data = f"{{'Type' : 'Location' # 'Value' : 0}}"
    count = 0
    while(True):
        print(data )
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(1)
        
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(1)

        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(5)
        count+=3    
        print(f"Times sent ==>> {count}")


def site_simulation_Mill1(ser : serial.Serial) -> None: 
    ser.write(b"AT+PARAMETER=10,7,2,7\r\n")
    data = f"{{'Type' : 'Location' # 'Value' : 1}}"

    while(True):
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(1)
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(1)
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(5)

def site_simulation_Mill2(ser : serial.Serial) -> None: 
    ser.write(b"AT+PARAMETER=10,7,2,7\r\n")
    data = f"{{'Type' : 'Location' # 'Value' : 2}}"

    while(True):
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(1)
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(1)
        ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
        time.sleep(5)

def test_loop(ser : serial.Serial)->None:
    locations = [6,0,2]
    ser.write(b"AT+PARAMETER=10,7,2,7\r\n")
    

    while True:
        choice = random.choice(locations)

        data = f"{{'Type' : 'Location' # 'Value' : {choice}}}"
        hearbeat = f"{{'Type' : 'Heartbeat' # 'Value' : {choice}}}"
        
        percent_chance = random.randint(0,11)
        if(percent_chance > 5):
            print("HEARTBEAT!!!!")
            ser.write(f"AT+SEND=100,{len(hearbeat)},{hearbeat}\r\n".encode())
            print(hearbeat)
            message = ser.readline().decode()
            print(message)
            time.sleep(1)
            to_txt(f"HEARTBEAT ==>> {choice}")
            
        for i in range(5):
            ser.write(f"AT+SEND=100,{len(data)},{data}\r\n".encode())
            print(choice)
            time.sleep(2)
            to_txt(f"LOCATION ==>> {choice}")
        
        
    



if __name__ == "__main__":

    ser = connect_port()
    # ser = serial.Serial("COM10" , 115200, timeout=1)
    ser.write(b"AT+PARAMETER=10,7,2,7\r\n")

    # c_t1 = Timer(120.0, heartbeats , args=(ser,))
    c_t1 = threading.Thread(target=heartbeats , args=(ser,))
    c_t1.start()

    #INFO How to run ==>> Comment one out and let it run.
    #MILL1 TEST LOOP
    site_simulation_Mill0(ser)
    # site_simulation_Mill1(ser)
    #MILL2 TEST LOOP
    # site_simulation_Mill2(ser)

    # test_loop(ser)c