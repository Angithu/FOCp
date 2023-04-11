import os
from time import sleep
import modbus_tk.modbus_tcp as modbus_tcp
import modbus_tk
import modbus_tk.defines as cst
from New_FOC import FOC_magr
from threading import Thread
class ModbusTask(Thread):
    def __init__(self, modbus_master, modbus_process_name, modbus_config_1):
        super().__init__()
        # self.ip = config_modbus.get("ip")
        # self.svid = config_modbus.get("svid")
        # self.SensorCnt = config_modbus.get("sensorCnt")
        # self.site = config_modbus.get("site")
        # self.location = config_modbus.get("location")
        # self.sensor = config_modbus.get("sensor")
        # self.tag = config_modbus.get("tag")
        self.master = modbus_master
        self.modbus_process_name = modbus_process_name
        self.modbus_config_1 = modbus_config_1
        ip_exchange = False
    def run(self):
        while True:
            try:
                if(ip_exchange==True):
                    self.master.close()
                    sleep(3)
                    method, con_json = FOC_magr.Config()
                    self.modbus_config_1 = con_json.get(self.modbus_process_name)
                    self.master = modbus_tcp.TcpMaster(host=self.modbus_config_1.get("ip"), port=502)
                    self.master.set_timeout(5.0)
                    print("reconnect")
                    ip_exchange = False
                # ee= len(self.modbus_config_1)
                for b in range(len(self.modbus_config_1.get("svid"))):
                    print(self.modbus_config_1.get("ID"))
                    # print(self.modbus_config_1.get("sensorcnt"), self.modbus_config_1.get("site"), self.modbus_config_1.get("location"))
                    # kk=self.modbus_config_1.get("svid")[b]
                    data = self.master.execute(self.modbus_config_1.get("svid")[b],cst.READ_HOLDING_REGISTERS,0,self.modbus_config_1.get("sensorcnt"))
                    sleep(0.5)
                    # print(data)
            except KeyboardInterrupt as e:
                print("Modbus", e)
                pid = os.getpid()
                os.popen('taskkill.exe /f /pid:%d' % pid)
            # except modbus_tk.modbus.ModbusError as e:
            #     print(str(e),"123")
            except Exception as e:
                print(str(e))
                if e.args[0] == int(10061):
                    ip_exchange = True