import logging
import threading, datetime
from time import time, sleep
from New_FOC import DB_Function as DBtask
from dbutils.pooled_db import PooledDB
from New_FOC import DB_SQL_Function as DB
import pymysql
import os, sys, json
from multiprocessing import Process
import multiprocessing
from New_FOC import MQTT_Function as MQTTtask
import modbus_tk.modbus_tcp as modbus_tcp
import modbus_tk
from New_FOC import Modbus_Function as Modbustask
from New_FOC import FTP_Function as FTPtask
from watchdog.observers import Observer
from flask import Flask, render_template, request
from flask_sockets import Sockets
from gevent import pywsgi
from geventwebsocket.handler import WebSocketHandler
app = Flask(__name__)
sockets = Sockets(app)

# File patth
path_file = os.path.dirname(os.path.abspath(__file__))

# Read MySQL setting protocol information
processes = []

def DBConfig(input,clog):
    try:
        lock_config = threading.Lock()
        pool_mysqlt = PooledDB(pymysql, mincached=10, maxcached=20, maxconnections=1200, maxshared=10, maxusage=600,
                            blocking=True, host="localhost", port=int(3306),
                            user="root", passwd="", db="Config", charset='utf8')
        if(pool_mysqlt!= None):

            t_plist = DB.SelectMysql_Tableallparameter(pool_mysqlt, lock_config, input, ["1"], ["1"], clog)
        else:
            clog.error("DB pool failed: %s" %(pool_mysqlt))
            t_plist= []
        return t_plist, pool_mysqlt
    except Exception as e:
        clog.error("DBConfig error: %s" %(e))
        return [], None
# combine config to json
def combine_json(conLog):
    P_list = "["
    t_p, txt_pool = DBConfig("processname", conLog)
    if(txt_pool!=None):
        con_json, txt_pool = DBConfig("DB", conLog)
        re_process = ""
        for gon in range(len(con_json)):
            k = len(con_json[gon])
            re_process +=","+ '"' + (con_json[gon][1]) + '"' + ":" + "{" +\
                         '"ID"' + ":" + '"' + str(con_json[gon][0]) + '"' + "," + '"mysqlip"' + ":" + '"' + \
                         con_json[gon][2] + '"' + "," + \
                          '"mysqlport"' + ":" + '"' + con_json[gon][3] + '"' + "," + '"mysqlusername"' + ":" + '"' + \
                          con_json[gon][4] + '"' + "," + \
                          '"mysqlpassword"' + ":" + '"' + con_json[gon][5] + '"' + "," + '"mysqltablename"' + ":" + '"' + \
                          con_json[gon][6] + '"' + "," + \
                          '"mssqlip"' + ":" + '"' + con_json[gon][7] + '"' + "," + '"mssqlserver"' + ":" + '"' + \
                          con_json[gon][8] + '"' + "," + \
                          '"mssqlusername"' + ":" + '"' + con_json[gon][9] + '"' + "," + '"mssqlpassword"' + ":" + '"' + \
                          con_json[gon][10] + '"' + "," + \
                          '"mssqltablename"' + ":" + '"' + con_json[gon][11] + '"' + "}"
            # if (gon < (len(con_json) - 1)):
            #     re_process += ","
        con_json, txt_pool = DBConfig("MQTT", conLog)
        for gom in range(len(con_json)):
            re_process += "," + '"' + (con_json[gom][1]) + '"' + ":" + "{" + '"ID"' + ":" + \
                          '"' + str(con_json[gom][0]) + '"' + "," + '"clientname"' + ":" + \
                          '"' + con_json[gom][2] + '"' + "," + \
                          '"ip"' + ":" + '"' + con_json[gom][3] + '"' + "," + '"port"' + ":" + str(con_json[gom][4]) + "," + \
                          '"username"' + ":" + '"' + con_json[gom][5] + '"' + "," + '"password"' + ":" + '"' + con_json[gom][6] + '"' + "," + \
                          '"topic"' + ":" + str(con_json[gom][7]) + "}"
            # if (gom < (len(con_json) - 1)):
            #     re_process += ","
        con_json, txt_pool = DBConfig("Modbus", conLog)
        for goj in range(len(con_json)):
            re_process += "," + '"' + (con_json[goj][1]) + '"' + ":" + "{"+\
                          '"ID"' + ":" + '"' + str(con_json[goj][0]) + '"' + "," +\
                          '"ip"' + ":" + '"' + con_json[goj][2] + '"' + "," + \
                          '"svid"' + ":" + con_json[goj][3] + "," + '"sensorcnt"' + ":" + '"' + (
                          con_json[goj][4]) + '"' + "," + \
                          '"site"' + ":" + '"' + con_json[goj][5] + '"' + "," + '"location"' + ":" + '"' + con_json[goj][
                              6] + '"' + "," + \
                          '"sensor"' + ":" + '"' + (con_json[goj][7]) + '"' + "," + '"tag"' + ":" + con_json[goj][8] + "}"
            # if (goj < (len(con_json) - 1)):
            #     re_process += ","
        con_json, txt_pool = DBConfig("FTP", conLog)
        for goy in range(len(con_json)):
            re_process += "," + '"' + (con_json[goy][1]) + '"' + ":" + "{" +\
                          '"ID"' + ":" + '"' + str(con_json[goy][0]) + '"' + "," +\
                          '"ip"' + ":" + '"' + con_json[goy][2] + '"' + "," + \
                          '"port"' + ":" + str(con_json[goy][3]) + "," + '"username"' + ":" + '"' + (
                          con_json[goy][4]) + '"' + "," + \
                          '"password"' + ":" + '"' + con_json[goy][5] + '"' + "," + '"savefilename"' + ":" + '"' + \
                          con_json[goy][6] + '"' + "," + \
                          '"filenametype"' + ":" + '"' + (con_json[goy][7]) + '"' + "," + '"remotefilesite"' + ":" + '"' + \
                          con_json[goy][8] + '"' + "}"
            # if (goy < (len(con_json) - 1)):
            #     re_process += ","
        re_data = ""
        for y in range(len(t_p)):
            P_list += '"' + t_p[y][1] + '"'
            if (y < (len(t_p) - 1)):
                P_list += ","
        P_list += "]"
        re_data = "{" + '"' + "Process" + '"' + ":" + str(P_list) + str(re_process) + "}"
        path = 'C:\\Users\\User\\PycharmProjects\\DB\\FOC\\Config\\test.txt'
        f = open(path, 'w')
        f.write(str(re_data))
        f.close()

def Config():
    try:
        with open((path_file) + "\\FOC\\Config\\test.txt", 'r') as f:
            try:
                aa = f.read()
                # gh = json.dumps(aa)
                js = json.loads(aa)
                listprocess = js.get('Process')
                return listprocess, js
            except Exception as e:
                # log.error(str(e))
                print("ConfigERROR " , str(e))
                return [""]
    except Exception as e:
        print("Config_readfile " ,str(e))

# Log
def recordlog(log,path,file,log_handler):
    while True:

        try:
            sleep(1)
            # 判斷檔案路徑
            if not os.path.exists(path):
                os.makedirs(path)
                file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-log")
                # Log Handler輸出控制台
                log_handler = logging.StreamHandler()
                log_handler.setLevel(logging.DEBUG)
                # 定義Handler輸出格式
                formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
                file.setFormatter(formatter)
                log_handler.setFormatter(formatter)
                log.addHandler(file)
                log.addHandler(log_handler)
            elif os.path.exists(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-log"):
                continue
            else:
                log.removeHandler(file)
                log.removeHandler(log_handler)
                file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-log")
                # Log Handler輸出控制台
                log_handler = logging.StreamHandler()
                log_handler.setLevel(logging.DEBUG)
                # 定義Handler輸出格式
                formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
                file.setFormatter(formatter)
                log_handler.setFormatter(formatter)
                log.addHandler(file)
                log.addHandler(log_handler)
        except Exception as e:
            log.error(str(e))
def recode(filename):
    try:
        path = (os.path.dirname(os.path.abspath(__file__))) + "\\FOC\\"
        pathmain = path + filename+"\\"
        log = logging.getLogger(filename)
        log.setLevel(logging.DEBUG)
        if not os.path.exists(pathmain):
            os.makedirs(pathmain)
            # if str(datetime.datetime.today().strftime("%M")) < str(30):
            file = logging.FileHandler(pathmain + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-log")
            # else:
            #     file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-30') + "-Log紀錄")
            # file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H:%M') + "-Log紀錄")
        else:
            # if str(datetime.datetime.today().strftime("%M")) < str(30):
            file = logging.FileHandler(pathmain + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-log")
            # else:
            #     file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-30') + "-Log紀錄")
        file.setLevel(logging.DEBUG)
        # Log Handler輸出控制台
        log_handler = logging.StreamHandler()
        log_handler.setLevel(logging.DEBUG)
        # 定義Handler輸出格式
        formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
        file.setFormatter(formatter)
        log_handler.setFormatter(formatter)
        log.addHandler(file)
        log.addHandler(log_handler)
        log.info(str("SuccessLogInitial"))
        return log,pathmain,file,log_handler
    except Exception as e:
        print(e)
class MyProcess(Process):
    def __init__(self, name):
        # super().__init__()
        super(MyProcess, self).__init__()
        self.name = name
    def run(self):
            try:
                p1, p2, p3, p4 = recode(self.name)
                p1.info("success")
                RecordLog = threading.Thread(target=recordlog, args=(p1, p2, p3, p4))
                RecordLog.start()
                function = (self.name.split('_'))

                if(function[0] == "DB"):
                    while True:
                        try:
                            start = time()
                            process_thread = []
                            method, con_json = Config()
                            p1.info("Read config: " + str(con_json))
                            # print("read config", con_json)
                            p1.info("running--DB: "+ str(multiprocessing.current_process().name))
                            # print(multiprocessing.current_process().name, " running--DB")
                            DB_json = con_json.get(multiprocessing.current_process().name)
                            p1.info('[' + multiprocessing.current_process().name +']' + str(DB_json))
                            # print('[' + multiprocessing.current_process().name +']', DB_json)
                            if(DB_json == None):
                                p1.info('[' + multiprocessing.current_process().name +']' + " config 參數為空")

                            else:
                                lockmysql = threading.Lock()
                                lockmssql = threading.Lock()
                                print(DB_json.get("mysqltablename"))

                                poolmysql, poolmssql =DB.pool_config(DB_json,p1)
                                print(poolmysql)
                                print(poolmssql)
                                if(poolmysql != None and poolmssql != None):
                                # poolmssql = pool_config1(DB_json,p1)
                                # print(poolmssql)
                                # print(poolmysql,poo+lmssql)
                                    Get_all_tablename = DB.SelectMysqlDB(poolmysql, lockmysql, DB_json.get("mysqltablename"),p1)
                                    p1.info("Table: "+ str(Get_all_tablename))
                                # print("table", Get_all_tablename)
                                    for a in range(len(Get_all_tablename)):
                                        p1.info("Table list: " + str(Get_all_tablename[a][0]))
                                    # print("table list: ", Get_all_tablename[a][0])
                                    # print(DB[4])
                                        task = DBtask.DBTask(lockmysql, lockmssql, poolmysql, poolmssql, Get_all_tablename[a][0], DB_json.get("mysqltablename"), p1) #lockmysql,lockmssql, poolmysql, poolmssql,
                                        process_thread.append(task)
                                        process_thread[a].start()
                                # for wt in range(len(Get_all_tablename)):
                                        process_thread[a].join()
                                else:
                                    p1.error("DB connect fail: connect_ERROR")
                            end = time()
                            p1.info('總共耗費了 %.3f 秒.' % (end - start))
                            # print('總共耗費了 %.3f 秒.' % (end - start))
                            sleep(30)
                        except KeyboardInterrupt as e:
                            print("DB", e)
                            pid = os.getpid()
                            os.popen('taskkill.exe /f /pid:%d' % pid)
                        #     print("key",e)
                        #     sys.exit()

                        except Exception as e:
                            pid = os.getpid()
                            print(e)

                            # break
                elif(function[0] == "FTP"):
                    lockFTP = threading.Lock()
                    method, con_json = Config()
                    p1.info("Read config: " + str(con_json))
                    # print("read config", con_json)
                    local_path = con_json.get(multiprocessing.current_process().name)
                    if(local_path == None):
                        p1.info('[' + multiprocessing.current_process().name + ']' + " config 參數為空")
                        # print('[' + multiprocessing.current_process().name + ']', " config 參數為空")
                    else:
                        event_handler = FTPtask.WatchdogWithFile(lockFTP, local_path, p1)
                        p1.info("Local: "+local_path.get("remotefilesite"))
                        # print("local", local_path[6])
                        ob = Observer()
                        watch = ob.schedule(event_handler, path=local_path.get("remotefilesite"))
                        ob.start()
                        try:
                            while True:
                                p1.info('[' + multiprocessing.current_process().name + ']' +" running--FTP")
                                # print(multiprocessing.current_process().name, " running--FTP")
                                # print(len(threading.enumerate()))
                                sleep(60)

                        except KeyboardInterrupt:
                            print("FTP")
                            pid = os.getpid()
                            os.popen('taskkill.exe /f /pid:%d' % pid)
                            ob.stop()
                        ob.join()
                elif(function[0] == "MQTT"):
                    try:
                        method, con_json = Config()
                        p1.info("Read config: " + str(con_json))
                        # print("read config", con_json)
                        # p1.info('[' + multiprocessing.current_process().name + ']' + " running--MQTT")
                        # print(multiprocessing.current_process().name," running--MQTT")
                        mqtt_config = con_json.get(multiprocessing.current_process().name)
                        if (mqtt_config == None):
                            p1.info('[' + multiprocessing.current_process().name + ']' + " config 參數為空")
                        else:
                            p1.info('[' + multiprocessing.current_process().name + ']' + " running--MQTT")
                            lockmssql_mqtt = threading.Lock()
                            mqtt_pool = MQTTtask.pool_MQ_config(mqtt_config,p1)

                            MQTTtask.MQTTTask(mqtt_config.get("ID"),mqtt_config.get("clientname"), mqtt_config.get("ip"), mqtt_config.get("port")
                                     ,mqtt_config.get("username"), mqtt_config.get("password"),
                                     mqtt_config.get("topic"), multiprocessing.current_process().name, p1,
                                     mqtt_pool,lockmssql_mqtt)


                    except Exception as e:
                        if(str(e)=="timed out"):
                            p1.error("timed out")

                        p1.error("MQTT" + str(e))
                        # print("MQTT", e)
                elif(function[0] == "Modbus"):
                    method, con_json = Config()
                    # method, con_json =schedule.every(30).seconds.do(Config)
                    modbus_config = con_json.get(multiprocessing.current_process().name)
                    # modbus_thread = []
                    if (modbus_config == None):
                        p1.info('[' + multiprocessing.current_process().name + ']' + " config 參數為空")
                    else:
                        print(modbus_config.get("ip"))
                        try:
                            master = modbus_tcp.TcpMaster(host=modbus_config.get("ip"),port=502)
                            master.set_timeout(5.0)
                        # while True:
                        #     for b in range(len(modbus_config)):
                            Modbustask.ModbusTask(master,multiprocessing.current_process().name,modbus_config)
                        except modbus_tk.modbus.ModbusError as e:
                            print("0",str(e))
                        except Exception as e:
                            print("1",str(e))

                    # modbus_task = ModbusTask(modbus_config[b],master)
                    #         modbus_thread.append(modbus_task)
                    #         modbus_thread[b].start()
                            # modbus_thread[b].join()
                        # for c in range(len(modbus_config)):
                        #     modbus_thread[c].join()
                elif(function[0] == "Watchdog"):
                    while True:
                        try:
                            p1.info('[' + multiprocessing.current_process().name + ']' + " running--WatchDog")
                        # print(multiprocessing.current_process().name, " running--WatchDog")
                            sleep(60)
                        except KeyboardInterrupt as e:
                            print("Watchdog", e)
                            pid = os.getpid()
                            os.popen('taskkill.exe /f /pid:%d' % pid)
                else:
                    while True:
                        p1.info('[' + multiprocessing.current_process().name + ']' + " running--else")
                        # print(multiprocessing.current_process().name, " running--else")
                        sleep(5)
            except Exception as e:
                p1.error(str(e))
                # print("1",e)

# 接收上報狀態報文
@app.route('/MQTT', methods=['POST'])
def echo_socket_controlupdate():
    try:
        print(str("RECEIVE_REPORT"))
        if request.get_data() is not None:
            now = request.get_data()  # 接收數據
            print(str("Report input data:") + str(now))
            json_data = json.loads(now.decode())
            print(json_data)
            reply = {"Reply": "OK"}
            # print(reply)
            return json.dumps(reply, ensure_ascii=False)
    except Exception as e:
        # 有異常，回滾事務
        print(str(e))

if __name__ == '__main__':
    # try:
    #     path = (os.path.dirname(os.path.abspath(__file__))) + "\\FOC\\"
    #     print(os.path.dirname(os.path.abspath(__file__)))
    #     pathmain = path + "Main\\"
    #     log = logging.getLogger("main")
    #     log.setLevel(logging.DEBUG)
    #     if not os.path.exists(pathmain):
    #         os.makedirs(pathmain)
    #         file = logging.FileHandler(pathmain + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-log")
    #     else:
    #         file = logging.FileHandler(pathmain + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-log")
    #     file.setLevel(logging.DEBUG)
    #     # Log Handler輸出控制台
    #     log_handler = logging.StreamHandler()
    #     log_handler.setLevel(logging.DEBUG)
    #     # 定義Handler輸出格式
    #     formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
    #     file.setFormatter(formatter)
    #     log_handler.setFormatter(formatter)
    #     log.addHandler(file)
    #     log.addHandler(log_handler)
    #     log.info(str("SuccessLog"))
    # except Exception as e:
    #     print(e)
    #
    # # main log
    # RecordLogmain = threading.Thread(target=recordlog, args=(log, pathmain, file, log_handler))
    # RecordLogmain.start()

    #combine_json(log)
    #sleep(3)

    # try:
    #     listprocess, initial_config = Config()
    #     # print(initial_config)
    #     log.info(listprocess)
    #     # print(listprocess)
    #     for i in range(len(listprocess)):
    #         print(listprocess[i])
    #         t = MyProcess(name=listprocess[i])
    #         t.start()
    #         print(t)
    #         processes.append(t)
    #
    #
    # except KeyboardInterrupt as e:
    #     print("keymain", e)
    #     pid = os.getpid()
    #     os.popen('taskkill.exe /f /pid:%d' % pid)
    # except Exception as e:
    #     log.error(e)

    #IP = '127.0.0.1:8888'
    server = pywsgi.WSGIServer(('127.0.0.1', 8088), app, handler_class=WebSocketHandler)
    server.serve_forever()