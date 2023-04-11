import pymssql
from dbutils.pooled_db import PooledDB
import json, os
from time import sleep
import paho.mqtt.client as mqtt
from threading import Thread
# DB Pool 連線數 for MQTT
def pool_MQ_config(configpoolMQ, Log):
    try:
        if(configpoolMQ.get("mssqlip")!=None  and configpoolMQ.get("mssqlserver")!=None and configpoolMQ.get("mssqltablename")!=None):
            pool_mssql_MQ = PooledDB(creator=pymssql, mincached=2, maxcached=20, maxconnections=1200, maxshared=100,
                                  maxusage=600,
                                  blocking=True, host=configpoolMQ.get("mssqlip"), server=configpoolMQ.get("mssqlserver"),
                                  user=configpoolMQ.get("mssqlusername"), password=configpoolMQ.get("mssqlpassword"),
                                  database=configpoolMQ.get("mssqltablename"))
            return pool_mssql_MQ
        else:
            return None
    except Exception as e:
        Log.error("pool_MQ_config: " + str(e))

# MQTT function
class MQTTTask(Thread):
    def __init__(self,mqtt_ID, mqtt_object, mqtt_host, mqtt_port, mqtt_user, mqtt_pass, topic_name, process_name ,LOG, mqttpool, mqttlock):
        # global test_name

        # test_name = topic_name
        super(MQTTTask, self).__init__()
        self.mqtt_ID = mqtt_ID
        self.topic_name = topic_name
        self.mqtt_object = mqtt_object
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_pass = mqtt_pass
        self.LOG = LOG
        self.mqttpool = mqttpool
        self.mqttlock = mqttlock
        self.client = None
        # self.client = self.start(self.mqtt_object,self.mqtt_user, self.mqtt_pass,self.mqtt_host, self.mqtt_port)
    def run(self):
        self.client = mqtt.Client(self.mqtt_object)
        # 設定建立連線回呼函數
        self.client.on_connect = self.on_connect
        # 設定接收訊息回呼函數
        self.client.on_message = self.on_message

        # 設定登入帳號密碼
        self.client.username_pw_set(self.mqtt_user, self.mqtt_pass)
        self.client.connect(self.mqtt_host, int(self.mqtt_port), 60)

        # print("client",client)
        # 保持連接
        self.client.loop_start()
        while self.is_alive():
            cnt=0
            try:
                sleep(1)
                # # sleep(20)
                # # 設定建立連線回呼函數
                # # client.on_connect = self.on_connect
                # # 設定接收訊息回呼函數
                # # client.on_message = self.on_message
                # Topic_name_update =[]
                # # method, con_json = Config()
                # # Topic_cnt = con_json.get(self.process_name)
                # # Topic_name_update = Topic_cnt[0].get("topic")
                # if(len(self.topic_name) >= len(Topic_name_update)):
                #     topic_list = []
                #     for j in range(len(self.topic_name)):
                #         check_topic = False
                #         for k in range(len(Topic_name_update)):
                #             if(self.topic_name[j] == Topic_name_update[k]):
                #                 check_topic = True
                #                 break
                #         if(check_topic != True):
                #             self.LOG.info("減少topic")
                #             # print("減少topic")
                #             client.unsubscribe(self.topic_name[j])
                #             topic_list.append(self.topic_name[j])
                #     if(len(topic_list)>0):
                #         for b in range(len(topic_list)):
                #             self.topic_name.remove(topic_list[b])
                # elif (len(self.topic_name) < len(Topic_name_update)):
                #     topic_list = []
                #     for j in range(len(Topic_name_update)):
                #         check_topic = False
                #         for k in range(len(self.topic_name)):
                #             if (Topic_name_update[j] == self.topic_name[k]):
                #                 check_topic = True
                #                 break
                #         if (check_topic != True):
                #             self.LOG.info("增加topic")
                #             # print("增加topic")
                #             client.subscribe(Topic_name_update[j])
                #             topic_list.append(Topic_name_update[j])
                #     if (len(topic_list) > 0):
                #         for b in range(len(topic_list)):
                #             self.topic_name.append(topic_list[b])
                # self.topic_name = Topic_name_update
            except KeyboardInterrupt as e:
                print("MQTT",e)
                pid = os.getpid()
                os.popen('taskkill.exe /f /pid:%d' % pid)
            except Exception as e:
                cnt+=1
                self.LOG.error("MQTT_ERROR: " + str(e))
                if(cnt==8):
                    cnt=0
                    self.client.reconnect()
                    print("reconnect")
                    # self.client.loop_stop()
                    # self.client = self.start(self.mqtt_object, self.mqtt_user, self.mqtt_pass, self.mqtt_host,self.mqtt_port)
        # Rec1 = threading.Thread(name='thread', target=search_topic(), args=(processes,))
        # Rec1.start()

    def start(self,mqtt_object1,mqtt_user1, mqtt_pass1,mqtt_host1, mqtt_port1):
        client = mqtt.Client(mqtt_object1)
        # 設定建立連線回呼函數
        client.on_connect = self.on_connect
        # 設定接收訊息回呼函數
        client.on_message = self.on_message

        # 設定登入帳號密碼
        client.username_pw_set(mqtt_user1, mqtt_pass1)
        client.connect(mqtt_host1, int(mqtt_port1), 60)

        # print("client",client)
        # 保持連接
        client.loop_start()
        return client
    def on_connect(self,client,userdata,flags,rc):
        self.LOG.info("Connected with result code : " + str(rc))
        # print("Connected with result code : " + str(rc))
        for topic in (self.topic_name):
            # topic = "test"
            client.subscribe(topic)
            self.LOG.info("Listen to %s"%topic)
            # print("Listen to %s"%topic)
    def on_message(self,client,userdata,msg):
        try:
            self.LOG.info(str(msg.topic))
            data = msg.payload.decode()
            data = json.loads(data)
            # print(data)
            if(data['code'] == "EOK"):

                print(self.mqtt_ID,"123456")
            # data = json.load(data)
            # for i in range(1, 9):
                table = "%s_%s_%s_%s"%(data['site'], data['sensor'], data['location'], data['tag'])
                print("tag",data['tag'])
                print("MQ",data['time'])
                bnb = data['time']
                #table_mess = threading.Thread(target=send_mess, name=data['time'], args=(bnb))
                #table_mess.start()
                # fixtime = (str((datetime.datetime.today()-datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')))
            #     if(self.mqttpool!=None):
            # #     self.LOG.info(str(table))
            #     # DB_Function.InsertSQL(DB_Function.pool_FBDC, table, ["DataValue", "DateTime"],[str(data["%02d"%i]), data['Time']])
            #         InsertSQL(self.mqttpool, self.mqttlock, table, ["DataValue", "DateTime"], [(data['value']), fixtime,], self.LOG)
            #     else:
            #         table = "%s_%s_%s_%s"%(data['site'], data['sensor'], data['location'], data['tag'])
            #         self.LOG.info("tablename: %s" %str(table))
            else:
                self.LOG.error("codevalue: %s is other: %s" %(data['code'],str(data)))

        except Exception as e:
            # Log_Function.log.error("data format error : " + str(e))
            self.LOG.error("data format error : " + str(e))
            # print("data format error : " + str(e))

