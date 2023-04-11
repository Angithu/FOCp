import threading, requests, time
import json
from flask import Flask, render_template
from flask_sockets import Sockets
from requests.adapters import HTTPAdapter
# 建構函數必須為指定一個參數
app = Flask(__name__)
sockets = Sockets(app)

def testclient1():
    global AgvId_agv
    AgvId_agv = 0
    #while True:
        # time.sleep(1)
    url1 = "http://127.0.0.1:8088/MQTT"

    re_data = {"AgvcId": "agvc002", "AgvcManufactor": "Inx-AT","Behavior": "@Go"}
    user_info1 = json.dumps(re_data)
    print((user_info1))
    s = requests.Session()
    s.mount(url1, HTTPAdapter(max_retries=3))
    try:
        r = s.post(url1, data=user_info1, timeout=(3, 6))
        c = r.json()
        # a = r.text
        # print(str(a))
        print(str(c))
        h = c["Reply"]
        # return r.text
        if h == "Ok":
            print(time.strftime('%Y-%m-%d %H:%M:%S') + "," + h)
    except requests.exceptions.RequestException as e:
        print(time.strftime('%Y-%m-%d %H:%M:%S') + " " + str(e))

        # try:
        #     rr = requests.post(url1, data=user_info1, timeout=(3.05, 27))  # timeout=(20, 20)
        #     rr.status_code
        #     print(rr.raise_for_status())
        #     cc = rr.json()
        #     print(cc)
        #     b = cc["AgvcId"]
        #     print(b)
        #     gg = rr.text
        #     print(gg)
        #     json_data = json.loads(gg)
        #     aa = json_data.get('AgvcId')
        #     gg = json_data.get('Reply')
        #     if aa == "agvc002" and gg == "Ok":
        #         print(time.strftime('%Y-%m-%d %H:%M:%S') + " " + str("Success"))
        #     # gg = json.loads(gg)
        #     print(aa)
        #     print((gg))
        #     # return rr.text
        # except requests.exceptions.RequestException as e:
        #     print(time.strftime('%Y-%m-%d %H:%M:%S') + " " + str(e))


# 主程式執行
if __name__ == "__main__":
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler

    t1 = threading.Thread(target=testclient1)
    t1.start()
    oo = threading.Timer()
    server = pywsgi.WSGIServer(('127.0.0.1', 8087), app, handler_class=WebSocketHandler)
    server.serve_forever()