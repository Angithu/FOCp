import multiprocessing, signal
from threading import Thread
import os, sys, json
from multiprocessing import Process
import pymysql, pymssql
from dbutils.pooled_db import PooledDB
import threading, datetime
from time import time, sleep
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import glob
from ftplib import FTP
import paho.mqtt.client as mqtt
import logging
import modbus_tk
import modbus_tk.defines as cst
import modbus_tk.modbus_tcp as modbus_tcp
from deepdiff import DeepDiff
import schedule
processes = []
js = None
initial_config =[]
update_config = []
path_file = os.path.dirname(os.path.abspath(__file__))

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

def DBConfig(input,clog):
    try:
        lock_config = threading.Lock()
        pool_mysqlt = PooledDB(pymysql, mincached=10, maxcached=20, maxconnections=1200, maxshared=10, maxusage=600,
                            blocking=True, host="localhost", port=int(3306),
                            user="root", passwd="", db="Config", charset='utf8')
        if(pool_mysqlt!= None):
            t_plist = SelectMysql_Tableallparameter(pool_mysqlt, lock_config, input, ["1"], ["1"], clog)
        else:
            clog.error("DB pool failed: %s" %(pool_mysqlt))
            t_plist= []
        return t_plist, pool_mysqlt
    except Exception as e:
        clog.error("DBConfig error: %s" %(e))
        return [], None

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
def ftpconnect(host, port, username, password):
    print(host, port, username, password)
    ftp = FTP()
    ftp.set_debuglevel(2)
    ftp.connect(host, port)
    ftp.login(username, password)
    return ftp
def search_topic(mqtt_client, type_mqtt):
    method, con_json = Config()
    Topic_cnt = con_json.get(type_mqtt)
    return Topic_cnt

# Watch dog
class Mywatchdog(FileSystemEventHandler):
    def __init__(self):
        FileSystemEventHandler.__init__(self)
        self.timer_watch = None
        self.update_config = None
        self.check_process1, self.update_config1 = Config()
    def on_modified(self, event):
        if self.timer_watch:
            self.timer_watch.cancel()
        self.timer_watch = threading.Timer(0.5, self.watch_process, args=(event.src_path, self.update_config1))
        self.timer_watch.start()
    def watch_process(self, file, config_initial):
        try:
            if(self.update_config!=None):
                config_initial = self.update_config
            combine_json(log)
            check_process, self.update_config = Config()
            log.info("con update: " + str(self.update_config))
            log.info("con initial: " + str(config_initial))
            result_diff = DeepDiff(config_initial, self.update_config)
            log.info("changed: %s" %(result_diff))
            # print("changed", result_diff)
            # print("con update: " + str(config_initial))
            if (len(check_process) >= len(processes)):
                # print("dictionary_item_added", result['dictionary_item_added'])
                name_changevalue = []
                name_additem = []
                name_addlist=[]
                name_removeitem = []
                name_removeitem1 = []
                name_removeitem2 = []
                # 'values_changed'
                try:
                    if((result_diff['values_changed'])!= None):
                        log.info("config modify value_change")
                        for u in ((result_diff['values_changed'])):
                            # print("name: ", h.split('root'))
                            new_valueitem = ((result_diff['values_changed']).get(u)).get("new_value")
                            old_valueitem = ((result_diff['values_changed']).get(u)).get("old_value")
                            # print(new_valueitem,old_valueitem)
                            change_process = u.split('root')
                            dict_item = change_process[1].find("['Process']")
                            if(dict_item == int(0)):
                                try:
                                    if ((result_diff['dictionary_item_added']) != None):
                                        log.info("Add process item and change process name ")
                                        for w in ((result_diff['dictionary_item_added'])):
                                            change_process_1 = w.split('root')
                                            dict_index = change_process_1[1].find(new_valueitem)
                                            if(dict_index == int(2)):
                                                for yu in processes:
                                                    if(old_valueitem == yu.name):
                                                        name_removeitem.append(yu)
                                                        yu.terminate()
                                                        sleep(3)
                                                        break
                                                if(len(name_removeitem)>0):
                                                    for bp in range(len(name_removeitem)):
                                                        processes.remove(name_removeitem[bp])
                                                        log.info("Subtract_Process: %s" % name_removeitem[bp])
                                                        sleep(3)
                                                        t = MyProcess(name=new_valueitem)
                                                        t.start()
                                                        processes.append(t)
                                                    log.info("Add Process")
                                except Exception as e:
                                    log.info("Not: %s" % e)
                                try:
                                    for yp in processes:
                                        if (old_valueitem == yp.name):
                                            log.info("Only change process name")
                                            name_removeitem1.append(yp)
                                            yp.terminate()
                                            sleep(3)
                                            break
                                    if (len(name_removeitem1) > 0):
                                        for bpi in range(len(name_removeitem1)):
                                            processes.remove(name_removeitem1[bpi])
                                            log.info("Subtract_Process: %s" % name_removeitem1[bpi])
                                            sleep(3)
                                            t = MyProcess(name=new_valueitem)
                                            t.start()
                                            processes.append(t)
                                        log.info("Add Process")
                                except Exception as e:
                                    log.info("Not: %s" % e)
                            # print("namesp: ", change_process)
                            # print("element: ", (result_diff['values_changed'].get(u)))
                            else:
                                log.info("only change item")
                                for q in processes:
                                    # print(change_process[1])
                                    index_change = change_process[1].find(q.name)
                                    # print("namesp1: ", (index_change),change_process[1])
                                    if (index_change == int(2)):
                                        # print(q)
                                        log.info(" %s value_change:" %q.name + str(result_diff['values_changed'].get(u)))
                                        name_changevalue.append(q)
                                        q.terminate()
                                        sleep(3)
                                        break

                        # print("name1", name_changevalue)
                        if (len(name_changevalue) > 0):
                            log.info("original: %s" %processes)
                            for h in range(len(name_changevalue)):
                                # log.info("name: "+ str(name[h]))
                                # print("name", name_changevalue[h])
                                processes.remove(name_changevalue[h])
                                log.info("Subtract_Process: %s" %name_changevalue[h])
                                sleep(3)
                                # log.info("delete_processing: %s" %processes)
                                # print(name_changevalue[h].name)
                                t = MyProcess(name=name_changevalue[h].name)
                                t.start()
                                processes.append(t)
                            log.info("Add Process")
                            log.info("complete_update: %s" %processes)
                except Exception as e:
                    log.info("Not: %s" % e)
                # 'iterable_item_added'
                try:
                    if ((result_diff['iterable_item_added']) != None):
                        log.info("config modify item_added")
                        for j in(result_diff['iterable_item_added']):
                            check_index = False
                            double_com = j.split('root')
                            double_pro = double_com[1].find("['Process']")
                            if(double_pro==int(0)):
                                log.info("Only add process name")
                            # yti = j.split("['Process']")
                            # print("namelist: ", result_diff['iterable_item_added'].get(j))
                                for q in processes:
                                    if(q.name == result_diff['iterable_item_added'].get(j)):
                                        check_index = True
                                        # name.append(q)
                                        # q.terminate()
                                        # sleep(3)
                                        break
                                if(check_index == True):
                                    # print("存在", result_diff['iterable_item_added'].get(j), str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                                    log.info("Exist: %s" %result_diff['iterable_item_added'].get(j))
                                else:
                                    # processes[i].terminate()  # process不存在
                                    log.info("No Exist and need add: %s: " %result_diff['iterable_item_added'].get(j))
                                    # print("不存在 需新增", result_diff['iterable_item_added'].get(j),
                                    #       str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                                    # print(yti.split('[]'))
                                    name_addlist.append(result_diff['iterable_item_added'].get(j))
                                # sleep(1)
                            else:
                                log.info("Only add item")
                                for r in processes:
                                    index_change_item = double_com[1].find(r.name)
                                    if (index_change_item == int(2)):
                                        # print(q)
                                        trip_index = True

                                        if(len(name_additem)>0):
                                            for cz in name_additem:
                                               if(cz.name == r.name):
                                                   trip_index = False
                                                   log.info(" %s add value_item:" % r.name + " Repeat")
                                                   break
                                        if(trip_index == True):
                                            log.info(" %s add value_item:" % r.name + str(double_com))
                                            name_additem.append(r)
                                            r.terminate()
                                            sleep(3)
                                            break
                except Exception as e:
                    log.info("Not: %s" %e)
                try:
                    if ((result_diff['iterable_item_removed']) != None):
                        log.info("config modify item_added")
                        for op in (result_diff['iterable_item_removed']):
                            log.info("Only delete item")
                            remove_com = op.split('root')
                            for mg in processes:
                                remove_che = remove_com[1].find(mg.name)
                                if (remove_che == int(2)):
                                    log.info(" %s delete value_item:" % mg.name + str(remove_com))
                                    name_removeitem2.append(mg)
                                    mg.terminate()
                                    sleep(3)
                                    break
                        if(len(name_removeitem2) > 0):
                            log.info("original: %s" % processes)
                            for hf in range(len(name_removeitem2)):
                                processes.remove(name_removeitem2[hf])
                                log.info("Subtract_Process: %s" % name_removeitem2[hf])
                                sleep(3)
                                t = MyProcess(name=name_removeitem2[hf].name)
                                t.start()
                                processes.append(t)
                            log.info("Add Process")
                except Exception as e:
                    log.info("Not: %s" % e)
                # print("name1ooooo",name_addlist)
                if (len(name_addlist) > 0):
                    for h in range(len(name_addlist)):
                        t = MyProcess(name=name_addlist[h])
                        t.start()
                        processes.append(t)
                        log.info("Add Process")
                if (len(name_additem) > 0):
                    log.info("original: %s" % processes)
                    for h in range(len(name_additem)):
                        # log.info("name: "+ str(name[h]))
                        # print("name", name_changevalue[h])
                        processes.remove(name_additem[h])
                        log.info("Subtract_Process: %s" % name_additem[h])
                        sleep(3)
                        # log.info("delete_processing: %s" %processes)
                        # print(name_changevalue[h].name)
                        t = MyProcess(name=name_additem[h].name)
                        t.start()
                        processes.append(t)
                    log.info("Add Process")
                    # log.info("complete_update: %s" % processes)
                # for i in range(len(check_process)):
                #     check_index = False
                #     for y in range(len(processes)):
                #         if (check_process[i] == processes[y].name):
                #             check_index = True
                #             break
                #     if (check_index == True):  # process存在
                #         # log.info("存在: " + check_process[i])
                #         print("存在", check_process[i], str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                #     else:
                #         # processes[i].terminate()  # process不存在
                #         # log.info("不存在 需新增: "+ check_process[i])
                #         print("不存在 需新增", check_process[i],
                #               str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                #         name.append(check_process[i])
                #         sleep(3)
                # print(DeepDiff(config_initial, self.update_config))
                # if (len(name) > 0):
                #     for h in range(len(name)):
                #         t = MyProcess(name=name[h])
                #         t.start()
                #         processes.append(t)
                #         log.info("新增Process")
                #         # print("新增Process")
                # elif (len(name)==0):
                #     print("config 修改")
                #     name1=[]
                #     result = DeepDiff(config_initial, self.update_config)
                #     print("D", result['values_changed'])
                #     print(len(result['values_changed']))
                #     for u in ((result['values_changed'])):
                #         # print("name: ", h.split('root'))
                #         change_process = u.split('root')
                #         print("namesp: ",change_process)
                #         print("element: ", (result['values_changed'].get(u)))
                #         for q in processes:
                #             print(change_process[1])
                #             index_change = change_process[1].find(q.name)
                #             print("namesp1: ", (index_change))
                #             if(index_change==int(2)):
                #                 print(q)
                #                 name1.append(q)
                #                 q.terminate()
                #                 sleep(3)
                #                 break
                #     print("name1",name1)
                #     print(type(name1))
                #     if(len(name1)>0):
                #         print(processes)
                #         for h in range(len(name1)):
                #             # log.info("name: "+ str(name[h]))
                #             print("name", name1[h])
                #             processes.remove(name1[h])
                #             print("減少Process")
                #             sleep(3)
                #             print(name1[h].name)
                #             t = MyProcess(name=name1[h].name)
                #             t.start()
                #             processes.append(t)
                #         print("新增Process")
                    # print(hh.find('Modbus_2'))
                    # dd= hh.split('Modbus_1')
                    # print(dd)
            elif (len(check_process) < len(processes)):
                name_removelist = []
                name_changevalue1 = []
                name_additem1 = []
                name_changp =[]
                #'iterable_item_removed'
                try:
                    if ((result_diff['iterable_item_removed']) != None):
                        log.info("config modify item_removed")
                        for irt in(result_diff['iterable_item_removed']):
                            check_index = False
                            double_xc = irt.split('root')
                            double_uyo = double_xc[1].find("['Process']")
                            if(double_uyo==int(0)):
                                for wp in processes:
                                    if (wp.name == result_diff['iterable_item_removed'].get(irt)):
                                        log.info(" delete process name: %s" % wp.name)
                                        name_removelist.append(wp)
                                        wp.terminate()
                                        sleep(3)
                                        break
                            else:
                                for fo in processes:
                                    # if(fo.name == result_diff['iterable_item_removed'].get(irt)):
                                    check_imn = double_xc[1].find(fo.name)
                                    if(check_imn == int(2)):
                                        trip_io = True
                                        if(len(name_changp)>0):
                                            for dp in name_changp:
                                                if(dp.name == fo.name):
                                                    trip_io = False
                                                    log.info(" %s add value_item:" % fo.name + " Repeat")
                                                    break
                                        if(trip_io == True):
                                            log.info(" %s delete value_item:" % fo.name + str(double_xc))
                                            name_changp.append(fo)
                                            fo.terminate()
                                            sleep(3)
                                            break


                        # for q in processes:
                        #     check_index = False
                        #     for j in (result_diff['iterable_item_removed']):
                        #         double_xc = j.split('root')
                        #         # double_uyo = double_xc[1].find("['Process']")
                        #         if (q.name == result_diff['iterable_item_removed'].get(j)):
                        #             # print("QQ", q.name, result_diff['iterable_item_removed'].get(j))
                        #             check_index = True
                        #             break
                        #     if (check_index == False):  # process存在
                        #         log.info("Exist: %s" % q.name)
                        #         print(double_xc[1])
                        #         check_oh = double_xc[1].find(q.name)
                        #         if(check_oh == int(0)):
                        #             log.info(" %s iterable_item_removed:" % q.name + str(double_xc))
                        #             name_changp.append(q)
                        #             q.terminate()
                        #             sleep(3)
                        #
                        #         # print("存在", q.name, str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                        #     else:
                        #         q.terminate()
                        #         log.info("No Exist and need delete: %s: " % q.name)
                        #         # print("不存在 需刪除", q.name,
                        #         #       str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                        #         name_removelist.append(q)
                        #         log.info("Subtract_Process: ")
                        #         sleep(3)
                    # for i in range(len(processes)):
                    #     check_index = False
                    #     for y in range(len(check_process)):
                    #         if (processes[i].name == check_process[y]):
                    #             check_index = True
                    #             break
                    #     if (check_index == True):  # process存在
                    #         # log.info("存在: " + processes[i].name)
                    #         print("存在", processes[i].name, str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                    #     else:
                    #         processes[i].terminate()  # process不存在
                    #         # log.info("不存在 需刪除: " + processes[i].name)
                    #         print("不存在 需刪除", processes[i].name,
                    #               str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                    #         name.append(processes[i])
                    #         # log.info("減少Process")
                    #         print("減少Process")
                    #         sleep(3)
                    # print((processes))
                        if (len(name_removelist) > 0):  # 刪除要關閉之process
                            for h in range(len(name_removelist)):
                                # log.info("name: "+ str(name[h]))
                                # print("name", name_removelist[h])
                                processes.remove(name_removelist[h])
                        if (len(name_changp) > 0):
                            log.info("original: %s" % processes)
                            for hty in range(len(name_changp)):
                                processes.remove(name_changp[hty])
                                log.info("Subtract_Process: %s" % name_changp[hty])
                                sleep(3)
                                t = MyProcess(name=name_changp[hty].name)
                                t.start()
                                processes.append(t)
                            log.info("Add Process")
                except Exception as e:
                    log.info("Not: %s" % e)
                    # time.sleep(5)
                    # print((processes))
            # log.info("PROCESS: "+ str(processes))
                #'values_changed'
                try:
                    if((result_diff['values_changed'])!= None):
                        log.info("config modify value_change")
                        # print(len(result_diff['values_changed']))
                        for u in ((result_diff['values_changed'])):
                            # print("name: ", h.split('root'))
                            change_process = u.split('root')
                            # print("namesp: ", change_process)
                            # print("element: ", (result_diff['values_changed'].get(u)))
                            for q in processes:
                                # print(change_process[1])
                                index_change = change_process[1].find(q.name)
                                # print("namesp1: ", (index_change),change_process[1])
                                if (index_change == int(2)):
                                    # print(q)
                                    log.info(" %s value_change:" % q.name + str(result_diff['values_changed'].get(u)))
                                    name_changevalue1.append(q)
                                    q.terminate()
                                    sleep(3)
                                    break
                        if (len(name_changevalue1) > 0):
                            log.info("original: %s" % processes)
                            for h in range(len(name_changevalue1)):
                                # log.info("name: "+ str(name[h]))
                                # print("name", name_changevalue1[h])
                                processes.remove(name_changevalue1[h])
                                log.info("Subtract_Process: %s" % name_changevalue1[h])
                                # print("減少Process")
                                sleep(3)
                                # print(name_changevalue1[h].name)
                                t = MyProcess(name=name_changevalue1[h].name)
                                t.start()
                                processes.append(t)
                            log.info("Add Process")
                            log.info("complete_update: %s" % processes)
                except Exception as e:
                    log.info("Not: %s" % e)
                #'iterable_item_added'
                try:
                    if ((result_diff['iterable_item_added']) != None):
                        log.info("config modify item_added")
                        for vb in(result_diff['iterable_item_added']):
                            double_com_1 = vb.split('root')
                            for rr in processes:
                                index_change_item_1 = double_com_1[1].find(rr.name)
                                if (index_change_item_1 == int(2)):
                                    # print(q)
                                    log.info(" %s add value_item:" % rr.name + str(double_com_1))
                                    name_additem1.append(rr)
                                    rr.terminate()
                                    sleep(3)
                                    break
                except Exception as e:
                    log.info("Not: %s" %e)
                if (len(name_additem1) > 0):
                    log.info("original: %s" % processes)
                    for hd in range(len(name_additem1)):
                        # log.info("name: "+ str(name[h]))
                        # print("name", name_changevalue[h])
                        processes.remove(name_additem1[hd])
                        log.info("Subtract_Process: %s" % name_additem1[hd])
                        sleep(3)
                        # log.info("delete_processing: %s" %processes)
                        # print(name_changevalue[h].name)
                        t = MyProcess(name=name_additem1[hd].name)
                        t.start()
                        processes.append(t)
                    log.info("Add Process")
            # print((processes))
            log.info("PROCESS: %s" %processes)
            # config_initial = self.update_config
        except Exception as e:
            log.error("mointer_process: "+str(e))
            # print("error", e)

# FTP function
class WatchdogWithFile(FileSystemEventHandler):
    def __init__(self, lock, settingcon,LOG):
        FileSystemEventHandler.__init__(self)
        self.cnt = 0
        self.timer = None
        self.lock = lock
        self.settingcon = settingcon
        self._LOG = LOG
        print("save",self.settingcon.get("savefilename"))
    def on_modified(self, event):
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(0.2, self.setup, args=(event.src_path, self.settingcon, self.lock,self._LOG))
        self.timer.start()
    def setup(self,filename,pathconfig ,Lock, Log):
        Lock.acquire()
        try:
            targetPattern = pathconfig.get("remotefilesite") + pathconfig.get("filenametype")
            print(targetPattern)
            kind_file = glob.glob(targetPattern)
            print(kind_file)
            if len(kind_file) > 0:
                Log.info(str(kind_file) + " " + str(filename) + " --select")
                # print(str(kind_file) + " " + str(filename) + "  select")
                for i in range(len(kind_file)):
                    file_type = kind_file[i].split('\\')
                    kind_type = (filename.split('\\'))
                    if file_type[(len(file_type) - 1)] == kind_type[(len(kind_type) - 1)]:
                        SMGFTP = ftpconnect(pathconfig.get("ip"), pathconfig.get("port"), pathconfig.get("username"), pathconfig.get("password"))
                        # print(SMGFTP.getwelcome())  # 獲得歡迎資訊A
                        SMGFTP.cwd(pathconfig.get("savefilename"))  # 設定FTP路徑
                        # print(filename, "Add file")
                        Log.info(filename + " --Add file")
                        print(open(filename, 'rb'), "read")
                        SMGFTP.storbinary("STOR " + str(kind_type[(len(kind_type) - 1)]), open(filename, 'rb'))
                        SMGFTP.quit()
                        break
            else:
                # print(str(kind_file) + " " + str(filename) + "  false")
                Log.info(str(kind_file) + " " + str(filename) + "  False")
        except Exception as e:
            Log.error("FTP_ERROR: "+ str(e))
            # print("ERROR " + str(e))
        finally:
            Lock.release()
            self.timer = None

# DB function
class DBTask(Thread):
    def __init__(self,mysqllock, mssqllock, mysqlpool, mssqlpool,tablename,table,LOG):#mysqllock, mssqllock, mysqlpool, mssqlpool,
        super().__init__()
        self._mysqllock = mysqllock
        self._mssqllock = mssqllock
        self._mysqlpool = mysqlpool
        self._mssqlpool = mssqlpool
        self._tablename = tablename  # 表名稱F12AP3_DIFLOW_2FC05
        self._table = table  # 庫名
        self._LOG = LOG
    def run(self):
        try:
            self._LOG.info("Run DB")
            # print("run DB")
            tablecolum = SelectMysqlDB_Tablecolumn(self._mysqlpool, self._mysqllock, self._tablename, self._table, self._LOG)
            # self._LOG.info(tablecolum)
            # print(tablecolum)
            tablename_parsing_count = self._tablename.split('_')
            insertcontentname = []
            for i in range(len(tablecolum)):
                insertcontentname.append(tablecolum[i][0])
            self._LOG.info(insertcontentname)
            # print(list(insertcontentname))
            # ttFBDC = SelectSQL_Tableallparameter(self._mssqlpool, self._mssqllock, self._tablename, self._LOG)
            # print(ttFBDC)
            ttSMG = SelectMysql_Tableallparameter(self._mysqlpool, self._mysqllock, self._tablename, ["1"], ["1"], self._LOG)
            if (len(tablename_parsing_count) == 3):
                self._LOG.info("Pass")
                # print("pass")
            else:
                if(len(ttSMG)==int(0)):
                    self._LOG.info("無收值")
                else:
                    for i in range(len(ttSMG)):
                        try:
                            self._LOG.info("_" + "insert")
                            # print()
                            insert_index = InsertSQL(self._mssqlpool, self._mssqllock,self._tablename,[str(insertcontentname[0]), str(insertcontentname[1]),
                            str(insertcontentname[2]), str(insertcontentname[5])],[str(ttSMG[i][0]), str(ttSMG[i][1]), str(ttSMG[i][2]),
                            str((datetime.datetime.strptime((str(ttSMG[i][6]).split('.')[0]),'%Y-%m-%d %H:%M:%S'))-(datetime.timedelta(hours=8)))],(self._LOG))
                            if (insert_index == True):
                                DeletSQL(self._mysqlpool, self._mysqllock, self._tablename, [str(insertcontentname[0])], [str(ttSMG[i][0])], ["="], self._LOG)
                            else:
                                self._LOG.info("insert _fail")
                        except Exception as e:
                            self._LOG.error(str(e))
                            # print("ERROR " + str(e))
                self._LOG.info('%s 完成!' % (self._tablename))
                    # print('%s 完成!' % (self._tablename))
        except Exception as e:
            self._LOG.error("DB_ERROR: "+ str(e))
            # print("error", e)

# MQTT function
class MQTTTask(object):
    def __init__(self,mqtt_ID, mqtt_object, mqtt_host, mqtt_port, mqtt_user, mqtt_pass, topic_name, process_name ,LOG, mqttpool, mqttlock):
        global test_name

        test_name = topic_name
        # self.test_name = test_name
        super(MQTTTask, self).__init__()
        self.mqtt_ID = mqtt_ID
        self.topic_name = topic_name
        self.mqtt_object = mqtt_object
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_pass = mqtt_pass
        # self.process_name = process_name
        self.LOG = LOG
        self.mqttpool = mqttpool
        self.mqttlock = mqttlock
        self.client = None
        # global index
        self.client = self.start(self.mqtt_object,self.mqtt_user, self.mqtt_pass,self.mqtt_host, self.mqtt_port)
        # # 建立 MQTT Client 物件
        # client = mqtt.Client(mqtt_object)
        # # 設定建立連線回呼函數
        # # client.message_callback_add(name_json, self.on_connect)
        # client.on_connect = self.on_connect
        # # 設定接收訊息回呼函數
        # client.on_message = self.on_message
        # # 設定登入帳號密碼
        # client.username_pw_set(mqtt_user, mqtt_pass)
        # client.connect(mqtt_host, int(mqtt_port),60)
        # # print("client",client)
        # # 保持連接
        # client.loop_start()
        while True:
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

                    # self.client.loop_stop()
                    cnt=0
                # print("MQTT_ERROR",e)
                # client.loop_stop()

                    self.client.loop_stop()
                    # self.client.reconnect()
                    # self.client.loop_start()
                    self.client = self.start(self.mqtt_object, self.mqtt_user, self.mqtt_pass, self.mqtt_host,self.mqtt_port)
        # Rec1 = threading.Thread(name='thread', target=search_topic(), args=(processes,))
        # Rec1.start()

    def start(self,mqtt_object1,mqtt_user1, mqtt_pass1,mqtt_host1, mqtt_port1):
        client = mqtt.Client(mqtt_object1)
        # 設定建立連線回呼函數
        client.message_callback_add("abc", self.on_message)
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
                print(test_name)
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

def send_mess(dgf):
    sleep(5)
    print(threading.current_thread().ident)
    print("send",dgf)



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

# Modbus function
class ModbusTask(object):
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
    # def run(self):
        while True:
            try:
                if(ip_exchange==True):
                    self.master.close()
                    sleep(3)
                    method, con_json = Config()
                    self.modbus_config_1 = con_json.get(self.modbus_process_name)
                    self.master = modbus_tcp.TcpMaster(host=self.modbus_config_1.get("ip"), port=502)
                    self.master.set_timeout(5.0)
                    print("reconnect")
                    ip_exchange = False
                # ee= len(self.modbus_config_1)
                for b in range(len(self.modbus_config_1.get("svid"))):
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
# main process
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
                            # con_json = DBConfig("DB", p1)
                            # if (con_json == None):
                            #     p1.info('[' + multiprocessing.current_process().name + ']' + " config 參數為空")
                            # else:
                            #     for gon in range(len(con_json)):
                            #         if(con_json[gon][1] == multiprocessing.current_process().name):
                            #             poolmysql, poolmssql = pool_configdb(con_json[gon], p1)
                            #             lockmysql = threading.Lock()
                            #             lockmssql = threading.Lock()
                            #             Get_all_tablename = SelectMysqlDB(poolmysql, lockmysql,con_json[gon][6], p1)
                            #             p1.info("Table: " + str(Get_all_tablename))
                            #             for a in range(len(Get_all_tablename)):
                            #                 p1.info("Table list: " + str(Get_all_tablename[a][0]))
                            #                 # print("table list: ", Get_all_tablename[a][0])
                            #                 # print(DB[4])
                            #                 task = DBTask(lockmysql, lockmssql, poolmysql, poolmssql,
                            #                               Get_all_tablename[a][0], con_json[gon][6],
                            #                               p1)  # lockmysql,lockmssql, poolmysql, poolmssql,
                            #                 process_thread.append(task)
                            #                 process_thread[a].start()
                            #                 # for wt in range(len(Get_all_tablename)):
                            #                 process_thread[a].join()
                            #             break
                            # end = time()
                            # p1.info('總共耗費了 %.3f 秒.' % (end - start))
                            # # print('總共耗費了 %.3f 秒.' % (end - start))
                            # sleep(600)

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

                                # print('[' + multiprocessing.current_process().name +']'," config 參數為空")
                            else:
                                lockmysql = threading.Lock()
                                lockmssql = threading.Lock()
                                print(DB_json.get("mysqltablename"))

                                poolmysql, poolmssql = pool_config(DB_json,p1)
                                print(poolmysql)
                                print(poolmssql)
                                if(poolmysql != None and poolmssql != None):
                                # poolmssql = pool_config1(DB_json,p1)
                                # print(poolmssql)
                                # print(poolmysql,poo+lmssql)
                                    Get_all_tablename = SelectMysqlDB(poolmysql, lockmysql, DB_json.get("mysqltablename"),p1)
                                    p1.info("Table: "+ str(Get_all_tablename))
                                # print("table", Get_all_tablename)
                                    for a in range(len(Get_all_tablename)):
                                        p1.info("Table list: " + str(Get_all_tablename[a][0]))
                                    # print("table list: ", Get_all_tablename[a][0])
                                    # print(DB[4])
                                        task = DBTask(lockmysql, lockmssql, poolmysql, poolmssql, Get_all_tablename[a][0], DB_json.get("mysqltablename"), p1) #lockmysql,lockmssql, poolmysql, poolmssql,
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
                        event_handler = WatchdogWithFile(lockFTP, local_path, p1)
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
                            mqtt_pool = pool_MQ_config(mqtt_config,p1)

                            MQTTTask(mqtt_config.get("ID"),mqtt_config.get("clientname"), mqtt_config.get("ip"), mqtt_config.get("port")
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
                            ModbusTask(master,multiprocessing.current_process().name,modbus_config)
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

def foo():
    # time.sleep(3)
    while True:
        print("success")
        time.sleep(3)
        # hh = ['MQTT','DB1']
        # process1=[]
        # for i in range(len(hh)):
        #     t = MyProcess(name=hh[i])
        #     process1.append(t)
        #     process1[i].start()
        #
        #     print('Process running:', process1[i], process1[i].is_alive())
        # for i in range(len(hh)):
        #     process1[i].join()
        # time.sleep(12)

    # gg = MyProcess(name='MQTT')
    # gg.daemon = True
    # gg.start()
    # print('Process running:', gg, gg.is_alive())
    # print("123")
    # gg.terminate()
    # print('Process terminated:', gg, gg.is_alive())
    # ggh = MyProcess(name='DB1')
    # ggh.start()
    # gg.join()
    # ggh.terminate()
    # ggh.join()
    # p.terminate()
    # p.join()
    # print('Process joined:', ggh, ggh.is_alive())
    # print('Process exit code:', ggh.exitcode)
    # name = multiprocessing.current_process().name
    # print("Starting %s \n" % name)
    # time.sleep(3)
    # print("Exiting %s \n" % name)


# # DB Pool 連線數
# def pool_configdb(configpool,Log):
#     try:
#         # print(configpool[0].get["mysqlip"])
#         aa= configpool[1]
#         pool_mysql = PooledDB(pymysql, mincached=10, maxcached=20, maxconnections=1200, maxshared=10, maxusage=600,
#                         blocking=True, host=configpool[2], port=int(configpool[3]), user=configpool[4], passwd=configpool[5],
#                         db=configpool[6], charset='utf8')
#         pool_mssql = PooledDB(creator=pymssql, mincached=2, maxcached=20, maxconnections=1200, maxshared=100,maxusage=600,
#                         blocking=True, host=configpool[7], server=configpool[8], user=configpool[9], password=configpool[10],
#                         database=configpool[11])
#         return pool_mysql, pool_mssql
#     except Exception as e:
#         Log.error("pool_config: "+str(e))
# DB Pool 連線數
def pool_config(configpool,Log):
    try:
        # print(configpool[0].get["mysqlip"])
        pool_mysql = PooledDB(pymysql, mincached=10, maxcached=20, maxconnections=1200, maxshared=10, maxusage=600,
                        blocking=True, host=configpool.get("mysqlip"), port=int(configpool.get("mysqlport")), user=configpool.get("mysqlusername"), passwd=configpool.get("mysqlpassword"),
                        db=configpool.get("mysqltablename"), charset='utf8')
        pool_mssql = PooledDB(creator=pymssql, mincached=2, maxcached=20, maxconnections=1200, maxshared=100,maxusage=600,
                        blocking=True, host=configpool.get("mssqlip"), server=configpool.get("mssqlserver"), user=configpool.get("mssqlusername"), password=configpool.get("mssqlpassword"),
                        database=configpool.get("mssqltablename"))
        return pool_mysql, pool_mssql
    except BaseException as e:
        Log.error("DB_connect_error: " + str(e))
        pool_mysql = None
        pool_mssql = None
        return pool_mysql, pool_mssql
    except Exception as e:
        Log.error("pool_config: "+str(e))
        pool_mysql = None
        pool_mssql = None
        return pool_mysql, pool_mssql
# def pool_config1(configpool1,Log1):
#     try:
#         pool_mssql = PooledDB(creator=pymssql, mincached=2, maxcached=20, maxconnections=1200, maxshared=100,maxusage=600,
#                         blocking=True, host=configpool1.get("mssqlip"), server=configpool1.get("mssqlserver"), user=configpool1.get("mssqlusername"), password=configpool1.get("mssqlpassword"),
#                         database=configpool1.get("mssqltablename"))
#         # con = pool_mssql.connection()
#         return pool_mssql
#     except BaseException as e:
#         print(f'数据库链接错误{e}')
#     except Exception as e:
#         Log1.error("pool_config1: "+str(e))
#         pool_mssql = None
#         return pool_mssql

# MySQL 語法
def SelectMysqlDB(dbpool,lock,TableName,Log):
    lock.acquire()
    try:
        conn = dbpool.connection()
        cur = conn.cursor()
        # 讀取MySQL數據庫資料
        # SQL = "SHOW DATABASES"
        SQL = "SHOW TABLES FROM `" + TableName + "`"
        Log.info(SQL)
        # print(SQL)
        # log.info(SQL)
        cur.execute(SQL)
        GetDatamy1 = cur.fetchall()
    except Exception as e:
        Log.error(" SelectmysqlTable_Fail: " + "MYSQLDB_table_list" + " " + str(e))
        # print("ERROR" + " SelectmysqlTable_Fail: " + "MYSQLDB_table_list" + " " + str(e))
        # LogNmae.error(" SelectTable_Fail: " + TableName + " " + str(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()
    lock.release()
    return GetDatamy1
def SelectMysqlDB_Tablecolumn(dbpool,lock,TableName,Table,Log):

    lock.acquire()
    GetDatamy2=None
    try:
        conn = dbpool.connection()
        cur = conn.cursor()
        # 讀取MySQL數據庫資料
        # SQL = "SHOW DATABASES"
        SQL = "SHOW COLUMNS FROM `" + TableName + "`" +"FROM `" + Table + "`"
        # print(SQL)
        Log.info(SQL)
        cur.execute(SQL)
        GetDatamy2 = cur.fetchall()
    except Exception as e:
        # log.error(" SelectmysqlTable_Fail: " + "MYSQLDB_table_list" + " " + str(e))
        Log.error("ERROR" + " SelectmysqlTable_Fail: " + "MYSQLDB_table_list" + " " + str(e))
        # print("ERROR" + " SelectmysqlTable_Fail: " + "MYSQLDB_table_list" + " " + str(e))
        # LogNmae.error(" SelectTable_Fail: " + TableName + " " + str(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()
    lock.release()
    return GetDatamy2
def SelectMysql_Tableallparameter(dbpool,lock,TableName, ContentName, SelectName, Log):

    lock.acquire()
    try:
        conn = dbpool.connection()
        cur = conn.cursor()
        # 讀取MySQL數據庫資料
        SQL = "SELECT * FROM `" + TableName + "` WHERE "
        for count in range(len(ContentName)):
            SQL += ContentName[count]
            SQL += '="'
            SQL += SelectName[count]
            SQL += '"'
            if count < len(ContentName) - 1:
                SQL += " and "
        Log.info(SQL)
        # print(SQL)
        # log.info(SQL)
        cur.execute(SQL)
        GetDatamy3 = cur.fetchall()
    except Exception as e:
        # log.error(" SelectTable_Fail: " + TableName + " all" + str(e))
        Log.error("ERROR" + " SelectTable_Fail: " + TableName + " all" + str(e))
        # print("ERROR" + " SelectTable_Fail: " + TableName + " all" + str(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()
    lock.release()
    return GetDatamy3
def DeletSQL(dbpool,lock,TableName, ContentName, Content, mark, Log):
    global e
    lock.acquire()
    try:
        conn = dbpool.connection()
        cur = conn.cursor()
        SQL = "DELETE FROM " + TableName + " WHERE "
        for count in range(len(ContentName)):
            SQL += ContentName[count]
            SQL += mark[count]
            SQL += "'"
            SQL += Content[count]
            SQL += "'"
            if count < len(ContentName) - 1:
                SQL += ' and '
        Log.info(SQL)
        # print(SQL)
        # log.info(SQL)
        cur.execute(SQL)
        conn.commit()

    except Exception as e:
        conn.rollback()
        # log.error(" DeletTable_Fail: " + TableName + " " + str(e))
        Log.error("ERROR" + " DeletTable_Fail: " + TableName + " " + str(e))
        # print("ERROR" + " DeletTable_Fail: " + TableName + " " + str(e))
    finally:
        cur.close()
        conn.close()
    lock.release()

# MS SQL 語法
def SelectSQL_Tableallparameter(dbpool,lock,TableName,Log):
    global e
    lock.acquire()
    try:
        GetData3 = None
        conn = dbpool.connection()
        cur = conn.cursor()
        # 讀取MySQL數據庫資料
        SQL = "SELECT * FROM " + TableName
        Log.info(SQL)
        print(SQL)
        cur.execute(SQL)
        GetData3 = cur.fetchall()
    except Exception as e:
        Log.error(" SelectTable_Fail: " + TableName + " " + str(e))
        # print("ERROR" + " SelectTable_Fail: " + TableName + " " + str(e))
        # LogNmae.error(" SelectTable_Fail: " + TableName + " " + str(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()
    lock.release()
    return GetData3
def InsertSQL(dbpool,lock, TableName, ContentName, Content, Log):
    global e
    lock.acquire()
    try:
        conn = dbpool.connection()
        cur = conn.cursor()
        SQL = "INSERT INTO " + '"' + TableName + '"' + " ( "
        for count in range(len(ContentName)):
            SQL += ContentName[count]
            if count < len(ContentName) - 1:
                SQL += " , "
        SQL += ' ) VALUES ('
        for count1 in range(len(Content)):
            SQL += "'"
            SQL += Content[count1]
            SQL += "'"
            if count1 < len(Content) - 1:
                SQL += " , "
        SQL += ' ) '
        Log.info(SQL)
        # print(SQL)
        cur.execute(SQL)
        conn.commit()
        GetData_insert = True
    except Exception as e:
        # 有異常，回滾事務
        # print("ERROR" +" InsertTable_Fail: " + TableName + " " + str(e))
        # error_class = e.__class__.__name__  # 取得錯誤類型
        detail = e.args[0]  # 取得詳細內容D
        if(str(detail)=="2627" or str(detail)=="2601"):
            GetData_insert = True
            Log.error("delete duplicated: " + TableName)
            # print("delete duplicated " + TableName)
        else:
            Log.error("InsertTable_Fail: " + TableName + " " + str(e))
            # print(" InsertTable_Fail: " + TableName + " " + str(e))
            GetData_insert = False
        # print("1"+ str(error_class))
        Log.error(str(detail))
        # print("2"+ str(detail))
        conn.rollback()
    finally:
        cur.close()
        conn.close()
    lock.release()
    return GetData_insert


# 監控 process
def mointer_process():
    event_watchdog = Mywatchdog()
    obw = Observer()
    # obw.schedule(event_watchdog, path=(path_file) + "\\FOC\\Config", recursive=True)
    obw.schedule(event_watchdog, path="C:\\xampp\\mysql\\data\\config", recursive=True)
    obw.start()

    try:
        while True:
            print(processes)
            # print("initial", initial_config)
            sleep(5)
    except KeyboardInterrupt:
        print("except")
        obw.stop()
    obw.join()
        # sleep(5)
        # print(len(processes))
        # if(a==10 and processes[0].is_alive() == True):
        #     processes[0].terminate()
        #     print('Process stop:', processes[0], processes[0].is_alive())
        # if(a==12 and processes[0].is_alive() == False):
        #     t = MyProcess(name="MQTT")
        #     t.start()
        #     processes.append(t)
        # a=a+1
        # print(a)
    # time.sleep(10)
    # aa.terminate()

def recordlog(log,path,file,log_handler):
    while True:

        try:
            sleep(1)
            # 判斷檔案路徑
            if not os.path.exists(path):
                os.makedirs(path)
                # if str(datetime.datetime.today().strftime("%M")) < str(30):
                #     file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-Log紀錄")
                # else:
                #     file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-30') + "-Log紀錄")
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
            # elif os.path.exists(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-30') + "-Log紀錄") and str(datetime.datetime.today().strftime("%M")) >= str(30):
            #     continue
            else:
                log.removeHandler(file)
                log.removeHandler(log_handler)
                # if str(datetime.datetime.today().strftime("%M")) < str(30):
                #     file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-00') + "-Log紀錄")
                # else:
                #     file = logging.FileHandler(path + datetime.datetime.today().strftime('%Y-%m-%d-%H-30') + "-Log紀錄")
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

if __name__ == '__main__':
    try:
        path = (os.path.dirname(os.path.abspath(__file__))) + "\\FOC\\"
        print(os.path.dirname(os.path.abspath(__file__)))
        pathmain = path + "Main\\"
        log = logging.getLogger("main")
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
        log.info(str("SuccessLog"))
    except Exception as e:
        print(e)

    RecordLogmain = threading.Thread(target=recordlog, args=(log, pathmain, file, log_handler))
    RecordLogmain.start()
    asa=[]
    Rec1 = threading.Thread(name="Thread", target=mointer_process)
    Rec1.start()

    combine_json(log)
    # lock_config = threading.Lock()
    # pool_mysqlt = PooledDB(pymysql, mincached=10, maxcached=20, maxconnections=1200, maxshared=10, maxusage=600,
    #                       blocking=True, host="localhost", port=int(3306),
    #                       user="root", passwd="", db="Config", charset='utf8')

    # P_list="["
    # t_p = DBConfig("processname",log)
    # con_json = DBConfig("DB", log)
    # re_process=""
    # for gon in range(len(con_json)):
    #     k = len(con_json[gon])
    #     re_process+='"'+ (con_json[gon][1])+'"' + ":" + "{" + '"mysqlip"'+":" + '"'+ con_json[gon][2] + '"' + "," +\
    #     '"mysqlport"' + ":" + '"' + con_json[gon][3] + '"' + "," + '"mysqlusername"' + ":" + '"' + con_json[gon][4] + '"'+ ","+ \
    #     '"mysqlpassword"' + ":" + '"' + con_json[gon][5] + '"' + "," + '"mysqltablename"' + ":" + '"' + con_json[gon][6] + '"'+ ","+ \
    #     '"mssqlip"' + ":" + '"' + con_json[gon][7] + '"' + "," + '"mssqlserver"' + ":" + '"' + con_json[gon][8] + '"'+ ","+ \
    #     '"mssqlusername"' + ":" + '"' + con_json[gon][9] + '"' + "," + '"mssqlpassword"' + ":" + '"' + con_json[gon][10] + '"'+ ","+ \
    #     '"mssqltablename"' + ":" + '"' + con_json[gon][11] + '"' + "}"
    #     if (gon<(len(con_json)-1)):
    #         re_process+=","
    # con_json = DBConfig("MQTT", log)
    # for gom in range(len(con_json)):
    #     re_process+=","+'"'+(con_json[gom][1])+'"' + ":" + "{" +'"clientname"'+":"+'"'+con_json[gom][2]+'"'+","+\
    #         '"ip"'+":"+'"'+con_json[gom][3]+'"'+","+'"port"'+":"+str(con_json[gom][4])+","+\
    #         '"username"'+":"+'"'+con_json[gom][5] + '"' + "," +'"password"'+":"+'"'+con_json[gom][6]+'"'+","+\
    #         '"topic"'+":"+ (con_json[gom][7])+"}"
    #     if (gom<(len(con_json)-1)):
    #         re_process+=","
    # con_json = DBConfig("Modbus", log)
    # for goj in range(len(con_json)):
    #     re_process += "," + '"' + (con_json[goj][1]) + '"' + ":" + "{"+'"ip"'+":"+'"'+con_json[goj][2]+'"'+","+\
    #         '"svid"'+":"+con_json[goj][3]+","+'"sensorcnt"'+":"+'"'+(con_json[goj][4])+'"'+","+\
    #         '"site"'+":"+'"'+con_json[goj][5] + '"' + "," +'"location"'+":"+'"'+con_json[goj][6]+'"'+","+\
    #         '"sensor"'+":"+'"'+ (con_json[goj][7])+'"'+","+'"tag"'+":"+con_json[goj][8]+"}"
    #     if (goj<(len(con_json)-1)):
    #         re_process+=","
    # con_json = DBConfig("FTP", log)
    # for goy in range(len(con_json)):
    #     re_process += "," + '"' + (con_json[goy][1]) + '"' + ":" + "{" + '"ip"' + ":" + '"' + con_json[goy][2] + '"' + "," +\
    #         '"port"' + ":" + str(con_json[goy][3]) + "," + '"username"' + ":" + '"' + (con_json[goy][4]) + '"' + "," + \
    #         '"password"' + ":" + '"' + con_json[goy][5] + '"' + "," + '"savefilename"' + ":" + '"' + con_json[goy][6] + '"' + "," +\
    #         '"filenametype"' + ":" + '"' + (con_json[goy][7]) + '"' + "," + '"remotefilesite"' + ":" + '"'+ con_json[goy][8] + '"'+ "}"
    #     if (goj < (len(con_json) - 1)):
    #         re_process += ","
    # # path = 'C:\\Users\\User\\PycharmProjects\\DB\\FOC\\Config\\test.txt'
    # # f = open(path, 'w')
    # # f.write(str(dertt))
    # # f.close()
    # #Get_all_tablename = SelectMysqlDB(pool_mysqlt, lock_config, "Config", log)
    # #kpo = Get_all_tablename[0][0]
    # # tablecolum = SelectMysqlDB_Tablecolumn(pool_mysqlt, lock_config, Get_all_tablename[0][0], "Config", log)
    # # t_p = SelectMysql_Tableallparameter(pool_mysqlt, lock_config, "processname", ["1"], ["1"], log)
    # re_data = ""
    # for y in range(len(t_p)):
    #     P_list+='"'+ t_p[y][1] +'"'
    #     if (y < (len(t_p)-1)):
    #         P_list+=","
    # P_list+="]"
    #     # P_list.append(y[1])
    #
    # # for ef in range(len(P_list)):
    # #     function = (P_list[ef].split('_'))
    # #     if(function[0] == "DB"):
    #
    # re_data ="{"+'"'+"Process"+'"'+":"+ str(P_list) + "," + str(re_process) + "}"
    # path='C:\\Users\\User\\PycharmProjects\\DB\\FOC\\Config\\test.txt'
    # f = open(path, 'w')
    # f.write(str(re_data))
    # f.close()
    try:
        listprocess, initial_config = Config()
        # print(initial_config)
        log.info(listprocess)
        # print(listprocess)
        for i in range(len(listprocess)):
            print(listprocess[i])
            t = MyProcess(name=listprocess[i])
            t.start()
            print(t)
            processes.append(t)

        # Rec1 = threading.Thread(name='thread', target= mointer_process)
        # Rec1.start()
            # Rec2 = threading.Thread(name='thread1', target=f1, args=(lista,))
            # Rec2.start()
            # time.sleep(20)
            # processes[0].terminate()
            # time.sleep(2)
            # print('Process stop:', processes[0], processes[0].is_alive())
        # while True:
        #     try:
        #         check_process, setting = Config()
        #         log.info("con update: " + str(setting))
        #         # print("con update", setting)
        #         if (len(check_process) >= len(processes)):
        #             name = []
        #             for i in range(len(check_process)):
        #                 check_index = False
        #                 for y in range(len(processes)):
        #                     if (check_process[i] == processes[y].name):
        #                         check_index = True
        #                         break
        #                 if (check_index == True):  # process存在
        #                     # log.info("存在: " + check_process[i])
        #                     print("存在", check_process[i], str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        #                 else:
        #                     # processes[i].terminate()  # process不存在
        #                     # log.info("不存在 需新增: "+ check_process[i])
        #                     print("不存在 需新增", check_process[i],
        #                           str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        #                     name.append(check_process[i])
        #                     sleep(3)
        #             if (len(name) > 0):
        #                 for h in range(len(name)):
        #                     t = MyProcess(name=name[h])
        #                     t.start()
        #                     processes.append(t)
        #                     log.info("新增Process")
        #                     # print("新增Process")
        #         elif (len(check_process) < len(processes)):
        #             name = []
        #             for i in range(len(processes)):
        #                 check_index = False
        #                 for y in range(len(check_process)):
        #                     if (processes[i].name == check_process[y]):
        #                         check_index = True
        #                         break
        #                 if (check_index == True):  # process存在
        #                     # log.info("存在: " + processes[i].name)
        #                     print("存在", processes[i].name, str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        #                 else:
        #                     processes[i].terminate()  # process不存在
        #                     # log.info("不存在 需刪除: " + processes[i].name)
        #                     print("不存在 需刪除", processes[i].name,
        #                           str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        #                     name.append(processes[i])
        #                     # log.info("減少Process")
        #                     print("減少Process")
        #                     sleep(3)
        #             # print((processes))
        #             if (len(name) > 0):  # 刪除要關閉之process
        #                 for h in range(len(name)):
        #                     # log.info("name: "+ str(name[h]))
        #                     print("name", name[h])
        #                     processes.remove(name[h])
        #                 # time.sleep(5)
        #         # log.info("PROCESS: "+ str(processes))
        #         print((processes[0].is_alive()))
        #         print((processes[0]))
        #         if(processes[0].is_alive()!=True):
        #             processes[0].terminate()
        #             sleep(3)
        #             print((processes[0].is_alive()))
        #             print(processes[0].exitcode)
        #             print(-signal.SIGTERM)
        #
        #     except Exception as e:
        #         # log.error("mointer_process: "+str(e))
        #         print("error", e)
        #     sleep(5)
        for i in range(len(processes)):
            # log.info("PROCESS:", len(processes), processes)
            # print("PROCESS:", len(processes), processes)
            processes[i].join()

        # print("success1")
            # time.sleep(10)

            # t.join()
        # t.join(timeout=5)
    # process_with_name.daemon = True  # 注意原代码有这一行，但是译者发现删掉这一行才能得到正确输出
    #     process_with_default_name = multiprocessing.Process(target=foo)
    #     p.start()
        # p.terminate()
        # p.join()
        # print('Process joined:', p, p.is_alive())
        # print('Process exit code:', p.exitcode)
        # process_with_default_name.start()
    except KeyboardInterrupt as e:
        print("keymain", e)
        pid = os.getpid()
        os.popen('taskkill.exe /f /pid:%d' % pid)
    except Exception as e:
        log.error(e)
        # print(e)