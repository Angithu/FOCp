from threading import Thread
import pymysql, pymssql
from dbutils.pooled_db import PooledDB
import threading
from New_FOC import DB_SQL_Function as DB
import datetime


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
            tablecolum = DB.SelectMysqlDB_Tablecolumn(self._mysqlpool, self._mysqllock, self._tablename, self._table, self._LOG)
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
            ttSMG = DB.SelectMysql_Tableallparameter(self._mysqlpool, self._mysqllock, self._tablename, ["1"], ["1"], self._LOG)
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
                            insert_index = DB.InsertSQL(self._mssqlpool, self._mssqllock,self._tablename,[str(insertcontentname[0]), str(insertcontentname[1]),
                            str(insertcontentname[2]), str(insertcontentname[5])],[str(ttSMG[i][0]), str(ttSMG[i][1]), str(ttSMG[i][2]),
                            str((datetime.datetime.strptime((str(ttSMG[i][6]).split('.')[0]),'%Y-%m-%d %H:%M:%S'))-(datetime.timedelta(hours=8)))],(self._LOG))
                            if (insert_index == True):
                                DB.DeletSQL(self._mysqlpool, self._mysqllock, self._tablename, [str(insertcontentname[0])], [str(ttSMG[i][0])], ["="], self._LOG)
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