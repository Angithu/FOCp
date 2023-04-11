import pymysql, pymssql
from dbutils.pooled_db import PooledDB

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