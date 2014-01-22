#!/usr/bin/python
#encoding=utf-8
import sys
import MySQLdb
import time
reload(sys)  
sys.setdefaultencoding('utf8')  

host1 = "1"
user1 = "1"
pass1 = "1"
db1 = "1"

host2 = "1"
user2 = "1"
pass2 = "1"
db2 = "1"

tablename = ("testinfo_db","device","device_db","app_traffic_db","data_connection","cell_strength_db","app_list_db")

def singleDB(db_name,limit):
        try:
                conn1 = MySQLdb.connect(host=host1, user=user1, passwd=pass1, db=db1 , charset="utf8" , connect_timeout=30)
                conn2 = MySQLdb.connect(host=host2, user=user2, passwd=pass2, db=db2 , charset="utf8" , connect_timeout=30)
                conn1.ping(True)
                conn2.ping(True)
        except Exception,ex:
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "conn error"
                return False

        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "Table:" + db_name + " Limit:" + str(limit)
        cursor2 = conn2.cursor()
        cursor1 = conn1.cursor()
        cursor2.execute("SET NAMES utf8")
        cursor1.execute("SET NAMES utf8")
        try:
                cursor1.execute("SELECT max(id) FROM " + db_name)
                id1 = cursor1.fetchall()
                id1 = id1[0][0]
        except Exception,ex:
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "id1 error"
                cursor1.close()
                cursor2.close()
                conn1.close()
                conn2.close()
                return False

        try:
                sql = "desc " + db_name
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , sql
                cursor2.execute(sql)
                result2 = cursor2.fetchall()
                sql = "insert into " + db_name + "("
                for des in result2:
                        sql = sql + str(des[0]) + ", "
                dbsql = sql[:-2] + ") values ("
        except Exception,ex:
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "desc table error"
                cursor1.close()
                cursor2.close()
                conn1.close()
                conn2.close()
                return False

        while(True):
                try:
                        cursor2.execute("SELECT max(id) FROM " + db_name)
                        id2_1 = cursor2.fetchall()
                        id2 = id2_1[0][0]
                except Exception,ex:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "id2 error"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False

                print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "Target:" , id1 , "Base:" , id2
                if id1 <= id2 + 10:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , db_name , "?"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False

                try:
                        sql = "SELECT * FROM " + db_name + " where id > " + str(id2) + " limit " + str(limit)
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , sql
                        cursor1.execute(sql)
                        result1 = cursor1.fetchall()
                except Exception,ex:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "select1 error"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False

                try:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "insert"
                        for record in result1:
                                sqltmp = dbsql
                                for re in record:
                                        sqltmp = sqltmp + "'" + str(re) + "', "
                                sqltmp = sqltmp[:-2] + ")"
                        #       print sqltmp
                                cursor2.execute(sqltmp)
                                conn2.commit()
                except Exception,ex:
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , ex;
                        conn2.rollback()
                        print time.strftime('%Y-%m-%d %X' , time.localtime(time.time())) , "select2 error"
                        cursor1.close()
                        cursor2.close()
                        conn1.close()
                        conn2.close()
                        return False
                break

        cursor1.close()
        cursor2.close()
        conn1.close()
        conn2.close()
        return True

if __name__ == '__main__':
        while(True):
                for name in tablename:
                        while(singleDB(name,50)):
                                time.sleep(0.1)
                                continue
                        time.sleep(1)
                time.sleep(60)
