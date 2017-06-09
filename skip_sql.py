#!/usr/bin/env python
#-*-coding:utf8-*-
import MySQLdb
import time
class Connection():
    def __init__(self,host,port,username,password,database,charset="utf8"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.charset = charset
        self.connect()

    def connect(self):
        self.conn = MySQLdb.connect(host=self.host, user=self.username, passwd=self.password, db=self.database, port=self.port, charset=self.charset)
        self.cursor = self.conn.cursor()
    
    def Query(self,query):
        self.cursor = self.conn.cursor()
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        self.cursor.close()
        return results

    def close(self):
        self.conn.close()

class Run(Connection):

    def kill_id(self):
        sql = "select ID from  information_schema.processlist where USER !='opsadmin' and user !='rdsadmin'"
        results = self.Query(sql)
        if results != ():
            for row in results:
                for id in row:
                    sql = "CALL mysql.rds_kill(%s)"%(id)
                    self.Query(sql)
        return

def main():
    try:
        r = Run("db-nlp-slave.cxleyzgw272j.us-east-1.rds.amazonaws.com",3306,"opsadmin","inveno_cn!admin+2016","information_schema")
        
        while True:
            r.kill_id()
            time.sleep(1)

    except Exception as e:
        print e

    finally:
        r.close()

if __name__ == "__main__":
    main()


