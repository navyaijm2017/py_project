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
        #self.cursor.close()
        self.conn.close()

class Run(Connection):

    def show_slave_status(self):
        sql = "show slave status"
        results = self.Query(sql)
        Slave_IO_Running = results[0][10]
        Slave_SQL_Running = results[0][11]
        Seconds_Behind_Master = results[0][32]
        if Slave_IO_Running != 'Yes' or Slave_SQL_Running != 'Yes':
            print "slave is not ok", Slave_IO_Running, Slave_SQL_Running
            return False
        elif int(Seconds_Behind_Master) > 300:
            print "slave behind master: %s" % Seconds_Behind_Master
            return False    
        else:
            print "slave is ok"
            return True

    def skip_error(self):
        sql = "CALL mysql.rds_skip_repl_error"
        return self.Query(sql)
    
    def kill_id(self):
        sql = "select ID from  information_schema.processlist  where USER='system user' and STATE='Waiting for master to send event'"
        results = self.Query(sql)
        if results != ():
            for row in results:
                for id in row:
                    sql = "CALL mysql.rds_kill(%s)"%(id)
                    print self.Query(sql)
        return

def main():
    try:
        r = Run("db-nlp-slave.cxleyzgw272j.us-east-1.rds.amazonaws.com",3306,"opsadmin","inveno_cn!admin+2016","information_schema")
        
        while not r.show_slave_status():
            r.kill_id()
            print r.skip_error()
            time.sleep(1)
        
        print '[check]',r.show_slave_status()

    except Exception as e:
        print e

    finally:
        r.close()

if __name__ == "__main__":
    main()


