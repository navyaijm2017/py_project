#!/usr/bin/env python
#-*-coding:utf8-*-
import sys
import socket,time
import requests
import json
import datetime
import MySQLdb
reload(sys);sys.setdefaultencoding("utf8")

host, port, username, password, database, charset = "172.31.31.10",3306,"monitor","123456","monitor_project","utf8"
#db = MySQLdb.connect(host=host, user=username, passwd=password, db=database, port=port, charset=charset)
try:
    db = MySQLdb.connect(host=host, user=username, passwd=password, db=database, port=port, charset=charset)
    cursor = db.cursor()
    sql = "select * from port_monitor where status='ON'"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        id = row[0]
        ip = row[1]
        port = row[2]
        type = row[3]
        mail = row[4]
        try:
            sc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sc.settimeout(3)
            sc.connect((ip,int(port)))
            msg = "[%s] %s:%s %s is ok!!!!"%(str(datetime.datetime.now()),ip,port,type)
        except:
            msg = {"phone":"%s" % (mail),"msg":"[%s] %s:%s %s is down!!!![负责人:%s]" % (str(datetime.datetime.now()),ip,port,type,mail)}
            response = requests.post("http://172.31.31.100:24680/message/weixin",data = json.dumps(msg))
        finally:
            with open("/tmp/port.log","a+") as o:
                if isinstance(msg,dict):
                    o.write(msg.get("msg")+'\n')
                else:
                    o.write(msg+'\n')
    cursor.close()
    db.close()
except:
    msg = {"phone":"haijun.zhao","msg":"[%s] 端口监控查询不到数据库了" % (str(datetime.datetime.now()))}
    response = requests.post("http://172.31.31.100:24680/message/weixin",data = json.dumps(msg))
    with open("/tmp/port.log","a+") as o:
        o.write(msg.get("msg")+'\n')
finally:
   pass 
