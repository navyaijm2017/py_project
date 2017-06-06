#!/usr/bin/env python
#-*-coding:utf8-*-
import requests,time,json
from requests import ConnectionError
from requests import HTTPError
from requests import Timeout
import MySQLdb
from tomorrow import threads
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')
now = time.strftime("%Y-%m-%d %H:%M:%S")

#记录日志的函数
def log_w(text):
    logfile = '/tmp/url.log'
    tt = str(now) + "\t" + str(text) + "\n"
    f = open(logfile,'a+')
    f.write(tt)
    f.close()    
#获取http状态码的函数
@threads(50) 
def get_code (url):
    try:
        r = requests.get(url,allow_redirects = False,timeout=3)
        return r.status_code
    except ConnectionError,e:
        return int(888)
    except HTTPError,e:
        return int(777)
    except Timeout,e:
        return int(666)


#主函数，从数据库查询需要监控的url和需要的信息，通过调用get_code函数获取http状态，做各种判断并且报警
def main():
    try:
        host, port, username, password, database, charset = "172.31.31.100",3306,"monitor","123456","monitor_project","utf8"
        db = MySQLdb.connect(host=host, user=username, passwd=password, db=database, port=port, charset=charset)
        cursor = db.cursor()
        sql = "select * from url_monitor where status='ON'"
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            my_url = row[1]
            mail = row[2]
            mess = row[3]
            get_code (my_url)
            if get_code (my_url) ==301:
                pass
            elif get_code (my_url) ==302:
                pass
            elif get_code (my_url) ==304:
                pass
            elif get_code (my_url) ==307:
                pass
            elif get_code (my_url) != 200:
                if get_code (my_url) ==888:
                    msg = {"phone":"%s" % (mail),"msg":"[%s]地址:%s,用途:%s,状态:连接错误,负责人:%s." % (str(now),my_url,mess,mail)}
                    response = requests.post("http://172.31.31.100:24680/message/weixin",data = json.dumps(msg))
                    text = '地址:%s,用途:%s|状态:连接错误|负责人:%s.' % (my_url,mess,mail)
                    log_w(text)
                elif get_code (my_url) ==777:
                    msg = {"phone":"%s" % (mail),"msg":"[%s]地址:%s,用途:%s,状态:http状态请求失败,负责人:%s." % (str(now),my_url,mess,mail)}
                    response = requests.post("http://172.31.31.100:24680/message/weixin",data = json.dumps(msg))
                    text = '地址:%s,用途:%s|状态:连接错误|负责人:%s.' % (my_url,mess,mail)
                    log_w(text) 
                elif get_code (my_url) ==666:
                    msg = {"phone":"%s" % (mail),"msg":"[%s]地址:%s,用途:%s,状态:连接超时,负责人:%s." % (str(now),my_url,mess,mail)}
                    response = requests.post("http://172.31.31.100:24680/message/weixin",data = json.dumps(msg))
                    text = '地址:%s,用途:%s|状态:连接超时|负责人:%s.' % (my_url,mess,mail)
                    log_w(text) 
                else:
                    code = get_code (my_url)
                    msg = {"phone":"%s" % (mail),"msg":"[%s]地址:%s,用途:%s,状态:%s,负责人:%s." % (str(now),my_url,mess,code,mail)}
                    response = requests.post("http://172.31.31.100:24680/message/weixin",data = json.dumps(msg))
                    text = '地址:%s,用途:%s|状态:%s|负责人:%s.' % (my_url,code,mess,mail)
                    log_w(text)
            else:
                print "http状态正常"
    except:
        msg = {"phone":"haijun.zhao","msg":"[%s] URL监控查询不到数据库了" % (str(now))}
        response = requests.post("http://172.31.31.100:24680/message/weixin",data = json.dumps(msg))
    finally:
        cursor.close()
        db.close()
        return get_code (my_url)
if __name__ == "__main__":
    main()
