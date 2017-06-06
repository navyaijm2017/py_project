#!/usr/bin/env python
#coding:utf-8
import sys
reload(sys);sys.setdefaultencoding("utf8")
import socket,time
import requests
import json
import commands
import datetime


def readFile(filename):
    with open(filename,"r") as o:
        data = o.read()
    return data

def send(msg):
    message = {"phone":"haijun.zhao","msg":msg}
    headers={"Content-Type":"application/json"}
    response = requests.post("http://172.31.xx.xx:24680/message/weixin",data = json.dumps(message),headers=headers)
    print response.text
    
data = readFile("ip.txt")
for dd in data.strip().split("\n"):
    ip,port = dd.split()
    try:
        sc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sc.settimeout(2)
        sc.connect((ip,int(port)))
    except:
        send("%s:%s 错误"%(ip,port))

    finally:
        sc.close()
   
