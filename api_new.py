#!/usr/bin/env python
#-*-coding:utf8-*-
#导入需要的模块
from flask import request
import urlparse
import commands
import datetime
from flask import Flask,jsonify
import MySQLdb,time,urllib
import json
import logging
#定义时间格式
timenow = str(datetime.datetime.now())
#定义flask的一个name
app = Flask(__name__)
#定义路由的访问路径和访问方式，其中，‘/autoscal’表示访问的路径，比如：http://ip:port/autoscal?aaa=111&bb=222
@app.route("/autoscal",methods=['POST','GET'])
#定义一个函数，调用这个函数，传入两个参数，实现autoscaling组的扩容和缩容
def instances_autoscal():
    #定义参数左边部分‘instances’和‘group_name’用于函数内部调用，右边部分‘request.args.get’+括号是flask传参数的固定格式，括号内的内容是外面调用api传的参数，上面的aaa=111的aaa就相当于这里的instances，bb=222的bb就相当于这里的group_name，111和222就是请求的时候传入的参数，多个参数传入用‘&’分割
    instances = request.args.get('instances')
    group_name = request.args.get('group_name')
    app.logger.warning('warning log')
    mylist = ['HadoopYarnNodeAutoScal','HadoopYarnNodeAutoScalOffline']
    if group_name not in mylist:
        return "请输入正确的组名"
    if int(instances) >20:
        return '扩容实例数不能大于20'
    else:
        app.logger.info('info log')
        commands.getoutput('aws autoscaling set-desired-capacity --auto-scaling-group-name %s --desired-capacity %d' % (group_name,int(instances)))
        with open ("/tmp/add.log",'a+') as log:
            log.write(timenow+'\n')
            log.write('扩容服务器数为:'+str(instances)+'\n')
        return '机器服务器数为:'+str(int(instances)+3)


@app.route("/get_instances",methods=['GET'])
def get_instances_count():
    app.logger.info('info log')
    tag_name = request.args.get('tag_name')
    result = commands.getoutput('aws ec2 describe-instances --filters   "Name=instance-state-code,Values=16" "Name=tag:Name,Values=%s"' % (tag_name))
    mylist = []
    for res in json.loads(result).get("Reservations"):
        for instance in res.get("Instances"):
            mylist.append(instance.get("PrivateIpAddress"))
    data =  '当前集群服务器个数为:'+str(len(mylist)+3)
    return  data


@app.route("/get_ip",methods=['GET'])
def get_ip():
    app.logger.info('info log')
    tag_name = request.args.get('tag_name')
    mylist=[]
    result = commands.getoutput('aws ec2 describe-instances --filters   "Name=instance-state-code,Values=16" "Name=tag:Name,Values=%s"' % (tag_name))
    for res in json.loads(result).get("Reservations"):
        for instance in res.get("Instances"):
            mylist.append(instance.get("PrivateIpAddress"))
    data =  mylist
    return jsonify(mylist)

if __name__ == "__main__":
    app.debug = True
    handler = logging.FileHandler('/data/logs/flask.log', encoding='UTF-8')
    handler.setLevel(logging.DEBUG)
    logging_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
    handler.setFormatter(logging_format)
    app.logger.addHandler(handler)
    app.run(host='0.0.0.0',port=8001,threaded=True)
