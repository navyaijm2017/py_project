#!/usr/bin/env python
# encoding: utf-8

import urllib
import urllib2
import socket
import time
import json


import datetime

from influxdb import InfluxDBClient
#InfluxDB client
client = InfluxDBClient('10.10.10.10', 8086, 'root', 'root', 'jmxDB', timeout=2, retries=3)


def write_inf(key,metric,endpoint,timestamp,value):
    json_body = [{"measurement": "resourcemanager","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value": value}}]
    json_body1 = [{"measurement": "resourcemanager","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value_int": value}}]
    try:
        if isinstance(value,float):
            client.write_points(json_body)
        else:
            client.write_points(json_body1)
    except Exception as e:
        print e

def get_host_info():
    hostname = socket.gethostname()
    #ip = socket.gethostbyname(hostname)
    return hostname

def get_send_url(url, data, headers={}):
    try:
        ret_data = None
        url = url + '?' + urllib.urlencode(data)
        req = urllib2.Request(url, headers=headers)
        res = urllib2.urlopen(req, timeout=300)
        ret_data = res.read()
        res.close()
    except Exception as ex:
        print ex
    finally:
        return ret_data

def get_metrics(data,ip='127.0.0.1',port='8088'):
    url = "http://%s:%s/jmx" %(ip,port)
    res = get_send_url(url,data)
    res = json.loads(res)
    return res

def get_metrics_value(monit_key,jmxdict,metric,endpoint,timestamp,step,tags,qry):
    p = []
    for key,vtype in monit_key:
        if key == 'LiveNodeManagers':
            value = jmxdict.get(key).count('HostName')


        if qry == "Hadoop:service=ResourceManager,name=RMNMInfo":
            print metric+"_"+key+"{host=\""+endpoint+"\"}" +" " ,value
            write_inf(key,metric,endpoint,timestamp,value)
def resourcemanager_Metrics(metric,endpoint,timestamp,step,tags):
    p = []

    info_json       =  "Hadoop:service=ResourceManager,name=RMNMInfo"

    monit_default_key = [
       ('LiveNodeManagers','GAUGE')
    ]
    monitor = [
        (info_json,monit_default_key)
    ]

    for qry,monit_key in monitor:
        data = {}
        data['qry'] = qry
        tags = qry.split(',')[-1]
        jmxs = get_metrics(data,endpoint)
        jmxdict = {}
        try:
            if jmxs['beans']:
                jmxdict = jmxs['beans'][0]
                get_metrics_value(monit_key,jmxdict,metric,endpoint,timestamp,step,tags,qry)
        except:
            pass

def get_all_ip():
    file_object = open('resourcemanager')
    try:
        all_the_text = file_object.read().split('\n')
        return  all_the_text
    finally:
        file_object.close()

def main(ip):
    metric = "resourcemanager"
    endpoint = ip
    timestamp = ((datetime.datetime.now()-datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"))
    step = 60
    tags = ''
    resourcemanager_Metrics(metric,endpoint,timestamp,step,tags)

if __name__ == "__main__":
        main("ip-10-10-103-15.ec2.internal")
