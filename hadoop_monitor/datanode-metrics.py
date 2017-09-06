#!/bin/env python
#-*- coding:utf-8 -*-
import urllib
import urllib2
import socket
import time
import json
import re
from influxdb import InfluxDBClient
import datetime
#InfluxDB client
client = InfluxDBClient('10.10.10.10', 8086, 'root', 'root', 'jmxDB', timeout=2, retries=3)

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

def get_metrics(data,ip='127.0.0.1',port='50075'):
    url = "http://%s:%s/jmx" %(ip,port)
    try:
        res = get_send_url(url,data)
        res = json.loads(res)
        return res
    except:
        pass
def write_inf(key,metric,endpoint,timestamp,value):
    json_body = [{"measurement": "datanode","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value": value}}]
    json_body1 = [{"measurement": "datanode","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value_int": value}}]
    try:
        if isinstance(value,float):
            client.write_points(json_body)
        else:
            client.write_points(json_body1)
    except Exception as e:
        print e
    #return json_body

def get_metrics_value(monit_key,jmxdict,metric,endpoint,timestamp,step,tags):
    for key,vtype in monit_key:
        if vtype == 'COUNTER':
            value = int(jmxdict.get(key)) * step
        elif key == 'MemHeapUsedPercent':
            try:
                value = float(jmxdict.get('MemHeapUsedM')) / int(jmxdict.get('MemHeapMaxM')) * 100
            except ZeroDivisionError:
                value = 0
        else:
            value = int(jmxdict.get(key))

        print metric+"_"+ key+"{host=\""+endpoint+"\"}" +" " ,value
        write_inf(key,metric,endpoint,timestamp,value)
        #json_body = [{"measurement": "hadoop","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value": value}}]
        #print json_body
def datanode_metrics(metric,endpoint,timestamp,step,tags):
    p = []
    jvm = "Hadoop:service=DataNode,name=JvmMetrics"
    monit_jvm_key = [
        ('MemHeapCommittedM','GAUGE'),
        ('MemHeapMaxM','GAUGE'),
        ('MemHeapUsedPercent','GAUGE'),
        ('GcCount','COUNTER'),
        ('GcTimeMillis','COUNTER'),
        ('LogError','COUNTER'),
        ('LogWarn','COUNTER'),
    ]
    datanodeinfo = "Hadoop:service=DataNode,name=DataNodeActivity-" + endpoint + "-50010"
    monit_datanodeinfo_key = [
        ('BytesWritten','COUNTER'),
        ('BytesRead','COUNTER'),
        ('BlocksWritten','COUNTER'),
        ('BlocksRead','COUNTER'),
        ('BlocksReplicated','COUNTER'),
        ('BlocksRemoved','COUNTER'),
        ('ReadsFromLocalClient','COUNTER'),
        ('ReadsFromRemoteClient','COUNTER'),
        ('WritesFromLocalClient','COUNTER'),
        ('WritesFromRemoteClient','COUNTER'),
        ('DatanodeNetworkErrors','COUNTER'),
    ]
    monitor = [
        (datanodeinfo,monit_datanodeinfo_key),
        (jvm,monit_jvm_key)
    ]

    for qry,monit_key in monitor:
        data = {}
        data['qry'] = qry
        jmxs = get_metrics(data,endpoint)
        jmxdict = {}
        try:
            if jmxs['beans']:
                jmxdict = jmxs['beans'][0]
                get_metrics_value(monit_key,jmxdict,metric,endpoint,timestamp,step,tags)
        except:
            pass

def get_all_ip():
    file_object = open('datanode')
    try:
        all_the_text = file_object.read().split('\n')
        return  all_the_text
    finally:
        file_object.close()

def main(ip):
    metric = "datanode"
    endpoint = ip
    timestamp = ((datetime.datetime.now()-datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"))
    #timestamp = int(time.time())
    step = 60
    tags = ''
    datanode_metrics(metric,endpoint,timestamp,step,tags)

if __name__ == "__main__":
    for ip  in get_all_ip()[:-1]:
        main(ip)

