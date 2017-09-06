#!/bin/env python
#-*- coding:utf-8 -*-
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
    json_body = [{"measurement": "nodemanager","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value": value}}]
    json_body1 = [{"measurement": "nodemanager","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value_int": value}}]
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

def get_metrics(data,ip='127.0.0.1',port='23999'):
    url = "http://%s:%s/jmx" %(ip,port)
    try:
        res = get_send_url(url,data)
        res = json.loads(res)
        return res
    except:
        pass

def get_metrics_value(monit_key,jmxdict,metric,endpoint,timestamp,step,tags):
    p = []
    for key,vtype in monit_key:
        if vtype == 'COUNTER':
            value = int(jmxdict.get(key)) * step
        elif key == 'MemHeapUsedPercent':
            try:
                value = float(jmxdict.get('MemHeapUsedM')) / int(jmxdict.get('MemHeapMaxM')) * 100
            except ZeroDivisionError:
                value = 0
        elif key == 'AllMemGB':
            value = int(jmxdict.get('AllocatedGB')) + int(jmxdict.get('AvailableGB'))
        elif key == 'AllVCores':
            value = int(jmxdict.get('AllocatedVCores')) + int(jmxdict.get('AvailableVCores'))
        else:
            value = int(jmxdict.get(key))
        print metric+"-"+ key+"{host=\""+endpoint+"\"}" +" " ,value
        write_inf(key,metric,endpoint,timestamp,value)


def nodemanager_Metrics(metric,endpoint,timestamp,step,tags):
    p = []
    jvm_qry = "Hadoop:service=NodeManager,name=JvmMetrics"
    monit_jvm_key = [
        ('MemHeapCommittedM','GAUGE'),
        ('MemHeapMaxM','GAUGE'),
        ('MemHeapUsedPercent','GAUGE'),
        ('GcCount','COUNTER'),
        ('GcTimeMillis','COUNTER'),
        ('LogError','COUNTER'),
        ('LogWarn','COUNTER'),
    ]
    shuffle_qry = "Hadoop:service=NodeManager,name=ShuffleMetrics"
    monit_shuff_key = [
        ('ShuffleOutputBytes','COUNTER'),
        ('ShuffleOutputsFailed','COUNTER'),
        ('ShuffleOutputsOK','COUNTER'),
        ('ShuffleConnections','COUNTER'),
    ]
    contain_qry = "Hadoop:service=NodeManager,name=NodeManagerMetrics"
    monit_contain_key = [
        ('ContainersFailed','COUNTER'),
        ('ContainersKilled','COUNTER'),
        ('AllocatedGB','GAUGE'),
        ('AvailableVCores','GAUGE'),
        ('AllMemGB','GAUGE'),
        ('AllocatedContainers','GAUGE'),
        ('AllocatedVCores','GAUGE'),
        ('AvailableGB','GAUGE'),
        ('AllVCores','GAUGE'),
    ]
    monitor = [
        (jvm_qry,monit_jvm_key),
        (shuffle_qry,monit_shuff_key),
        (contain_qry,monit_contain_key),
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
    file_object = open('nodemanager')
    try:
        all_the_text = file_object.read().split('\n')
        return  all_the_text
    finally:
        file_object.close()

def main(ip):
    metric = "nodemanager"
    endpoint = ip
    timestamp = ((datetime.datetime.now()-datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"))
    step = 60
    tags = ''
    nodemanager_Metrics(metric,endpoint,timestamp,step,tags)

if __name__ == "__main__":
    for ip  in get_all_ip()[:-1]:
        main(ip)
