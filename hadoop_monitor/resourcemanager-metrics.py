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
        if vtype == 'COUNTER':
            value = int(jmxdict.get(key)) * step
        elif key == 'MemHeapUsedPercent':
            try:
                value = float(jmxdict.get('MemHeapUsedM')) / int(jmxdict.get('MemHeapMaxM')) * 100
            except ZeroDivisionError:
                value = 0
        elif key == 'AllocatedMB':
            value = int(jmxdict.get(key)) * 1024 * 1024
        elif key == 'AvailableMB':
            value = int(jmxdict.get(key)) * 1024 * 1024
        elif key == 'AllMem':
            value = (int(jmxdict.get('AllocatedMB')) + int(jmxdict.get('AvailableMB'))) * 1024 * 1024
        elif key == 'AllVCores':
            value = int(jmxdict.get('AllocatedVCores')) + int(jmxdict.get('AvailableVCores'))
        else:
            value = int(jmxdict.get(key))

        if qry == "Hadoop:service=ResourceManager,name=JvmMetrics":
            print metric+"_"+key+"{host=\""+endpoint+"\"}" +" " ,value
            write_inf(key,metric,endpoint,timestamp,value)
        elif qry.find('q0='):
            if qry.find('q1='):
                key = qry.split('q0=')[-1].split('q1=')[-1]+"_"+key
                write_inf(key,metric,endpoint,timestamp,value)
                print metric+"_"+qry.split('q0=')[-1].split('q1=')[-1]+"_"+key+"{host=\""+endpoint+"\"}" +" " ,value
        else:
            write_inf(key,metric,endpoint,timestamp,value)
            print metric+"_"+key+"{host=\""+endpoint+"\"}" +" " ,value

def resourcemanager_Metrics(metric,endpoint,timestamp,step,tags):
    p = []
    jvm = "Hadoop:service=ResourceManager,name=JvmMetrics"
    monit_jvm_key = [
        ('MemHeapCommittedM','GAUGE'),
        ('MemHeapMaxM','GAUGE'),
        ('MemHeapUsedPercent','GAUGE'),
        ('GcCount','COUNTER'),
        ('GcTimeMillis','COUNTER'),
        ('LogError','COUNTER'),
        ('LogWarn','COUNTER'),
    ]

    root            =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root"
    nlp             =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=nlp"
    search_queue    =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=search_queue"
    offline         =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=offline"
    common_queue    =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=common_queue"
    eport_queue     =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=report_queue"
    realtime        =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=realtime"
    default         =  "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default"

    monit_default_key = [
       ('AppsSubmitted','GAUGE'),
       ('AppsKilled','GAUGE'),
       ('AppsFailed','GAUGE'),
       ('AppsRunning','GAUGE'),
       ('AppsPending','GAUGE'),
       ('AllocatedMB','GAUGE'),
       ('AvailableMB','GAUGE'),
       ('AllMem','GAUGE'),
       ('AllocatedVCores','GAUGE'),
       ('AvailableVCores','GAUGE'),
       ('AllVCores','GAUGE'),
       ('AllocatedContainers','GAUGE'),
       ('ActiveUsers','GAUGE'),
       ('ActiveApplications','GAUGE'),
    ]
    monitor = [
        (jvm,monit_jvm_key),
        (root,monit_default_key),
        (nlp,monit_default_key),
        (search_queue,monit_default_key),
        (offline,monit_default_key),
        (common_queue,monit_default_key),
        (eport_queue,monit_default_key),
        (realtime,monit_default_key),
        (default,monit_default_key),
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
    for ip  in get_all_ip()[:-1]:
        main(ip)
