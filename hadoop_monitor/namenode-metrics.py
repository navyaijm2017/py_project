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
    json_body = [{"measurement": "namenode","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value": value}}]
    json_body1 = [{"measurement": "namenode","tags": {"key": key,"metric": metric, "endpoint": endpoint}, "time": timestamp,"fields": {"value_int": value}}]
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

def get_metrics(data,ip='127.0.0.1',port='50070'):
    url = "http://%s:%s/jmx" %(ip,port)
    res = get_send_url(url,data)
    res = json.loads(res)
    return res

def get_metrics_value(monit_key,jmxdict,metric,endpoint,timestamp,step,tags):
    p = []
    for key,vtype in monit_key:
        if key == 'PercentUsed':
            value = jmxdict.get(key)/100
            #value = round(jmxdict.get(key),0)
        elif key == 'TotalBlocks':
            value = int(jmxdict.get(key))
      #   elif key == 'TotalBlocks':
      #      key="TotalBlocksReplication"
      #          value=int(jmxdict.get(key)) * 3
        elif vtype == 'COUNTER':
            value = int(jmxdict.get(key)) * step
        elif key == 'MemHeapUsedPersent':
            try:
                value = float(jmxdict.get('MemHeapUsedM')) / int(jmxdict.get('MemHeapMaxM')) * 100
            except ZeroDivisionError:
                value = 0
        elif key == 'State':
            if jmxdict.get(key) == 'active':
                value = 1
            elif jmxdict.get(key) == 'standby':
                value = 1
            else:
                value = 0
        else:
            value = float(jmxdict.get(key))

        if key == 'TotalBlocks':
            print metric+"_"+ key+"{host=\""+endpoint+"\"}" +" " ,value
            print metric+"_"+ key+"Replication{host=\""+endpoint+"\"}" +" " ,value*3
            write_inf(key,metric,endpoint,timestamp,value)
            write_inf("TotalBlocksReplication",metric,endpoint,timestamp,value*3)
        else:
            print metric+"_"+ key+"{host=\""+endpoint+"\"}" +" " ,value
            write_inf(key,metric,endpoint,timestamp,value)
            
def namenode_Metrics(metric,endpoint,timestamp,step,tags):
    p = []
    jvm_qry = "Hadoop:service=NameNode,name=JvmMetrics"
    monit_jvm_key = [
        ('MemHeapCommittedM','GAUGE'),
        ('MemHeapMaxM','GAUGE'),
        ('MemHeapUsedPersent','GAUGE'),
        ('GcCount','COUNTER'),
        ('GcTimeMillis','COUNTER'),
        ('LogError','COUNTER'),
        ('LogWarn','COUNTER'),
    ]
    status_qry = "Hadoop:service=NameNode,name=NameNodeStatus"
    monit_status_key = [
        ('State','GAUGE'),
    ]
    info_qry = "Hadoop:service=NameNode,name=NameNodeActivity"
    monit_info_key = [
        ('CreateFileOps','COUNTER'),
        ('DeleteFileOps','COUNTER'),
        ('FileInfoOps','COUNTER'),
        ('SyncsNumOps','COUNTER'),
        ('TransactionsNumOps','COUNTER'),
    ]
    dfs_qry = "Hadoop:service=NameNode,name=NameNodeInfo"
    monit_dfs_key = [
        ('PercentUsed','GAUGE'),
        ('TotalBlocks','GAUGE'),
        ('TotalFiles','GAUGE'),
        ('Total','GAUGE'),
        ('Used','GAUGE'),
        ('Free','GAUGE'),
        ('NumberOfMissingBlocks','GAUGE'),
    ]

    dfs_state_qry = "Hadoop:service=NameNode,name=FSNamesystemState"
    dfs_state_key = [
        ('NumLiveDataNodes','GAUGE'),
        ('NumDeadDataNodes','GAUGE'),
        ('NumStaleDataNodes','GAUGE'),
    ]

    dfs_rpc_qry = "Hadoop:service=NameNode,name=RpcActivityForPort9000"
    dfs_rpc_key = [
        ('RpcQueueTimeNumOps','GAUGE'),
        ('RpcProcessingTimeNumOps','GAUGE'),
        ('RpcProcessingTimeAvgTime','GAUGE'),
        ('RpcAuthenticationFailures','GAUGE'),
        ('RpcAuthenticationSuccesses','GAUGE'),
        ('RpcAuthorizationFailures','GAUGE'),
        ('RpcQueueTimeAvgTime','GAUGE'),
        ('RpcAuthorizationSuccesses','GAUGE'),
        ('RpcClientBackoff','GAUGE'),
        ('RpcSlowCalls','GAUGE'),
        ('CallQueueLength','GAUGE'),
    ]

    monitor = [
        (jvm_qry,monit_jvm_key),
        (info_qry,monit_info_key),
        (status_qry,monit_status_key),
        (dfs_qry,monit_dfs_key),
        (dfs_state_qry,dfs_state_key),
        (dfs_rpc_qry,dfs_rpc_key),
    ]

    for qry,monit_key in monitor:
        data = {}
        data['qry'] = qry
        jmxs = get_metrics(data,endpoint)
        jmxdict = {}
        if jmxs['beans']:
            jmxdict = jmxs['beans'][0]
            get_metrics_value(monit_key,jmxdict,metric,endpoint,timestamp,step,tags)


def get_all_ip():
    file_object = open('namenode')
    try:
        all_the_text = file_object.read().split('\n')
        return  all_the_text
    finally:
        file_object.close()

def main(ip):
    metric = "namenode"
    endpoint = ip
    timestamp = ((datetime.datetime.now()-datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"))
    #timestamp = int(time.time())
    step = 60
    tags = ''
    namenode_Metrics(metric,endpoint,timestamp,step,tags)

if __name__ == "__main__":
    for ip  in get_all_ip()[:-1]:
        main(ip)
