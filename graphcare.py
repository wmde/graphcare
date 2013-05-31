#!/usr/bin/python
#$ -l h_rt=0:30:00 
#$ -l virtual_free=100M 
#$ -l arch=sol 
#$ -l fs-user-store=1
#$ -t 1
#$ -N graphcare 
#$ -m ae
#$ -wd /home/jkroll/graphcare/logs
#$ -b y
# -*- coding:utf-8 -*-
import os
import sys
import argparse
import json
import subprocess
import time
import datetime
import shlex
from gp import *

def MakeLogTimestamp(unixtime= None):
    if unixtime==None: unixtime= time.time()
    return time.strftime("%Y%m%d %H:%M.%S", time.gmtime(unixtime))

def log(s):
    print("[%s] %s" % (MakeLogTimestamp(), str(s).rstrip('\n')))

class GraphservConfig:
    def __init__(self, file):
        """ example config file contents:
        { 
            "remoteHost": "foo.bar.org",
            "graphservPort": "6666", 
            "graphservHttpPort": "8090",
            "graphservUser": "donalfonso",
            "graphservPassword": "secret",
            "sshUser": "donalfonso"
        }
        """
        self.remoteHost= 'ortelius.toolserver.org'
        self.graphservPort= '6666'
        self.graphservHttpPort= '8090'
        self.graphservUser= ''
        self.graphservPassword= ''
        self.sshUser= ''
        self.loadJson(file)
    
    def loadJson(self, file):
        values= json.load(file)
        for k in values:
            self.__dict__[k]= str(values[k])

class GraphcoreInstanceConfig(list):
    class Entry:
        pass
    def __init__(self, file):
        """ example config file contents:
        [ 
            {"name": "dewiki", "refreshIntervalHours": "1.0"},
            {"name": "frwiki", "refreshIntervalHours": "2.0"},
            {"name": "enwiki", "refreshIntervalHours": "3.5"}
        ]
        """
        v= json.load(file)
        for i in v:
            entry= GraphcoreInstanceConfig.Entry()
            #~ entry.__dict__= i
            for k in i:
                entry.__dict__[str(k)]= str(i[k])
            self.append(entry)

def CheckGraphserv(servconfig):
    for i in range(0, 3):
        try:
            conn= client.Connection(client.ClientTransport(servconfig.remoteHost, int(servconfig.graphservPort)))
            conn.connect()
            log("graphserv is running, protocol version is %s." % conn.getProtocolVersion())
            conn.close()
            return
        except client.gpException as ex:
            log(str(ex))
            log("trying to restart graphserv on %(remoteHost)s" % servconfig.__dict__)
            try:
                if servconfig.remoteHost=='localhost':
                    args= []
                else:
                    args= ['ssh', '-f', '%(sshUser)s@%(remoteHost)s' % servconfig.__dict__ ]
                args= args + shlex.split('nohup screen -dm -S mytestsession bash -c "cd /mnt/user-store/jkroll/graphserv-instance/ && \
../graphserv/graphserv.dbg -t %(graphservPort)s -H %(graphservHttpPort)s -l eia -c ../graphserv/graphcore/graphcore \
| tee graphserv-$(date +%%F_%%T).log"' % servconfig.__dict__)
                log(args)
                p= subprocess.Popen(args,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
                while p.returncode==None:
                    line= p.stdout.readline()
                    if len(line): log("\t%s" % line)
                    else: time.sleep(0.2)
                    sys.stdout.flush()
                    p.poll()
                if p.returncode!=0:
                    log("child error code was %s" % p.returncode)
            except subprocess.CalledProcessError as ex:
                print "Exception: %s\n" % str(ex)
                print str(ex.output)
        time.sleep(5)
    raise Exception("tried restarting graphserv for 3 times, giving up.") 

def CheckGraphcores(servconfig, instanceconfig):
    conn= client.Connection(client.ClientTransport(servconfig.remoteHost, int(servconfig.graphservPort)))
    conn.connect()
    
    #~ conn.authorize('password', '%s:%s' % (str(servconfig.graphservUser), str(servconfig.graphservPassword)))
    
    for i in instanceconfig:
        log("checking %s with refresh interval %s hours" % (i.name, i.refreshIntervalHours))
        needsReload= True
        try:
            conn.use_graph(str(i.name))
            conn.get_meta('last_full_import')
            timestampString= conn.statusMessage.strip().replace('_', ' ')
            timestamp= datetime.datetime(*time.strptime(timestampString, "%Y-%m-%d %H:%M:%S+00:00")[0:6])
            now= datetime.datetime.utcnow()
            delta= now - timestamp
            log('last full import was at %s, %.2f hours ago' % (timestampString, delta.total_seconds()/60.0/60.0))
            if delta > datetime.timedelta(hours=float(i.refreshIntervalHours)):
                log('graph needs update')
            else:
                log('graph is current')
                needsReload= False
        except client.gpException as ex:
            log(str(ex))
            
        if needsReload:
            try:
                conn.protocol_version()
            except client.gpException as ex:
                log('exception caught before trying to reload. server down/graphserv crashed?')
                raise
            log('reloading graph %s' % str(i.name))
            # TODO: write something better than the dreaded readwiki.sh hack, and do some error checking
            if servconfig.remoteHost=='localhost':
                args= []
            else:
                args= ['ssh', '%(sshUser)s@%(remoteHost)s' % servconfig.__dict__ ]
            args= 'GRAPHSERV_HOST=%(remoteHost)s GRAPHSERV_PORT=%(graphservPort)s /mnt/user-store/jkroll/graphserv-instance/readwiki.sh ' % servconfig.__dict__
            args= args + i.name
            log(args)
            p= subprocess.Popen(args,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0, shell=True)
            while p.returncode==None:
                line= p.stdout.readline()
                if len(line): log("\t%s" % line)
                else: time.sleep(0.2)
                p.poll()
            if p.returncode!=0:
                log("child error code was %s" % p.returncode)
            else:
                log("imported %s." % str(i.name))
    conn.close()



if __name__ == '__main__':
    parser= argparse.ArgumentParser(description= 'Catgraph Maintenance Job Script.')
    parser.add_argument('-s', '--server-config', default='~/.graphcare-serverconfig.json', help='server config file. ' + GraphservConfig.__init__.__doc__)
    parser.add_argument('-i', '--instance-config', default='~/.graphcare-instanceconfig.json', help='instance config file. ' + GraphcoreInstanceConfig.__init__.__doc__)
    
    args= parser.parse_args()
    
    gc= GraphservConfig(open(os.path.expanduser(args.server_config)))
    
    instances= GraphcoreInstanceConfig(open(os.path.expanduser(args.instance_config)))

    CheckGraphserv(gc)
    CheckGraphcores(gc, instances)
    
    sys.exit(0)
