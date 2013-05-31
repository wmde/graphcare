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
import MySQLdb
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
        self.graphservWorkDir= '/mnt/user-store/jkroll/graphserv-instance/'
        self.graphservExecutable= '../graphserv/graphserv.dbg'
        self.graphcoreExecutable= '../graphserv/graphcore/graphcore'
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
                args= args + shlex.split('nohup screen -dm -S mytestsession bash -c "mkdir -p %(graphservWorkDir)s && cd %(graphservWorkDir)s && \
%(graphservExecutable)s -t %(graphservPort)s -H %(graphservHttpPort)s -l eia -c %(graphcoreExecutable)s 2>&1 \
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


def GetSQLServerForDB(wiki):    # wiki is dbname without '_p' suffix
    return '%s.labsdb' % wiki   # only on labs!
    

def ReloadGraph(conn, instance, namespaces= '*'):
    #~ timestamp=$(date --rfc-3339=seconds|sed -e 's/ /_/g')
    #~ feedertimestamp=$(date +%Y%m%d%H%M%S)
    d= datetime.datetime.utcnow()
    #~ timestamp= d.isoformat('_')
    timestamp= d.strftime("%Y-%m-%dT%H:%M:%S")
    feedertimestamp= d.strftime('%Y%m%d%H%M%S')
    log ('timestamp: %s, feedertimestamp: %s' % (timestamp, feedertimestamp))
    
    query= """SELECT /* SLOW_OK */ B.page_id, cl_from FROM categorylinks 
JOIN page AS B 
ON B.page_title = cl_to 
AND B.page_namespace = 14 
AND B.page_id!=0 
AND cl_from!=0"""
    if namespaces!='*':
        query+= ('\nAND (')
        namespaces= list(namespaces)
        for i in range(len(namespaces)): namespaces[i]= 'page_namespace=%s' % namespaces[i]
        query+= (' OR '.join(namespaces))
        query+= ')'
    log('%s: %s' % (instance, query))

    #~ conn= MySQLdb.connect(read_default_file=os.path.expanduser('~')+'/.my.cnf', host=GetSQLServerForDB(instance))
    #~ cur= conn.cursor()
    #~ cur.execute('USE %s_p' % instance)
    #~ cur.execute(query)
    #~ with open('/tmp/foo', 'w') as outfile:
        #~ while True:
            #~ row= cur.fetchone()
            #~ if not row: break
            #~ outfile.write('%d, %d\n' % (row[0], row[1]))
    
    # get arcs from sql
    tmpnam= '/tmp/foo'  #xxx change
    import _mysql
    db= _mysql.connect(read_default_file=os.path.expanduser('~')+'/.my.cnf', host=GetSQLServerForDB(instance), db=instance+'_p')
    db.query(query)
    result= db.use_result()
    with open(tmpnam, 'w') as outfile:
        while True:
            row= result.fetch_row()
            if not row: break
            outfile.write('%d, %d\n' % (int(row[0][0]), int(row[0][1])))
    db.close()

    # create/use graph
    try:
        conn.use_graph(instance)
    except Exception as ex:
        log(str(ex))
        conn.create_graph(instance)
        conn.use_graph(instance)

    conn.allowPipes= True
    
    log("sending arcs to graphcore")
    
    # load arcs from temp file into graphcore
    conn.execute('add-arcs < %s' % tmpnam)
    
    log("setting meta variables")
    
    # make stuff compatible with gpfeeder 
    conn.execute("set-meta last_full_import %s" % timestamp)
    conn.execute("set-meta gpfeeder_graph_type with-leafs")
    conn.execute("set-meta gpfeeder_status polling")
    conn.execute("set-meta gpfeeder_timestamp %s" % feedertimestamp)
    conn.execute("set-meta gpfeeder_namespaces %s" % ('*' if namespaces=='*' else (','.join(namespaces))))
    conn.execute("set-meta gpfeeder_dels_offset 0")
    conn.execute("set-meta gpfeeder_dels_state up_to_date")
    conn.execute("set-meta gpfeeder_dels_until %s" % feedertimestamp)
    conn.execute("set-meta gpfeeder_mods_offset 0")
    conn.execute("set-meta gpfeeder_mods_state up_to_date")
    conn.execute("set-meta gpfeeder_mods_until %s" % feedertimestamp)
    
    log("imported %s." % instance)
    

def CheckGraphcores(servconfig, instanceconfig):
    conn= client.Connection(client.ClientTransport(servconfig.remoteHost, int(servconfig.graphservPort)))
    conn.connect()
    
    conn.authorize('password', '%s:%s' % (str(servconfig.graphservUser), str(servconfig.graphservPassword)))
    
    for i in instanceconfig:
        log("checking %s with refresh interval %s hours" % (i.name, i.refreshIntervalHours))
        needsReload= True
        try:
            conn.use_graph(str(i.name))
            conn.get_meta('last_full_import')
            timestampString= conn.statusMessage.strip()
            timestamp= datetime.datetime(*time.strptime(timestampString, "%Y-%m-%dT%H:%M:%S")[0:6]) #+00:00"
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
        except Exception as ex:
            log(str(ex))
            log("continuing...")
            
        if needsReload:
            try:
                conn.protocol_version()
            except client.gpException as ex:
                log('exception caught before trying to reload. server down/graphserv crashed?')
                raise
            log('reloading graph %s...' % str(i.name))
            if False:
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
            else:
                ReloadGraph(conn, str(i.name))
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
