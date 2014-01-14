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
import socket
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
        self.graphservExecutable= '$HOME/graphserv/graphserv.dbg'
        self.graphcoreExecutable= '$HOME/graphserv/graphcore/graphcore'
        self.sshUser= ''
        self.loadJson(file)
        self.graphservWorkDir= os.path.expanduser(os.path.expandvars(self.graphservWorkDir))
        self.graphservExecutable= os.path.expanduser(os.path.expandvars(self.graphservExecutable))
        self.graphcoreExecutable= os.path.expanduser(os.path.expandvars(self.graphcoreExecutable))
        thishost= socket.gethostname()
        self.graphservWorkDir= os.path.join(self.graphservWorkDir, thishost)
    
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
            {"name": "enwiki.categoriesonly", "refreshIntervalHours": "3.5", "namespaces": [14] }
        ]
        """
        v= json.load(file)
        for i in v:
            entry= GraphcoreInstanceConfig.Entry()
            #~ entry.__dict__= i
            for k in i:
                entry.__dict__[str(k)]= i[k]
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
                args= args + shlex.split('nohup screen -dm -S graphserv bash -c "mkdir -p %(graphservWorkDir)s && cd %(graphservWorkDir)s && \
%(graphservExecutable)s -t %(graphservPort)s -H %(graphservHttpPort)s -l eia -c %(graphcoreExecutable)s -p ../gspasswd.conf -g ../gsgroups.conf 2>&1 \
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
    dbname= instance.split('_')[0]
    
    #~ query= """SELECT /* SLOW_OK */ B.page_id, cl_from FROM categorylinks 
#~ JOIN page AS B 
#~ ON B.page_title = cl_to 
#~ AND B.page_namespace = 14 
#~ AND B.page_id!=0 
#~ AND cl_from!=0"""
    query= """select /* SLOW_OK */ B.page_id, N.page_id 
from page as N
join categorylinks on cl_from = N.page_id
join page as B 
on B.page_title = cl_to 
and B.page_namespace = 14"""
# where N.page_namespace = 14;"""
    if namespaces!='*':
        query+= ('\nWHERE (')
        namespaces= list(namespaces)
        nslist= []
        for i in range(len(namespaces)): nslist.append('N.page_namespace=%s' % namespaces[i])
        query+= (' OR '.join(nslist))
        query+= ')'
    log('%s: running sql import query, namespaces=%s' % (instance, str(namespaces)))
    log(query)

    # get arcs from sql
    tmpnam= '/tmp/foo'  #xxx change
    import _mysql
    db= _mysql.connect(read_default_file=os.path.expanduser('~')+'/.my.cnf', host=GetSQLServerForDB(dbname), db=dbname+'_p')
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
        log("creating graph: %s" % instance)
        conn.create_graph(instance)
        conn.use_graph(instance)

    conn.allowPipes= True
    
    log("clearing graphcore")
    conn.execute("clear");
    
    log("sending arcs to graphcore")
    
    # load arcs from temp file into graphcore
    conn.execute('add-arcs < %s' % tmpnam)
    
    log("setting meta variables")
    
    # make stuff compatible with gpfeeder 
    conn.execute("set-meta last_full_import %s" % timestamp)
    conn.execute("set-meta gpfeeder_graph_type with-leafs")
    conn.execute("set-meta gpfeeder_status polling")
    conn.execute("set-meta gpfeeder_timestamp %s" % feedertimestamp)
    conn.execute("set-meta gpfeeder_namespaces %s" % ('*' if namespaces=='*' else ('_'.join([str(i) for i in namespaces]))))
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
                ReloadGraph(conn, 
                    i.db if 'db' in i.__dict__ else str(i.name), 
                    i.namespaces if 'namespaces' in i.__dict__ else '*')
    conn.close()

def DumpAllGraphs(servconfig):
    conn= client.Connection(client.ClientTransport(servconfig.remoteHost, int(servconfig.graphservPort)))
    conn.strictArguments= False
    conn.connect()
    conn.authorize('password', '%s:%s' % (str(servconfig.graphservUser), str(servconfig.graphservPassword)))
    
    dumpdir= os.path.join(servconfig.graphservWorkDir, 'dumps')
    if not os.path.isdir(dumpdir):
        os.mkdir(dumpdir)
        log('created %s.' % dumpdir)
    
    graphs= conn.capture_list_graphs()
    for line in graphs:
        graph= line[0]
        dest= os.path.join(dumpdir, graph+'.dump')
        log('dumping %s to %s.' % (graph, dest))
        conn.use_graph(graph)
        conn.dump_graph(dest)

    conn.close()
    log('done.')
    

def LoadAllGraphs(servconfig):
    conn= client.Connection(client.ClientTransport(servconfig.remoteHost, int(servconfig.graphservPort)))
    conn.strictArguments= False
    conn.connect()
    conn.authorize('password', '%s:%s' % (str(servconfig.graphservUser), str(servconfig.graphservPassword)))
    
    dumpdir= os.path.join(servconfig.graphservWorkDir, 'dumps')
    for f in os.listdir(dumpdir):
        graphname= os.path.splitext(f)[0]
        try:
            conn.use_graph(graphname)
            log('clearing existing graph %s.' % graphname)
            conn.clear()
        except client.gpClientException:
            log('creating graph %s.' % graphname)
            conn.create_graph(graphname)
            conn.use_graph(graphname)
        filename= os.path.join(dumpdir, f)
        log('loading graph %s from %s.' % (graphname, filename))
        conn.load_graph(filename)

    conn.close()
    log('done.')
    

if __name__ == '__main__':
    parser= argparse.ArgumentParser(description= 'Catgraph Maintenance Job Script.', formatter_class= argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-s', '--server-config', default='~/.graphcare-serverconfig.json', help='server config file. ' + GraphservConfig.__init__.__doc__)
    parser.add_argument('-i', '--instance-config', default='~/.graphcare-instanceconfig.json', help='instance config file. ' + GraphcoreInstanceConfig.__init__.__doc__)
    parser.add_argument('-a', '--action', default='update', choices=['update', 'dump-all-graphs', 'load-all-graphs'], 
        help='action to run. \n* update: start graphserv if necessary, update graphs (default)\n * dump-all-graphs: save all running graphs to $graphservWorkDir/dumps.\n * load-all-graphs: load all graphs from $graphservWorkDir/dumps.')
    
    args= parser.parse_args()
    
    gc= GraphservConfig(open(os.path.expanduser(args.server_config)))
    
    instances= GraphcoreInstanceConfig(open(os.path.expanduser(args.instance_config)))

    if args.action=='update':
        CheckGraphserv(gc)
        CheckGraphcores(gc, instances)
    elif args.action=='dump-all-graphs':
        DumpAllGraphs(gc)
    elif args.action=='load-all-graphs':
        CheckGraphserv(gc)
        LoadAllGraphs(gc)
    
    sys.exit(0)
