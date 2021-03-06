#!/usr/bin/python
# -*- coding:utf-8 -*-
import os
import errno
import sys
import argparse
import json
import subprocess
import time
import datetime
import shlex
import MySQLdb
import socket
import re
import csv
from gp import *

myhostname= socket.gethostname()


def MakeLogTimestamp(unixtime= None):
    if unixtime==None: unixtime= time.time()
    return time.strftime("%Y%m%d %H:%M.%S", time.gmtime(unixtime))

def log(s):
    print("[%s] %s" % (MakeLogTimestamp(), str(s).rstrip('\n')))

class GraphservConfig:
    def __init__(self):
        """ example config file contents:
        { 
            "remoteHost": "foo.bar.org",
            "graphservPort": "6666", 
            "graphservHttpPort": "8090",
            "graphservUser": "donalfonso",
            "graphservPassword": "secret",
            "sshUser": "donalfonso"
            etc...
        }
        """
        self.remoteHost= 'ortelius.toolserver.org'
        self.graphservPort= '6666'
        self.graphservHttpPort= '8090'
        self.graphservUser= ''
        self.graphservPassword= ''
        self.graphservWorkDir= '/mnt/user-store/jkroll/graphserv-instance/'
        self.graphservExecutable= '$HOME/graphserv/graphserv.dbg'
        self.graphservParams= '-l eia'
        self.graphcoreExecutable= '$HOME/graphserv/graphcore/graphcore'
        self.hostmapPath= '$HOME/hostmap'
        self.sshUser= ''
    
    def load(self, filename):
        values= json.load(open(filename))
        for k in values:
            self.__dict__[k]= str(values[k])
        self.graphservWorkDir= os.path.expanduser(os.path.expandvars(self.graphservWorkDir))
        self.graphservExecutable= os.path.expanduser(os.path.expandvars(self.graphservExecutable))
        self.graphcoreExecutable= os.path.expanduser(os.path.expandvars(self.graphcoreExecutable))
        self.graphservWorkDir= os.path.join(self.graphservWorkDir, myhostname)
        self.hostmapPath= os.path.expanduser(os.path.expandvars(self.hostmapPath))
        return self

class GraphcoreInstanceConfig(list):
    class Entry:
        # could set defaults here
        pass
    def __init__(self, filename):
        """ example config file contents:
        [ 
            {"name": "dewiki", "refreshIntervalHours": "1.0"},
            {"name": "frwiki", "refreshIntervalHours": "2.0"},
            {"name": "enwiki.categoriesonly", "refreshIntervalHours": "3.5", "namespaces": [14] }
        ]
        """
        v= json.load(open(filename))
        for i in v:
            entry= GraphcoreInstanceConfig.Entry()
            #~ entry.__dict__= i
            for k in i:
                entry.__dict__[str(k)]= i[k]
            self.append(entry)

def CheckGraphserv(servconfig):
    starttime= time.time()
    maxwaittime= 15
    while time.time()-starttime < maxwaittime:
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
%(graphservExecutable)s -t %(graphservPort)s -H %(graphservHttpPort)s -c %(graphcoreExecutable)s -p ../gspasswd.conf -g ../gsgroups.conf %(graphservParams)s 2>&1 \
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
        time.sleep(.5)
    raise Exception("tried restarting graphserv for %d seconds, giving up." % maxwaittime) 


def GetSQLServerForDB(wiki):    # wiki is dbname with or without '_p' suffix
    if '_' in wiki: wiki= wiki.split('_')[0]
    return '%s.analytics.db.svc.eqiad.wmflabs' % wiki
    

def ReloadGraph(conn, instance, dbname, sqlhost, namespaces= '*'):
    #~ timestamp=$(date --rfc-3339=seconds|sed -e 's/ /_/g')
    #~ feedertimestamp=$(date +%Y%m%d%H%M%S)
    log("ReloadGraph: dbname: %s, sqlhost: %s" % (dbname, sqlhost)) 
    d= datetime.datetime.utcnow()
    #~ timestamp= d.isoformat('_')
    timestamp= d.strftime("%Y-%m-%dT%H:%M:%S")
    feedertimestamp= d.strftime('%Y%m%d%H%M%S')
    log ('timestamp: %s, feedertimestamp: %s' % (timestamp, feedertimestamp))
    query= """select /* SLOW_OK */ B.page_id, N.page_id 
from page as N
join categorylinks on cl_from = N.page_id
join page as B 
on B.page_title = cl_to 
and B.page_namespace = 14"""
    if namespaces!='*':
        query+= ('\nWHERE (')
        namespaces= list(namespaces)
        nslist= []
        for i in range(len(namespaces)): nslist.append('N.page_namespace=%s' % namespaces[i])
        query+= (' OR '.join(nslist))
        query+= ')'
    log('%s: running sql import query, namespaces=%s, database %s on sql host %s' % (instance, str(namespaces), dbname, sqlhost))
    log(query)

    import tempfile
    with tempfile.NamedTemporaryFile() as f:
        # get arcs from sql
        tmpnam= f.name
        import _mysql, _mysql_exceptions
        try:
            db= _mysql.connect(read_default_file=os.path.expanduser('~')+'/.my.cnf', host=sqlhost, db=dbname)
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

        except _mysql_exceptions.OperationalError as ex:
            if ex[0]==1049: #   "Unknown database"
                log("%s" % ex)
            else:
                raise
    

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
            instancename= str(i.name)
            db= i.db if 'db' in i.__dict__ else instancename.split('_')[0] + '_p'
            ReloadGraph(conn, 
                instance= instancename,
                dbname= db,
                sqlhost= servconfig.sqlHost if 'sqlHost' in servconfig.__dict__ else GetSQLServerForDB(db), 
                namespaces= i.namespaces if 'namespaces' in i.__dict__ else '*')
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
    if not os.path.isdir(dumpdir):
        log('no such directory %s.' % dumpdir)
    else:
        for f in os.listdir(dumpdir):
            graphname= os.path.splitext(f)[0]
            try:
                conn.use_graph(graphname)
                log('clearing existing graph %s.' % graphname)
                conn.clear()
            except client.gpProcessorException:
                log('creating graph %s.' % graphname)
                conn.create_graph(graphname)
                conn.use_graph(graphname)
            filename= os.path.join(dumpdir, f)
            log('loading graph %s from %s.' % (graphname, filename))
            conn.load_graph(filename)

    conn.close()
    log('done.')


#~ http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

# get wiki-to-host mapping as dict
def GetHostmap(servconfig):
    with open(os.path.expanduser(os.path.join(servconfig.hostmapPath, "graphs.json"))) as f:
        return json.load(f)

def RefreshHostmap(servconfig):
    mapdir= os.path.expanduser(servconfig.hostmapPath)
    mkdir_p(mapdir)
    conn= client.Connection(client.ClientTransport(servconfig.remoteHost, int(servconfig.graphservPort)))
    conn.strictArguments= False
    conn.connect()
    graphs= conn.capture_list_graphs()
    graphs= set( i[0] for i in graphs )
    graphtohost= { }
    for file in os.listdir(mapdir):
        fpath= os.path.join(mapdir, file)
        if (not os.path.isfile(fpath)) or fpath.endswith('.json'): continue
        host= open(fpath, 'r').read().strip()
        if file in graphs:
            # if this graph is running on any host, don't map it to this host.
            graphs.remove(file)
            graphtohost[file]= host
        elif host == myhostname:
            # if this graph is no longer running on this host, remove the mapping.
            log('removing graph %s from host map' % file)
            os.remove(os.path.join(mapdir, file))
        else:
            graphtohost[file]= host
    for graph in graphs:
        log('mapping graph %s to host %s' % (graph, myhostname))
        mapfilename= os.path.join(mapdir, graph)
        tmpname= mapfilename + '.%s.tmp' % myhostname   # assumes this script does not run in parallel on the same host!
        with open(tmpname, 'w') as f: f.write(myhostname)
        os.rename(tmpname, mapfilename)
        graphtohost[graph]= myhostname
    hostmapname= os.path.join(mapdir, 'graphs.json')
    tmpname= '%s.%s.tmp' % (hostmapname, myhostname)
    with open(tmpname, 'w') as f: json.dump(graphtohost, f)
    os.rename(tmpname, hostmapname)
    
# get list of wikipedia DB names to import
def GetWikis():
    wikis= []
    # crude way to get all known wikis: parse the hosts file for labsdb host names...
    # XXXXXX labs apparently switched to proper dns instead of stuffing everything into /etc/hosts... todo: update this
    with open('/etc/hosts') as f:
        for line in f:
            m= re.findall("\W(\w{2,3}wik(?:i|tionary|ivoyage).labsdb)+", line)
            if m:
                for hostname in m:
                    dbname= hostname.split('.')[0]
                    try:
                        conn= MySQLdb.connect(read_default_file=os.path.expanduser('~')+'/.my.cnf', host=GetSQLServerForDB(dbname), db=dbname+'_p')
                        cursor= conn.cursor()
                        cursor.execute('SELECT * FROM categorylinks LIMIT 1')
                        cursor.fetchall()
                        cursor.execute('SELECT * FROM page LIMIT 1')
                        cursor.fetchall()
                        cursor.close()
                        conn.close()
                        wikis.append(dbname)
                    except MySQLdb.ProgrammingError:
                        pass
    return wikis

# action 'wiki-stats'
def WikiStats(servconfig):
    writer= csv.DictWriter(sys.stdout, fieldnames= [ "Wiki", "Category Links", "Category Links incl. Leaves", 
        "RAM Estimate Cat. Links (MB)", "RAM Estimate Leaf Links (MB)", "Graph exists (Categories only)", "Graph exists (incl. Leaves)" ] )
    writer.writeheader()
    existing_graphs= GetHostmap(servconfig)
    for dbname in GetWikis():
        if dbname=='enwiki': continue
        #~ if not 'wiktionary' in dbname: continue
        #~ if not 'wikivoyage' in dbname: continue
        conn= MySQLdb.connect(read_default_file=os.path.expanduser('~')+'/.my.cnf', host=GetSQLServerForDB(dbname), db=dbname+'_p')
        cursor= conn.cursor()
        query= "select count(*) from categorylinks where cl_type = 'subcat'"
        cursor.execute(query)
        subcatlinks= int(cursor.fetchall()[0][0])
        query= "select count(*) from categorylinks"
        cursor.execute(query)
        leaflinks= int(cursor.fetchall()[0][0])
        writer.writerow({ "Wiki": dbname, 
                "Category Links": subcatlinks, 
                "Category Links incl. Leaves": leaflinks, 
                "RAM Estimate Cat. Links (MB)": (subcatlinks * 16.1 + 1024*1024) / (1024*1024), 
                "RAM Estimate Leaf Links (MB)": (leaflinks * 16.1 + 1024*1024) / (1024*1024),
                "Graph exists (Categories only)": (dbname+'_ns14' in existing_graphs), 
                "Graph exists (incl. Leaves)": (dbname in existing_graphs) })
        sys.stdout.flush()

# action 'list-wikis'
def ListWikis():
    for w in GetWikis():
        print w

# action 'create-instanceconfig-missingwikis'
def CreateMissingWikisInstanceconfig(servconfig):
    config= []
    existing_graphs= set(GetHostmap(servconfig))
    wikis= set(GetWikis())
    wikis= set(map(lambda name: name+'_ns14', wikis))
    missing_wikis= wikis - existing_graphs
    for wiki in missing_wikis:
        config.append( { "name": wiki,
            "refreshIntervalHours": "4.0", 
            "namespaces": [ 14 ] } );
    print json.dumps(config, indent=4)
    

if __name__ == '__main__':
    parser= argparse.ArgumentParser(description= 'Catgraph Maintenance Job Script.', formatter_class= argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-s', '--server-config', default='~/.graphcare-serverconfig.json', help='server config file. ' + GraphservConfig.__init__.__doc__)
    parser.add_argument('-i', '--instance-config', default='~/.graphcare-instanceconfig.json', help='instance config file. ' + GraphcoreInstanceConfig.__init__.__doc__)
    parser.add_argument('-a', '--action', default=[], action='append',
        choices=['update', 'dump-all-graphs', 'load-all-graphs', 'refresh-host-map', 'wiki-stats', 'create-instanceconfig-missingwikis', 'list-wikis'], 
        help='action to run. \n* update: start graphserv if necessary, update graphs, refresh hostmap (default)\n * dump-all-graphs: save all running graphs to $graphservWorkDir/dumps.\n * load-all-graphs: load all graphs from $graphservWorkDir/dumps.')
    
    args= parser.parse_args()
    gc= GraphservConfig().load(os.path.expanduser(args.server_config))
    instances= GraphcoreInstanceConfig(os.path.expanduser(args.instance_config))
    
    if not len(args.action):
        args.action= ['update'] # default
    
    for action in args.action:
        if action=='update':
            CheckGraphserv(gc)
            CheckGraphcores(gc, instances)
            RefreshHostmap(gc)
        elif action=='dump-all-graphs':
            DumpAllGraphs(gc)
        elif action=='load-all-graphs':
            CheckGraphserv(gc)
            LoadAllGraphs(gc)
        elif action=='refresh-host-map':
            RefreshHostmap(gc)
        elif action=='wiki-stats':
            WikiStats(gc)
        elif action=='create-instanceconfig-missingwikis':
            CreateMissingWikisInstanceconfig(gc)
        elif action=='list-wikis':
            ListWikis()
    
    sys.exit(0)
