#!/usr/bin/env python
"""
This module defines an Emulab specific cluster hooks and exposes configuration information
such as location of RAMCloud binaries and list of hosts. EMULAB_HOST environment must be 
exported to point to node-0 in the Cloudlab experiment where the caller has passwordless
ssh access. Check https://www.cloudlab.us/ssh-keys.php if you didn't export keys already

Sample localconfig.py 
from emulabconfig import *
hooks = EmulabClusterHooks(makeflags='-j12 DEBUG=no');
# EmulabClusterHooks(makeflags='-j12 DPDK=yes DPDK_DIR=/local/RAMCloud/deps/dpdk-16.07')
# builds for DPDK
hosts = getHosts()

"""

import subprocess
import sys
import os
import re
import socket
import xml.etree.ElementTree as ET

__all__ = ['getHosts', 'local_scripts_path', 'top_path', 'obj_path',
            'default_disks', 'EmulabClusterHooks', 'log'] 

hostname = socket.gethostname()

def log(msg):
    print '[%s] %s' % (hostname, msg)


# If run locally, connects to EMULAB_HOST and gets the manifest from there to
# populate host list, since this is invoked to compile RAMCloud (rawmetrics.py)
# the default is to see if you can get the manifest locally
def getHosts():
    nodeId = 0
    serverList = []
    try:
        log("trying to get manifest locally")
        out = captureSh("/usr/bin/geni-get manifest",shell=True, stderr=subprocess.STDOUT)
    except:
        log("trying EMULAB_HOST to get manifest")
        if 'EMULAB_HOST' not in os.environ:
            log("'EMULAB_HOST' not exported")
            sys.exit(1)
        out = subprocess.check_output("ssh %s /usr/bin/geni-get manifest" % os.environ['EMULAB_HOST'],
                                       shell=True, stderr=subprocess.STDOUT)

    root = ET.fromstring(out)
    for child in root.getchildren():
        if child.tag.endswith('node'):
            for host in child.getchildren():
                if host.tag.endswith('host'):
                    serverList.append((host.attrib['name'], host.attrib['ipv4'], nodeId))
                    nodeId += 1
    return serverList

def ssh(server, cmd, checked=True):
    """ Runs command on a remote machine over ssh.""" 
    if checked:
        return subprocess.check_call('ssh %s "%s"' % (server, cmd),
                                     shell=True, stdout=sys.stdout)
    else:
        return subprocess.call('ssh %s "%s"' % (server, cmd),
                               shell=True, stdout=sys.stdout)


def pdsh(cmd, checked=True):
    """ Runs command on remote hosts using pdsh on remote hosts"""
    log("Running parallely on all hosts")
    if checked:
        return subprocess.check_call('pdsh -w^./.emulab-hosts "%s"' % cmd,
                                     shell=True, stdout=sys.stdout)
    else:
        return subprocess.call('pdsh -w^./.emulab-hosts "%s"' %cmd,
                               shell=True, stdout=sys.stdout)

def captureSh(command, **kwargs):
    """Execute a local command and capture its output."""

    kwargs['shell'] = True
    kwargs['stdout'] = subprocess.PIPE
    p = subprocess.Popen(command, **kwargs)
    output = p.communicate()[0]
    if p.returncode:
        raise subprocess.CalledProcessError(p.returncode, command)
    if output.count('\n') and output[-1] == '\n':
        return output[:-1]
    else:
        return output

try:
    git_branch = re.search('^refs/heads/(.*)$',
                           captureSh('git symbolic-ref -q HEAD 2>/dev/null'))
except subprocess.CalledProcessError:
    git_branch = None
    obj_dir = 'obj'
else:
    git_branch = git_branch.group(1)
    obj_dir = 'obj.%s' % git_branch

# Command-line argument specifying where the server should store the segment
# replicas by default.
default_disks = '-f /dev/sdb,/dev/sdc'

class EmulabClusterHooks:
    def __init__(self, makeflags=''):
        self.remotewd = None
        self.hosts = getHosts()
	self.makeflags = makeflags
        self.parallel = self.cmd_exists("pdsh")
        if not self.parallel:
            log("NOTICE: Remote commands could be faster if you install and configure pdsh")
            self.remote_func = self.serial
        else:
            with open("./.emulab-hosts",'w') as f:
                for host in self.hosts:
                    f.write(host[0]+'\n')
            self.remote_func = pdsh

    def cmd_exists(self, cmd):
        return subprocess.call("type " + cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0
    
    def serial(self, cmd, checked=True):
        for host in self.hosts:
            log("Running on %s" % host[0])
            ssh(host[0], cmd, checked=checked)

    def get_remote_wd(self):
        if self.remotewd is None:
            self.remotewd = os.path.join(captureSh('ssh %s pwd' % self.hosts[0][0]),
                                                   'RAMCloud')
        return self.remotewd

    def get_remote_scripts_path(self):
        return os.path.join(self.get_remote_wd(), 'scripts')

    def get_remote_obj_path(self):
        return os.path.join(self.get_remote_wd(), obj_dir)

    def send_code(self):
        for host in self.hosts:
            log("Sending code to %s" % host[0])
            subprocess.check_call("rsync -ave ssh --exclude 'logs/*' " +
                              "--exclude 'docs/*' " +
                              "./ %s:%s/ > /dev/null" % (host[0],
                                                         self.get_remote_wd()),
                              shell=True, stdout=sys.stdout)
    
    def compile_code(self, clean=False):
        log("Compiling code")
        clean_cmd = ''
        if clean:
            clean_cmd = 'make clean;'
        self.remote_func('(cd %s; %s make %s  > %s/build.log 2>&1)' % (self.get_remote_wd(),
                          clean_cmd, self.makeflags, self.get_remote_wd()))

    def kill_procs(self):
        log("Killing existing processes")
        self.remote_func('sudo pkill -9 server')
        self.remote_func('sudo pkill -9 coordinator')
        self.remote_func('sudo pkill -9 ClusterPerf')

    def create_log_dir(self):
        log("creating log directories")
        self.remote_func(
            '(cd %s; ' % self.get_remote_wd() +
            'mkdir -p $(dirname %s)/shm; ' % self.cluster.log_subdir +
            'mkdir -p %s; ' % self.cluster.log_subdir +
            'rm logs/latest; ' +
            'ln -sf $(basename %s) logs/latest)' % self.cluster.log_subdir)

    def fix_disk_permissions(self):
        log("Fixing disk permissions")
        disks = default_disks.split(' ')[1].split(',')
        cmds = ['sudo chmod 777 %s' % disk for disk in disks]
        self.remote_func('(%s)' % ';'.join(cmds))

    def cluster_enter(self, cluster):
        self.cluster = cluster
        log('== Connecting to Emulab via %s ==' % self.hosts[0][0])
        self.kill_procs()
        self.send_code()
        self.compile_code(clean=False)
        self.create_log_dir()
        self.fix_disk_permissions()
        log('== Emulab Cluster Configured ==')
        log("If you are running clusterperf, it might take a while!")

    def collect_logs(self):
        log("Collecting logs")
        for host in self.hosts:
            subprocess.check_call("rsync -ave ssh " +
                                  "%s:%s/logs/ logs/> /dev/null" % (host[0],
                                                                    self.get_remote_wd()),
                                                                    shell=True, stdout=sys.stdout)

    def cluster_exit(self):
        log('== Emulab Cluster Tearing Down ==')
        self.collect_logs()
        log('== Emulab Cluster Torn Down ==')
        pass

local_scripts_path = os.path.dirname(os.path.abspath(__file__))
top_path = os.path.abspath(local_scripts_path + '/..')
obj_path = os.path.join(top_path, obj_dir)
