import subprocess
import sys
import os
import re
import socket

hosts = [
            ('pc813.emulab.net', '155.98.36.113', 0),
            ('pc806.emulab.net', '155.98.36.106', 1),
            ('pc807.emulab.net', '155.98.36.107', 2),
            ('pc805.emulab.net', '155.98.36.105', 3)
        ]

def ssh(server, cmd, checked=True):
    if checked:
        return subprocess.check_call('ssh %s "%s"' % (server, cmd),
                                     shell=True, stdout=sys.stdout)
    else:
        return subprocess.call('ssh %s "%s"' % (server, cmd),
                               shell=True, stdout=sys.stdout)

hostname = socket.gethostname()
def log(msg):
    print '[%s] %s' % (hostname, msg)

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
# replicas when one device is used.
default_disk1 = '-f /dev/sda2'

# Command-line argument specifying where the server should store the segment
# replicas when two devices are used.
default_disk2 = '-f /dev/sda2,/dev/sda3'

class EmulabClusterHooks:
    def __init__(self):
        self.remotewd = None

    def get_remote_wd(self):
        if self.remotewd is None:
            self.remotewd = os.path.join(captureSh('ssh %s pwd' % hosts[0][0]),
                                                   'RAMCloud')
        return self.remotewd

    def get_remote_scripts_path(self):
        return os.path.join(self.get_remote_wd(), 'scripts')

    def get_remote_obj_path(self):
        return os.path.join(self.get_remote_wd(), obj_dir)

    def send_code(self, server):
        log("Sending code to %s" % server)
        subprocess.check_call("rsync -ave ssh --exclude 'logs/*' " +
                              "--exclude 'docs/*' " +
                              "./ %s:%s/ > /dev/null" % (server,
                                                         self.get_remote_wd()),
                              shell=True, stdout=sys.stdout)
 
    def compile_code(self, server, clean=False):
        log("Compiling code on %s" % server)
        clean_cmd = ''
        if clean:
            clean_cmd = 'make clean;'
        ssh(server,
            '(cd %s; (%s make -j 8)  > ' % (self.get_remote_wd(), clean_cmd) +
             '%s/build.log)' % self.get_remote_wd())

    def create_log_dir(self, server):
        log("Creating %s on %s" % (self.cluster.log_subdir, server))
        ssh(server,
            '(cd %s; ' % self.get_remote_wd() +
            'mkdir -p $(dirname %s)/shm; ' % self.cluster.log_subdir +
            'mkdir -p %s; ' % self.cluster.log_subdir +
            'rm logs/latest; ' +
            'ln -sf $(basename %s) logs/latest)' % self.cluster.log_subdir)

    def fix_disk_permissions(self, server):
        log("Fixing disk permissions on %s" % server)
        disks = default_disk2.split(' ')[1].split(',')
        cmds = ['sudo chmod 777 %s' % disk for disk in disks]
        ssh(server, '(%s)' % ';'.join(cmds))

    def cluster_enter(self, cluster):
        self.cluster = cluster
        log('== Connecting to Emulab via %s ==' % hosts[0][0])
        for host in hosts:
            hostName = host[0]
            log('-- Preparing host ' + hostName)
            self.send_code(hostName)
            self.compile_code(hostName)
            self.create_log_dir(hostName)
            self.fix_disk_permissions(hostName)
        log('== Emulab Cluster Configured ==')

    def collect_logs(self, server):
        log('Collecting logs from host ' + server)
        subprocess.check_call("rsync -ave ssh " +
                              "%s:%s/logs/ logs/> /dev/null" % (server,
                                                         self.get_remote_wd()),
                              shell=True, stdout=sys.stdout)

    def cluster_exit(self):
        log('== Emulab Cluster Tearing Down ==')
        for host in hosts:
            hostName = host[0]
            self.collect_logs(hostName)
        log('== Emulab Cluster Torn Down ==')
        pass

hooks = EmulabClusterHooks()

local_scripts_path = os.path.dirname(os.path.abspath(__file__))
top_path = os.path.abspath(local_scripts_path + '/..')

obj_path = os.path.join(top_path, obj_dir)

