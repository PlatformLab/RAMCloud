from __future__ import division, print_function
import cluster
from cluster import *
import config
import os
import pprint
from optparse import OptionParser

"""
Starts a new server (master and backup) on one of the hosts available
from config.py. Returns to the client the service locator of the server
via a print message.
"""

def start_server(options):

    new_cluster = Cluster(log_exists=True, cluster_name_exists=True)

    path = options.shared_dir
    # sorted files only
    files = sorted([f for f in os.listdir(path)
               if os.path.isfile( os.path.join(path, f) )])

    hosts_used = []
    cluster_name = None
    for file in files:
        if file.startswith("README"):
            continue
        elif (file.startswith("cluster")):
            cluster_name = file
            continue
        else:
            hosts_used.append(file.split('_')[0])

    hosts = getHosts()
    mhost = hosts[0] # just default value
    for host in hosts:
        # host[0] = rcxx
        if host[0] not in hosts_used:
            mhost = host
            break
        if host == hosts[len(hosts) - 1]: # end of list
            mhost = 'NOSERVER'

    # no masters are available, indicate this to the client
    if mhost == 'NOSERVER':
        print ('%s' % 'NOSERVER')
    else:
        rchost = mhost[0] # rchost = rcxx
        host = rchost.split('_')[1] # host = xx, just the host number
        print ('%s' % (server_locator(new_cluster.transport, mhost, server_port)))
        # mhost is the correct host
        if cluster_name is not None:
            new_cluster.start_server(mhost, args='-d --clusterName=%s' %(cluster_name),
                                    backup=True, kill_on_exit=False)
        else:
            new_cluster.start_server(mhost, backup=True, kill_on_exit=False)

    new_cluster.__exit__(None, None, None)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--dir", dest="shared_dir",
                    help="Directory shared between hosts for cleanup")
    (options,args) = parser.parse_args()

    start_server(options)
