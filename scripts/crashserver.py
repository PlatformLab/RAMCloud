import cluster
from cluster import *
from optparse import OptionParser

"""
Crashes a server specified by the service locator by killing it.
"""

def kill_server(options):

    new_cluster = Cluster(log_exists=True, cluster_name_exists=True)
    locator = options.service_locator
    new_cluster.kill_server(locator)
    new_cluster.__exit__(None, None, None)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-l", "--locator", dest="service_locator",
                      help="Service locator of the server that"
                      "needs to be killed")
    (options,args) = parser.parse_args()

    kill_server(options)
