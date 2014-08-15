# README

The spec files in this directory allow for building RAMCloud RPMs.  At the 
moment this has been only tested on Scientific Linux 6.

The ramcloud.spec files builds a ramcloud RPM containing 
/usr/bin/rc{Server,Coordinator} plus sub packages ramcloud-devel and 
ramcloud-libs.  The ramcloud-devel package contains the header files of the
C bindings in /usr/include/ramcloud.  The ramcloud-libs package contains the
ramcloud shared library.

## Building RAMCloud RPMs
The build environment has to have the ramcloud-zookeeper package installed.
This package can be built using the ramcloud-zookeeper.spec definition and
standard ZooKeeper sources.  The package is _not_ necessary for the final
RAMCloud packages because ZooKeeper code is statically linked.

The RAMCloud source tarball can be created from git like
    git archive --format=tar --prefix=ramcloud-1.0/ --output=ramcloud-1.0.tar \
        <treeish>
    gzip ramcloud-1.0.tar
    cp ramcloud-1.0.tar <rpmbuild directory>/SOURCES

The release string in the ramcloud.spec definition should be adapted to the date
of the snapshot and the git treeish.

Note that Infiniband support requires Infiniband hardware present on the
build machine.

