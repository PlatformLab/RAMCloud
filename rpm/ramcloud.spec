%define zookeeper_version 3.4.5

Name:		ramcloud	
Version:	1.0
Release:	20140807gitfbe68a%{?dist}
Summary:	RAMCloud key-value store
Group:		Applications/System
License:	MIT
URL:		http://ramcloud.stanford.edu
Source0:	%{name}-%{version}.tar.gz
# The tarball is created like so 'git archive --format=tar --prefix=ramcloud-1.0/ --output=ramcloud-1.0.tar fbe68a'
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: boost-devel
BuildRequires: gcc-c++ >= 4.4.6
BuildRequires: gtest-devel
BuildRequires: make
BuildRequires: openssl-devel
BuildRequires: pcre-devel
BuildRequires: perl
BuildRequires: protobuf-devel
BuildRequires: ramcloud-zookeeper = %{zookeeper_version}

%description
RAMCloud is a key-value store that keeps all data in DRAM at all times (it is not a cache like memcached). Furthermore, it takes advantage of high-speed networking such as Infiniband or 10Gb Ethernet to provide very high performance.

%package devel
Summary: RAMCloud headers
Group: Applications/System
%description devel
Headers for RAMCloud C bindings

%package libs
Summary: RAMCloud libraries
Group: Applications/System
%description libs
RAMCloud shared libraries

%prep
%setup -q

%build
make %{?_smp_mflags} \
  DEBUG=no \
  BASECFLAGS='-g -I/opt/ramcloud-zookeeper-%{zookeeper_version}/usr/include' \
  ZOOKEEPER_LIB=/opt/ramcloud-zookeeper-%{zookeeper_version}/usr/lib/libzookeeper_mt.a   

%install
rm -rf %{buildroot}
install -D -m 755 obj/server %{buildroot}%{_bindir}/rcServer
install -D -m 755 obj/coordinator %{buildroot}%{_bindir}/rcCoordinator

mkdir -p %{buildroot}/usr/include/ramcloud
install -m 644 -t %{buildroot}/usr/include/ramcloud \
  src/CRamCloud.h \
  src/Status.h \
  src/RejectRules.h

install -D -m 755 obj/libramcloud.so %{buildroot}%{_libdir}/libramcloud.so.1.0.0
ln -s libramcloud.so.1.0.0 %{buildroot}%{_libdir}/libramcloud.so.1
ln -s libramcloud.so.1 %{buildroot}%{_libdir}/libramcloud.so

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/rcServer
%{_bindir}/rcCoordinator
%doc

%files devel
%defattr(-,root,root,-)
%{_includedir}/ramcloud

%files libs
%defattr(-,root,root,-)
%{_libdir}/libramcloud.*

%changelog
* Thu Aug 7 2014 Jakob Blomer <jblomer@cern.ch> - 1.0-20140807gitfbe68a
- Initial packaging
