Name:		ramcloud-zookeeper
Version:	3.4.5
Release:	1
Summary:	An Apache Zookeeper distribution for RAMCloud
Group:		Applications/System
License:	APL 2.0
URL:		http://zookeeper.apache.org
Source0:	zookeeper-%{version}.tar.gz
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: ant
BuildRequires: apache-ivy
BuildRequires: cppunit-devel
BuildRequires: gcc
BuildRequires: java-devel
BuildRequires: java-javadoc
BuildRequires: libtool
BuildRequires: perl

%description
This is a distribution of ZooKeeper that is just enough for building RAMCloud RPMs.
Files reside in /opt.

%prep
%setup -q -n zookeeper-%{version}


%build
ant package-native


%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/%{name}-%{version}
cp -av build/c/build/* %{buildroot}/opt/%{name}-%{version}/


%clean
rm -rf %{buildroot}


%files
%defattr(-,root,root,-)
/opt/%{name}-%{version}


%changelog
* Thu Aug 7 2014 Jakob Blomer <jblomer@cern.ch> - 3.4.5-1
- Initial packaging
