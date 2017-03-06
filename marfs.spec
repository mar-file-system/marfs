# http://stackoverflow.com/questions/880227/what-is-the-minimum-i-have-to-do-to-create-an-rpm-file
# Don't try fancy stuff like debuginfo, which is useless on binary-only
# packages. Don't strip binary too
# Be sure buildpolicy set to do nothing
%define        __spec_install_post %{nil}
%define          debug_package %{nil}
%define        __os_install_post %{_dbpath}/brp-compress
Name:		marfs
Version:	1.6
Release:	1%{?dist}
Summary:	MarFS

Group:		Utilities/System
License:	BSD
URL:		https://github.com/mar-file-system/marfs
Source0:	%{name}-%{version}.tar.gz
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires:	libcurl-devel libattr-devel
BuildRequires:	glibc-devel fuse-devel libattr-devel make curl-devel
BuildRequires:	openssl-devel libxml2-devel
Requires:	curl openssl

%description
MarFS provides a scalable near-POSIX file system by using one or more POSIX
file systems as a scalable metadata component and one ore more data stores
(object, file, etc) as a scalable data component. Our initial implementation
will use GPFS file systems as the metadata component and Scality and EMC ECS
ViPR object stores as the data component.


%prep
%setup -q


%build
./do_build.sh

%install
rm -rf %{buildroot}
mkdir -p  %{buildroot}

# in builddir
cp -a * %{buildroot}
find %buildroot -type f \( -name '*.so' -o -name '*.so.*' \) -exec chmod 755 {} +


%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_libdir}/libmarfs.so*
/usr/lib/marfs/scripts/*
/usr/lib/marfs/parse-inc/*
%doc /usr/share/doc/%{name}-%{version}/*





%changelog
* Fri Oct 21 2016  Yuriy Shestakov <yuriy.shestakov@archive-engines.com> 0.92-beta
- Assemble binary RPM 
