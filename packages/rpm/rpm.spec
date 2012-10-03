## This is a boilerplate file for Google opensource projects.
## To make it useful, replace <<TEXT>> with actual text for your project.
## Also, look at comments with "## double hashes" to see if any are worth
## uncommenting or modifying.

%define	RELEASE	1
%define rel     %{?CUSTOM_RELEASE} %{!?CUSTOM_RELEASE:%RELEASE}
%define	prefix	/usr

Name: %NAME
Summary: <<SUMMARY>>
Version: %VERSION
Release: %rel
Group: <<GROUP>>
URL: http://code.google.com/p/<<NAME>>/
License: <<BSD OR GPL OR ...>>
Vendor: Google
Packager: Google Inc. <opensource@google.com>
Source: http://%{NAME}.googlecode.com/files/%{NAME}-%{VERSION}.tar.gz
Distribution: Redhat 7 and above.
Buildroot: %{_tmppath}/%{name}-root
Prefix: %prefix

%description
<<DESCRIPTION>>

%package devel
Summary: <<SUMMARY>>
Group: <<GROUP>>
Requires: %{NAME} = %{VERSION}

%description devel
<<DESCRIPTION>.

%changelog
    * <<DATE>> <opensource@google.com>
    - First draft

%prep
%setup

%build
%configure
make

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

## Mark all installed files within /usr/share/doc/{package name} as
## documentation (eg README, Changelog).  This depends on the following
## two lines appearing in Makefile.am:
##     docdir = $(prefix)/share/doc/$(PACKAGE)-$(VERSION)
##     dist_doc_DATA = AUTHORS COPYING ChangeLog INSTALL NEWS README
%docdir %{prefix}/share/doc/%{NAME}-%{VERSION}
%{prefix}/share/doc/%{NAME}-%{VERSION}/*
## This captures the rest of your documentation; the stuff in doc/
## %doc doc/*

## If you have libraries, a likely line is this one:
## %{_libdir}/*.so.*

<<FILES TO INSTALL>>


%files devel
%defattr(-,root,root)

## If you have libraries, a likely set of lines are these:
## %{_includedir}/%{NAME}
## %{_libdir}/*.a
## %{_libdir}/*.la
## %{_libdir}/*.so
## %{_libdir}/pkgconfig/*.pc

<<ADDITIONAL FILES TO INSTALL FOR DEVELOPERS>>
