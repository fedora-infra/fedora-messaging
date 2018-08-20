%global pkgname fedora-messaging
%global srcname fedora_messaging
%global desc \
Tools and APIs to make working with AMQP in Fedora easier.

# EPEL7 support: default Python is Python 2, and Python2 packages prefix
# is unversioned.
%if 0%{?rhel} && 0%{?rhel} <= 7
%global py2_namespace python
%global default_pyver 2
%else
%global py2_namespace python2
%global default_pyver 3
%endif

Name:           %{pkgname}
Version:        1.0.0
Release:        0.1.a1%{?dist}
Summary:        Set of tools for using Fedora's messaging infrastructure

License:        GPLv2+
URL:            https://github.com/fedora-infra/fedora-messaging
Source0:        %{url}/archive/v%{version}a1/%{srcname}-%{version}a1.tar.gz
BuildArch:      noarch

BuildRequires:  %{py2_namespace}-devel
BuildRequires:  %{py2_namespace}-blinker
BuildRequires:  %{py2_namespace}-click
BuildRequires:  %{py2_namespace}-jsonschema
BuildRequires:  %{py2_namespace}-pytoml
BuildRequires:  %{py2_namespace}-pika
BuildRequires:  %{py2_namespace}-six
%if 0%{?rhel} && 0%{?rhel} <= 7
BuildRequires:  pytest
BuildRequires:  python-twisted-core
BuildRequires:  pyOpenSSL
%else
BuildRequires:  python2-pytest
BuildRequires:  python2-twisted
BuildRequires:  python2-pyOpenSSL
%endif
#BuildRequires:  %%{py2_namespace}-pytest-twisted
BuildRequires:  %{py2_namespace}-mock
# Python3
BuildRequires:  python%{python3_pkgversion}-devel
BuildRequires:  python%{python3_pkgversion}-blinker
BuildRequires:  python%{python3_pkgversion}-click
BuildRequires:  python%{python3_pkgversion}-jsonschema
BuildRequires:  python%{python3_pkgversion}-pytoml
BuildRequires:  python%{python3_pkgversion}-pika
BuildRequires:  python%{python3_pkgversion}-six
BuildRequires:  python%{python3_pkgversion}-pytest
BuildRequires:  python%{python3_pkgversion}-twisted
#BuildRequires:  python%%{python3_pkgversion}-pyOpenSSL
#BuildRequires:  python%%{python3_pkgversion}-pytest-twisted
BuildRequires:  python%{python3_pkgversion}-mock

BuildRequires:  python-sphinx

%description %{desc}

%package     -n python2-%{pkgname}
Summary:        %{summary}
Requires:       %{name} = %{version}
%{?python_enable_dependency_generator}

%description -n python2-%{pkgname} %{desc}

%package     -n python3-%{pkgname}
Summary:        %{summary}
Requires:       %{name} = %{version}
%{?python_enable_dependency_generator}

%description -n python3-%{pkgname} %{desc}


%package doc
Summary:        Documentation for %{pkgname}
%description doc
Documentation for %{pkgname}.


%prep
%autosetup -n %{srcname}-%{version}a1


%build
%py2_build
%py3_build
# generate docs
PYTHONPATH=${PWD} sphinx-build -M html -d docs/_build/doctrees docs docs/_build/html
PYTHONPATH=${PWD} sphinx-build -M man -d docs/_build/doctrees docs docs/_build/man
# remove the sphinx-build leftovers
rm -rf docs/_build/*/.buildinfo


%install
%py2_install
%py3_install
install -D -m 644 config.toml.example $RPM_BUILD_ROOT%{_sysconfdir}/fedora-messaging/config.toml
install -D -m 644 docs/_build/man/fedora-messaging.1 $RPM_BUILD_ROOT%{_mandir}/man1/fedora-messaging.1


%check
export PYTHONPATH=.
pytest
pytest-3


%files
%license LICENSE
%doc README.rst
%config(noreplace) %{_sysconfdir}/fedora-messaging/config.toml

%files -n python2-%{pkgname}
%license LICENSE
%{python2_sitelib}/*

%files -n python3-%{pkgname}
%license LICENSE
%{python3_sitelib}/*
%{_bindir}/%{name}
%{_mandir}/man1/%{name}.*

%files doc
%license LICENSE
%doc README.rst docs/*.rst docs/_build/html


%changelog
* Wed Aug 15 2018 Aurelien Bompard <abompard@fedoraproject.org> - 1.0.0-0.1.a1
- Initial package
