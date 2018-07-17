%{!?__python2: %global __python2 %__python}
%{!?python2_sitelib: %global python2_sitelib %(%{__python2} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}

%global pkgname fedora-messaging
%global srcname fedora_messaging
%global desc \
Tools and APIs to make working with AMQP in Fedora easier.


Name:           %{pkgname}
Version:        1.0.0
Release:        0.1.a1%{?dist}
Summary:        Set of tools for using Fedora's messaging infrastructure

License:        GPLv2+
URL:            https://github.com/fedora-infra/fedora-messaging
Source0:        https://github.com/fedora-infra/%{name}/archive/v%{version}a1/%{srcname}-%{version}a1.tar.gz
BuildArch:      noarch

BuildRequires:  python2-devel
BuildRequires:  python2-blinker
BuildRequires:  python2-click
BuildRequires:  python2-jsonschema
BuildRequires:  python2-pytoml
BuildRequires:  python2-pika
BuildRequires:  python2-six
BuildRequires:  python2-pytest
BuildRequires:  python2-twisted
BuildRequires:  python2-pyOpenSSL
#BuildRequires:  python2-pytest-twisted
BuildRequires:  python2-mock
# Python3
BuildRequires:  python3-devel
BuildRequires:  python3-blinker
BuildRequires:  python3-click
BuildRequires:  python3-jsonschema
BuildRequires:  python3-pytoml
BuildRequires:  python3-pika
BuildRequires:  python3-six
BuildRequires:  python3-pytest
BuildRequires:  python3-twisted
BuildRequires:  python3-pyOpenSSL
#BuildRequires:  python3-pytest-twisted
BuildRequires:  python3-mock
BuildRequires:  python3-sphinx

%description %{desc}

%package     -n python2-%{pkgname}
Summary:        %{summary}
Requires:       %{name} = %{version}
%{?python_provide:%python_provide python2-%{pkgname}}
Requires:       python2-blinker
Requires:       python2-click
Requires:       python2-jsonschema
Requires:       python2-pytoml
Requires:       python2-pika
Requires:       python2-six

%description -n python2-%{pkgname} %{desc}

%package     -n python3-%{pkgname}
Summary:        %{summary}
Requires:       %{name} = %{version}
%{?python_provide:%python_provide python3-%{pkgname}}
Requires:       python3-blinker
Requires:       python3-click
Requires:       python3-jsonschema
Requires:       python3-pytoml
Requires:       python3-pika
Requires:       python3-six

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
PYTHONPATH=${PWD} sphinx-build-3 -M html -d docs/_build/doctrees docs docs/_build/html
PYTHONPATH=${PWD} sphinx-build-3 -M man -d docs/_build/doctrees docs docs/_build/man
# remove the sphinx-build leftovers
#rm -rf docs/_build/.{doctrees,buildinfo}
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
