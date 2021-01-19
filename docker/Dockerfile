FROM ubuntu:20.04

# Upgrade and install curl, sudo, essential development tools nginx, and xubuntu-desktop
RUN apt update -y && apt upgrade -y

ENV DEBIAN_FRONTEND=noninteractive 
RUN  apt update -y && apt install -y --no-install-recommends curl git sudo build-essential \
                   lldb python dirmngr gnupg \
                   apt-transport-https ca-certificates software-properties-common \
                   systemd dbus systemd systemd-cron rsyslog iproute2 libnss3 \
                   locales \
                   && apt clean -y

# Free a bit of space
RUN rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV container docker LANG=C.UTF-8
# Systemd post install

## configure rsyslog
RUN sed -i 's/^\(module(load="imklog")\)/#\1/' /etc/rsyslog.conf

## remove systemd unnecessary services
RUN find /etc/systemd/system \
    /lib/systemd/system \
    -path '*.wants/*' \
    -not -name '*dbus*' \
    -not -name '*journald*' \
    -not -name '*systemd-tmpfiles*' \
    -not -name '*systemd-user-sessions*' \
    -exec rm \{} \;

RUN systemctl set-default multi-user.target
RUN systemctl mask dev-hugepages.mount sys-fs-fuse-connections.mount


VOLUME ["/sys/fs/cgroup", "/tmp", "/run", "/run/lock"]
STOPSIGNAL SIGRTMIN+3

# Add a user `dev` so that you're not developing as the `root` user
# The user needs to sudoer be able to install code-server
RUN adduser --gecos '/usr/bin/bash' --disabled-password eopfdev && \
  echo "eopfdev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/nopasswd
ENV SHELL bash

# Install miniconda
RUN curl -L https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o Miniconda3-latest-Linux-x86_64.sh
RUN chmod u+x ./Miniconda3-latest-Linux-x86_64.sh
RUN sudo ./Miniconda3-latest-Linux-x86_64.sh -bfp /usr/local/devtools
RUN rm -f Miniconda3-latest-Linux-x86_64.sh

# Install code-server 
RUN curl -fsSL https://code-server.dev/install.sh > install.sh
RUN sh install.sh --method=standalone --prefix=/usr/local/devtools
RUN rm -f install.sh


RUN chown -R eopfdev:eopfdev /usr/local/devtools


USER eopfdev
WORKDIR /home/eopfdev
# Create USER environment variable to prevent "could not determine the current user, please set $USER" error when running "cargo new ..."
ENV USER eopfdev

ENV PATH /usr/local/devtools/bin:${PATH}


# Install JupyterLab
RUN conda install -c conda-forge -y jupyterlab

# Install JupyterLab extensions
RUN conda install -y ipykernel
RUN conda install -y -c conda-forge jupyterlab-git
ADD jupyter_notebook_config.py /usr/local/devtools/etc/jupyter/jupyter_notebook_config.py


# Install code-server extensions
## git
RUN code-server --install-extension eamodio.gitlens
## Python
RUN code-server --install-extension donjayamanne.python-extension-pack
RUN code-server --install-extension LittleFoxTeam.vscode-python-test-adapter
RUN code-server --install-extension njpwerner.autodocstring
## Jupyter
RUN code-server --install-extension ms-toolsai.jupyter
## Markdown
RUN code-server --install-extension yzhang.markdown-all-in-one
## JSon
RUN code-server --install-extension nickdemayo.vscode-json-editor
## Toml
RUN code-server --install-extension bungcip.better-toml
## yaml
RUN code-server --install-extension redhat.vscode-yaml
## rst
RUN code-server --install-extension lextudio.restructuredtext

# clone eopf
RUN git clone https://github.com/sdorgancs/eopf.git

RUN conda env create -f eopf/environment.yml

# Configure services
USER root
WORKDIR /
RUN curl -L https://github.com/caddyserver/caddy/releases/download/v2.2.1/caddy_2.2.1_linux_amd64.tar.gz | tar -xvz
RUN mv caddy /usr/local/bin
RUN mkdir -p /usr/local/lib/caddy
ADD Caddyfile /usr/local/lib/caddy/Caddyfile

ADD caddy.service /etc/systemd/system/caddy.service
ADD bootstrap.service /etc/systemd/system/bootstrap.service
ADD jupyterlab.service /etc/systemd/system/jupyterlab.service
ADD codeserver.service /etc/systemd/system/codeserver.service

RUN systemctl enable /etc/systemd/system/caddy.service
RUN systemctl enable /etc/systemd/system/bootstrap.service

ADD start.sh /sbin
RUN chmod u+x /sbin/start.sh
CMD ["/sbin/init"]