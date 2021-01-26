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
ENV container docker
## remove systemd unnecessary services

RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == \
  systemd-tmpfiles-setup.service ] || rm -f $i; done); \
  rm -f /lib/systemd/system/multi-user.target.wants/*; \
  rm -f /etc/systemd/system/*.wants/*; \
  rm -f /lib/systemd/system/local-fs.target.wants/*; \
  rm -f /lib/systemd/system/sockets.target.wants/*udev*; \
  rm -f /lib/systemd/system/sockets.target.wants/*initctl*; \
  rm -f /lib/systemd/system/basic.target.wants/*;


RUN systemctl set-default multi-user.target
RUN systemctl mask dev-hugepages.mount sys-fs-fuse-connections.mount


VOLUME ["/sys/fs/cgroup", "/tmp", "/run", "/run/lock"]
STOPSIGNAL SIGRTMIN+3

# Add a user `dev` so that you're not developing as the `root` user
# The user needs to sudoer be able to install code-server
RUN adduser --gecos '/usr/bin/bash' --disabled-password eopfdev && \
  echo "eopfdev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/nopasswd
RUN groupadd docker && usermod -aG docker eopfdev
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

# Install docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
RUN apt-key fingerprint 0EBFCD88
RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
RUN apt update -y && apt install -y docker-ce docker-ce-cli containerd.io

RUN chown -R eopfdev:eopfdev /usr/local/devtools


USER eopfdev
WORKDIR /home/eopfdev
# Create USER environment variable to prevent "could not determine the current user, please set $USER" error when running "cargo new ..."
ENV USER eopfdev

ENV PATH /usr/local/devtools/bin:${PATH}

# RUN conda install pip -y
# # Install JupyterLab
RUN conda install -c conda-forge -y jupyterlab nodejs nbdime nbresuse jupyterlab-git jupyter-resource-usage ipykernel
ADD jupyter_notebook_config.py /usr/local/devtools/etc/jupyter/jupyter_notebook_config.py
# Install JupyterLab extensions
# RUN conda install -y ipykernel
# RUN conda install -y -c conda-forge 
ADD jupyter_notebook_config.py /usr/local/devtools/etc/jupyter/jupyter_notebook_config.py
RUN jupyter serverextension enable --py nbdime --sys-prefix
RUN jupyter labextension install nbdime-jupyterlab
RUN jupyter serverextension enable --py nbresuse --sys-prefix
RUN jupyter nbextension install --py nbresuse --sys-prefix
RUN jupyter nbextension enable --py nbresuse --sys-prefix


# Install code-server extensions

# Python https://github.com/cdr/code-server/issues/2341
# https://github.com/microsoft/vscode-python/issues/14579
RUN curl -L https://github.com/microsoft/vscode-python/releases/download/2020.9.114305/ms-python-release.vsix \
  -o ms-python-release.vsix
RUN code-server --install-extension ./ms-python-release.vsix --force || true
RUN rm ms-python-release.vsix
# # RUN code-server --install-extension donjayamanne.python-extension-pack
# # RUN code-server --install-extension LittleFoxTeam.vscode-python-test-adapter
# RUN code-server --install-extension njpwerner.autodocstring
# ## Jupyter https://github.com/cdr/code-server/issues/2293
# # RUN curl -L https://marketplace.visualstudio.com/_apis/public/gallery/publishers/ms-toolsai/vsextensions/jupyter/2020.12.414227025/vspackage \
# #   -o ms-toolsai.jupyter.vsix
# # RUN code-server --install-extension ./ms-toolsai.jupyter.vsix --force || true
# ## Markdown
RUN code-server --install-extension yzhang.markdown-all-in-one
## JSon
RUN code-server --install-extension nickdemayo.vscode-json-editor
## Toml
RUN code-server --install-extension bungcip.better-toml
## yaml
RUN code-server --install-extension redhat.vscode-yaml
## rst
# RUN code-server --install-extension lextudio.restructuredtext

# # clone eopf
RUN git clone https://github.com/sdorgancs/eopf.git

# Init conda env eopf
RUN conda env create -f eopf/environment.yml
SHELL ["conda", "run", "-n", "eopf", "/bin/bash", "-c"]
RUN conda init bash

ADD vscode /home/eopfdev/eopf/.vscode
ADD env /home/eopfdev/eopf/.env
ADD settings.json /home/eopfdev/.local/share/code-server/User

ADD config /home/eopfdev/.eopf/config

RUN npm install -g ungit

# Configure services
USER root
WORKDIR /
# install file browser
RUN curl -fsSL https://filebrowser.org/get.sh | bash
RUN chmod a+x /usr/local/bin/filebrowser
USER eopfdev
WORKDIR /home/eopfdev/.local
RUN /usr/local/bin/filebrowser config init --baseurl /browser --auth.method=noauth --port=8888
# https://github.com/filebrowser/filebrowser/issues/700
RUN /usr/local/bin/filebrowser users add "default-user" "default-password"
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
ADD ray.service /etc/systemd/system/ray.service
ADD filebrowser.service /etc/systemd/system/filebrowser.service
ADD gitwebui.service /etc/systemd/system/gitwebui.service

RUN mkdir -p /var/www/ui
ADD dashboard /var/www/ui

RUN systemctl enable /etc/systemd/system/caddy.service
RUN systemctl enable /etc/systemd/system/bootstrap.service
RUN systemctl enable docker
RUN systemctl enable rsyslog
#https://github.com/j8r/dockerfiles/tree/master/systemd/ubuntu
#ADD journald.conf /etc/systemd/journald.conf

ADD start.sh /sbin
RUN chmod u+x /sbin/start.sh
VOLUME /home
CMD ["/bin/bash", "-c", "exec /sbin/init --log-target=journal 3>&1"]