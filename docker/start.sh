#!/bin/bash
function start_services {
    # systemctl enable desktop.service
    systemctl enable codeserver.service
    # systemctl enable rstudio.service
    systemctl enable jupyterlab.service
    systemctl start codeserver.service
    # systemctl start rstudio.service
    systemctl start jupyterlab.service
}


function rename_user {
    echo "rename eopfdev in ${USERNAME}" > /logs
    id eopfdev
    usermod -l ${USERNAME} eopfdev
    groupmod -n ${USERNAME} eopfdev
    usermod -d /home/${USERNAME} -m ${USERNAME}
    usermod -c "${USERFULLNAME}" ${USERNAME}
    id $1
    sed -i "s/USER/${USERNAME}/g" /etc/systemd/system/jupyterlab.service
    sed -i "s/USER/${USERNAME}/g" /etc/systemd/system/codeserver.service
    echo "export USER=${USERNAME}" > /home/${USERNAME}/.local/user_env.sh
    echo "export PATH=/home/${USERNAMME}/.local/bin" >> /home/${USERNAME}/.local/user_env.sh
    echo "source /home/${USERNAME}/.local/user_env.sh" /home/${USERNAME}/.bashrc
}

rename_user
start_services