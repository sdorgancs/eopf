#!/bin/bash
function start_services {
    systemctl enable codeserver.service
    systemctl enable jupyterlab.service
    systemctl enable ray.service

    systemctl start codeserver.service
    systemctl start jupyterlab.service
    systemctl start ray.service
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
    sed -i "s/USER/${USERNAME}/g" /etc/systemd/system/ray.service
    echo "export USER=${USERNAME}" > /home/${USERNAME}/.local/user_env.sh
    echo "export USERFULLNAME=${USERFULLNAME}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export PYTHONPATH=${PYTHONPATH}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export PATH=/home/${USERNAME}/.local/bin:/usr/local/devtools/bin:\${PATH}" >> /home/${USERNAME}/.local/user_env.sh
    echo "conda activate eopf" >> /home/${USERNAME}/.local/user_env.sh
    echo "source /home/${USERNAME}/.local/user_env.sh" >> /home/${USERNAME}/.bashrc
}

rename_user
start_services
