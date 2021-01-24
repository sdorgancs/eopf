#!/bin/bash
function start_services {
    systemctl enable codeserver.service
    systemctl enable jupyterlab.service
    systemctl enable ray.service
    systemctl enable filebrowser.service

    systemctl start codeserver.service
    systemctl start jupyterlab.service
    systemctl start ray.service
    systemctl start filebrowser.service
}


function rename_user {
    echo "rename eopfdev in ${USERNAME}" > /logs
    id eopfdev
    usermod -l ${USERNAME} eopfdev
    groupmod -n ${USERNAME} eopfdev
    usermod -d /home/${USERNAME} -m ${USERNAME}
    usermod -c "${USERFULLNAME}" ${USERNAME}
    usermod -aG docker ${USERFULLNAME}
    id $1

    # echo "${USERNAME}:100000:65536" > /etc/subgid
    # echo "${USERNAME}:100000:65536" > /etc/subuid

    sed -i "s/USER/${USERNAME}/g" /etc/systemd/system/jupyterlab.service
    sed -i "s/USER/${USERNAME}/g" /etc/systemd/system/codeserver.service
    sed -i "s/USER/${USERNAME}/g" /etc/systemd/system/ray.service
    sed -i "s/USER/${USERNAME}/g" /etc/systemd/system/filebrowser.service

    sed -i "s/USER/${USERNAME}/g" /home/${USERNAME}/eopf/.env

    echo "export USER=${USERNAME}" > /home/${USERNAME}/.local/user_env.sh
    echo "export USERFULLNAME=${USERFULLNAME}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export PYTHONPATH=/home/${USERNAME}/eopf" >> /home/${USERNAME}/.local/user_env.sh
    echo "export AWS_ACCESS_KEY_ID=test" >> /home/${USERNAME}/.local/user_env.sh
    echo "export AWS_SECRET_ACCESS_KEY=test" >> /home/${USERNAME}/.local/user_env.sh
    echo "export AWS_DEFAULT_REGION=us-east-1" >> /home/${USERNAME}/.local/user_env.sh
    echo "export GIT_AUTHOR_NAME=${USERFULLNAME}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export GIT_AUTHOR_EMAIL=${USEREMAIL}" >> /home/${USERNAME}/.local/user_env.sh
    echo "export PATH=/home/${USERNAME}/bin:/home/${USERNAME}/.local/bin:/usr/local/devtools/bin:\${PATH}" >> /home/${USERNAME}/.local/user_env.sh
    echo "conda activate eopf" >> /home/${USERNAME}/.local/user_env.sh
    echo "source /home/${USERNAME}/.local/user_env.sh" >> /home/${USERNAME}/.bashrc
    chown -R ${USERNAME}:${USERNAME} /home/${USERNAME}/eopf
}

rename_user
start_services
