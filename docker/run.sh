docker rm -f eopfsde
docker build -t eopfsde .
docker run --name eopfsde -dt --rm \
            -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
            -v $(pwd)/env:/env  \
            -p 80:80 \
            -p 443:443 \
            --tmpfs /run --tmpfs /run/lock --tmpfs /tmp \
            --security-opt seccomp=unconfined \
            --env USERNAME=toto \
            --env USERFULLNAME="toto toto" \
            eopfsde

docker logs -f eopfsde
