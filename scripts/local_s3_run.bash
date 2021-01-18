source env.bash

echo "starting fake S3 server"
export SERVICES=s3:5000
export DEFAULT_REGION=us-east-1

docker run -d -p 5000:5000 -p 8080:8080 \
    -e SERVICES \
    -e DEFAULT_REGION \
    --name s3-test \
    atlassianlabs/localstack
echo "waiting 10 seconds for fake s3 server to be ready"
sleep 10

if [[ ! -f mc ]]
then
    echo "downloading mc command line tool"
    curl -L -o mc https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod u+x mc
fi
echo "prepare s3 bucket"
./mc alias set s3 http://localhost:5000 ${AWS_ACCESS_KEY_ID} ${AWS_DEFAULT_REGION}
./mc mb s3/testbucket
./mc cp ../README.md s3/testbucket
