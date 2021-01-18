#TODO change passwor when tests are ok
source env.sh

echo "starting S3 server"
export SERVICES=s3:5000
export DEFAULT_REGION=us-east-1

docker run -d -p 5000:5000 -p 8080:8080 \
    -e SERVICES \
    -e DEFAULT_REGION \
    --name s3-test \
    atlassianlabs/localstack
sleep 10

./mc alias set s3 http://localhost:5000 ${AWS_ACCESS_KEY_ID} ${AWS_DEFAULT_REGION}
./mc mb s3/testbucket
./mc cp README.md s3/testbucket
