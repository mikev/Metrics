$Env:AWS_ACCESS_KEY="XXXXX"
$Env:AWS_SECRET_ACCESS_KEY="SSSSS"
$Env:AWS_SESSION_TOKEN="TTTTT"

docker build -t iot-ingest-image -f Dockerfile .
docker run -it -e AWS_ACCESS_KEY -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN --rm iot-ingest-image
