$Env:AWS_ACCESS_KEY="XXXXX"
$Env:AWS_SECRET_ACCESS_KEY="SSSSS"
$Env:AWS_SESSION_TOKEN="TTTTT"

aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/k0m1p4t7
docker build -t iot-packet-metrics .
docker tag iot-packet-metrics:latest public.ecr.aws/k0m1p4t7/iot-packet-metrics:latest
docker push public.ecr.aws/k0m1p4t7/iot-packet-metrics:latest

docker run -it -e AWS_ACCESS_KEY -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN --rm iot-packet-metrics
