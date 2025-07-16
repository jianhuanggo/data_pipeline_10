export REPO_NAME=pg_finance_trade_test8
export AWS_ACCOUNT_NUMBER=717435123117
export AWS_CLIENT=latest
export AWS_REGION=us-east-1
export API_GATEWAY_API_ID=$(aws apigateway --profile ${AWS_CLIENT} get-rest-apis | jq -r -c '.items[] | select(.name | contains("MyApi")) | .id')
export API_GATEWAY_ROOT_RES_ID=$(aws apigateway --profile ${AWS_CLIENT} get-resources --rest-api-id ${API_GATEWAY_API_ID} | jq -r -c '.items[] | select(.path == "/") | .id')


export DOCKER_DEFAULT_PLATFORM=linux/amd64

pip freeze > requirements.txt

aws ecr --profile latest create-repository --repository-name ${REPO_NAME}
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_NUMBER}.dkr.ecr.${AWS_REGION}.amazonaws.com
docker build -t ${REPO_NAME} .
docker tag ${REPO_NAME}:latest ${AWS_ACCOUNT_NUMBER}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:latest
docker push ${AWS_ACCOUNT_NUMBER}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:latest


aws iam --profile ${AWS_CLIENT} create-role --role-name lambda-ex --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"}]
  }'


aws lambda --profile ${AWS_CLIENT} create-function --function-name lambda-${REPO_NAME} --package-type Image --role arn:aws:iam::${AWS_ACCOUNT_NUMBER}:role/lambda-ex --code ImageUri=${AWS_ACCOUNT_NUMBER}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:latest --timeout 30
export API_GATEWAY_RES_ID=$(aws apigateway --profile ${AWS_CLIENT} create-resource --rest-api-id ${API_GATEWAY_API_ID} --parent-id ${API_GATEWAY_ROOT_RES_ID} --path-part ${REPO_NAME} | jq -r -c ".id")



aws apigateway --profile ${AWS_CLIENT} put-method --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET --authorization-type "NONE" --no-api-key-required
aws apigateway --profile ${AWS_CLIENT} put-integration --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET --type AWS_PROXY --integration-http-method POST --uri "arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_NUMBER}:function:lambda-${REPO_NAME}/invocations" --credentials arn:aws:iam::717435123117:role/role-api-gateway-ex


aws apigateway --profile ${AWS_CLIENT} put-integration-response --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET --status-code 200
aws apigateway --profile ${AWS_CLIENT} put-method-response --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET --status-code 200 --response-parameters "{}" --response-models '{"application/json": "Empty"}'


aws apigateway --profile ${AWS_CLIENT} create-deployment --rest-api-id ${API_GATEWAY_API_ID} --stage-name prod --description 'Deploying new method to prod'


test:

https://cw8ttqau95.execute-api.us-east-1.amazonaws.com/prod/pg_finance_trade_test8