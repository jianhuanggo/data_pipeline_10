export REPO_NAME=pg_finance_trade_test
export AWS_ACCOUNT_NUMBER=717435123117
export AWS_CLIENT=latest
export AWS_REGION=us-east-1
export API_GATEWAY_API_ID=$(aws apigateway --profile ${AWS_CLIENT} get-rest-apis | jq -r -c '.items[] | select(.name | contains("MyApi")) | .id')
export API_GATEWAY_ROOT_RES_ID=$(aws apigateway --profile ${AWS_CLIENT} get-resources --rest-api-id ${API_GATEWAY_API_ID} | jq -r -c '.items[] | select(.path == "/") | .id')


export API_GATEWAY_RES_ID=$(aws apigateway --profile ${AWS_CLIENT} get-resources --rest-api-id ${API_GATEWAY_API_ID} --query "items[?pathPart=='${REPO_NAME}'].id" --output text)


aws apigateway --profile ${AWS_CLIENT} delete-method-response --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET --status-code 200
aws apigateway --profile ${AWS_CLIENT} delete-integration-response --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET --status-code 200

aws apigateway --profile ${AWS_CLIENT} delete-integration --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET
aws apigateway --profile ${AWS_CLIENT} delete-method --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID} --http-method GET


aws apigateway --profile ${AWS_CLIENT} delete-resource --rest-api-id ${API_GATEWAY_API_ID} --resource-id ${API_GATEWAY_RES_ID}
aws lambda --profile ${AWS_CLIENT} delete-function --function-name lambda-${REPO_NAME}

aws ecr --profile ${AWS_CLIENT} delete-repository --repository-name ${REPO_NAME} --force