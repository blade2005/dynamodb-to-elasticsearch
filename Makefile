all: clean zip upload

zip:
	zip -r -x@exclude.lst lambda.zip *
clean:
	rm lambda.zip 
upload:
	cd ../; aws lambda update-function-code --function-name DynamoDBtoES --zip-file fileb://$(CURDIR)/lambda.zip --region us-west-2
