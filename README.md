# Road Monitoring backend server 
APIs and presentation server for road monitoring project. Follow the step-by-step guide here [link TBA]

Files:
- `flask_app.py` contains the server logic and exposes the APIs on `/road-data`, PUT and GET to respectively push and pull data on the cloud
- `aws_lambda.py` is not meant to be run, but to be copied in your newly created Lambda function on AWS, as illustrated in our guide
- `retrieve_object.py` deals with collecting data from AWS S3 storage
