# taxi-poc-aws

AWS Assignment for Innovation Architect: Retrieve data from TLC and generate illustrations for different users.


# what is this project?

As said, this is a project for AWS interview.

So, all things are based on aws.


## data-pre-treatment

### Steps:

- Create a s3 bucket named **taxi-poc-formatted** and create an EMR cluster.
- Add *format_and_split_csv.py* to cluster's step and wait until finish.
- Now you will get pretty data in **taxi-poc-formatted** bucket on s3.

### Additional:

The data format in the open data is not consistent from Y2015 to Y2018. So, we have to explorer the data formats:

- Run *scan_data_types.py* to get all header types.
- One more problem: some data don't have pickup & dropoff location lat & lng. They only have location ID.
- Run *scan_location_latlng.py* to get locations' lat & lng from google api.

## data-replay

### Steps:

- Create a Kinesis data stream named **taxi-poc-input**.
- Run maven project under data-replay folder.

### Additional:

The hardest part would be speed control. We need to create 3 threads:

- One for reading. It reads the file from s3 and put data into a Queue.
- One for writing. It writes the data to Kinesis data stream.
- One for speed controlling. It tells writing thread how many records to write and sleep for a while.

Reference: https://github.com/aws-samples/amazon-kinesis-replay.

## data-analytics

### Steps:

- Create a Kinesis data stream named **taxi-poc-trip-record**.
- Go into data-analytics folder run maven clean package & upload the package to S3.
- Create a Kinesis data analytics flink application & start with the package.

Now you will get pretty records in **taxi-poc-trip-record**.

- Build an AWS elasticsearch cluster and build index for data-record(in scripts folder)
- Create a Kinesis firehose application to transfer data. Choose the cluster as target.


## Backend & Frontend

Deploy them in VPC at any way you like.
