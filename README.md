*A full write-up on this project can be found on my [blog](https://skhan-tech.github.io/2020/04/03/Classifying-The-Web/).*

# Business Classifier

The goal of this project is to predict whether a website is a business website or not. The code is split into three logical components.

1. Business Classifier Model (BusinessClassifier_Modeling.ipynb)
2. Spark Job Submission Scripts (bin folder)
3. Spark Parquet Output File Analysis (Spark_Data_Analysis.ipynb)

*Note: These are command line arguments for local testing as well as deploying to AWS*

**Local Testing**

`
spark-submit ./business_classifier.py \
    --num_output_partitions 1 --log_level WARN \
    ./input/all_wet_CC-MAIN-2017-13.txt business_classifier
`

**STEP 1**  
Take a 1% sample of the February 2020 Common Crawl WET (text) file (~25M web pages out of 2.6B)

`
shuf -n 560 ./bin/wet.paths-02-2020 > ./bin/wet.paths-2020-sample
`

**STEP 2** 
Login to AWS (need to pip install aws cli - https://pypi.org/project/awscli/)

`
aws configure
	-key
	-pass
	-Availability Zone: us-east-1a
`

**STEP 3**  
Run the 1% sample using Spark in AWS EMR. Setup for 4 RDD partitions across 5 nodes (1 master 4 slave). 

Cost is ~$1/hour (4 nodes * $0.25/hr), takes about 2 days to complete.

Ideally I would run this on a 100 node instance but Amazon could not up my limit quickly enough. Best practice is to have the number of cores in the cluster match your partitions. With 100 nodes i would have had 4 cores per server across 100 servers, so 400 RDD partitions are needed in order to optimize our run.

`
./aws-submit ./wet.paths-2020-sample my-common-crawl-project subnet-XXXXXXXX 4 4 5
`

**STEP 4** 
Download the completed Parquet files and review the output

`
aws s3 cp s3://my-common-crawl-project/output/business_classification ./ --recursive
`
