# Pinterest Data Pipeline
real time &amp; historical Pinterest data pipeline simulation

NOTE: Refactor & ReadMe documentation update in progress
<!--
  ?  Table of Contents, if the README file is long
  x  A description of the project: what it does, the aim of the project, and what you learned
  x  Installation instructions
    Usage instructions
    File structure of the project
-->
## What it does
The Pinterest Data Pipeline is takes Pinterest user generated data through an ETL process. This is handled in two ways. First through the processes of handling "legacy" data from an external database to an S3 data store for cleansing ad-hock reporting. Secondly, data is handled with data streams.
## Project aim
This project is an educational exercise for hands on big data gathering and processing (ETL) using common big data tools: 
Kafka, Spark, Airflow (both on prem &amp; in AWS WMAA), AWS Kinesis and DataBricks.

### Concepts learned/demonstrated
<ul>
<li>Using an EC2 instance via SSH, SSHFS and SCP.
<li>Local configuration of Kafka topics via command line.
<li>Basic MSK configuration in the AWS console.
<li>Kinesis stream connectivity with API Gateway
<li>Airflow in AWS - WMAA
<li>Use of Databricks to:
    <ul>
        <li>Connect to AWS S3 bucket
        <li>Load a series of files into a Spark dataframe
        <li>Connect to an AWS Kinesis data stream
        <li>Using PySpark to clean the data/streams and provide basic query results for static data
        <li>Store data transformed in DataBricks data lake
    </ul>
</ul>

## Architecture 
<img src="https://raw.githubusercontent.com/DanBachmann/Pinterest_data_pipeline/main/documentation_resources/AiCore%20Pinterest%20Pipeline.png">

## Report Request Output
Since this project is connected directly to Github via the DataBricks Repo feature, DataBricks removes all notebook output before committing to Github, the reporting output can be seen in [this snapshot of the notebook](DataBricks/Report_Request_Notebook_Output.html)

## Set-up
### On prem components
The on premise software for processing of legacy data plus initial exploratory testing was done on Ubuntu Linux using Conda to manage dependencies.
<br>The main components are: 
<ul>
    <li>Java JRE 8
    <li>Apache AirFlow
    <li>Apache Spark
    <li>Apache Kafka via Confluent
</ul>
For a detailed technical view of the supporting software used, see 
[this Conda environment export](documentation_resources/conda-env.yaml).

### Set-up script
With the software security credentials and other minor configurations set as per the AiCore Data Engineering notebook instructions, the topics for on premise legacy data topics can be created by the following script in the set_up folder:
<ul>
    <li>setup_topics_and_connector.sh
</ul>

## Legacy Data Population
Shell scripts are provided to prevent errors and to aid in returning to this process in the future.
To load the data in the legacy database into an S3 bucket using Kafka, execute the following on the EC2 instance within the VPC:
    
    1run_this_on_kafka_ec2.sh

This will execute the following commands:

    cd ~/confluent-7.2.0/bin/
    ./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties

The next script can be run on another computer, for example, a local one.

    2run_this_to_send_messages.sh 
    
This will execute the following:

    python3 user_posting_emulation.py





## Usage Instructions
<br>


[![License](https://img.shields.io/badge/License-Boost_1.0-lightblue.svg)](https://www.boost.org/LICENSE_1_0.txt)
