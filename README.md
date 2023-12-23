# Pinterest_data_pipeline
real time &amp; historical Pinterest data pipeline simulation


At minimum, your README file should contain the following information:

    Project Title
    Table of Contents, if the README file is long
    A description of the project: what it does, the aim of the project, and what you learned
    Installation instructions
    Usage instructions
    File structure of the project
    License information


## Concepts learned/demonstrated
<ul>
<li>Using an EC2 instance via SSH, SSHFS and SCP.
<li>Configuration of Kafka topics via command line.
<li>Basic MSK configuration in the AWS console.
<li>Use of Databricks to:
    <ul>
        <li>Connect to AWS S3 bucket
        <li>Load a series of files into a Spark dataframe
        <li>Using PySpark to clean the data and provide basic query results
    </ul>
    Of note is that Databricks appears to run extremely slow. This is apparently due to the shared nature of the AI Core account; however, it can be seen that syntax checking is generally quick and processing of dataframes appears to be lazy loaded as the serious delays only happen when displaying the data even if the dataframe only has a few rows.
</ul>

## Installation Instructions
install Java JRE 8
install Kafka
install aws component
### Templates
In the templates folder, there are templates for the configuration files which need to be in place first. These are:
<ul>
<li>
<li>
</ul>
### Set-up scripts
With the security credentials filled in, in the set-up folder
<ul>
<li>setup_topics_and_connector.sh
</ul>

## Usage Instructions
<br>


### Optional Parameters
<hr>

[![License](https://img.shields.io/badge/License-Boost_1.0-lightblue.svg)](https://www.boost.org/LICENSE_1_0.txt)
