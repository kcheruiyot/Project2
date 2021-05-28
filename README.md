# Project2

## Project Description
Create a Spark Application that process Covid data.
Your project  should involve some analysis of covid data (Every concept of spark from rdd, dataframes, sql, dataset and optimization methods should be included, persistence also).  The final expected output is different trends that you have observed as part of data collectivley and how can WHO make use of these trends to make some useful decisions.

Lets the P2 Demo, have presentation with screen shots and practical demo for at least one of your trends.

## Technologies Used
  * Intellij - 2021.1
  * Ubuntu - 20.4
  * Hadoop - 3.3.0
  * Apache - Hive - 2.3.8
  * Apache Spark - 3.3.1
  * Scala - 2.12.3

## Features
List of features ready and TODOs for future development

  * User can run each trends code
  * Awesome feature 2
  * Awesome feature 3
  
  To-do list:

  * Wow improvement to be done 1
  * Wow improvement to be done 2

## Getting Started
GitHub clone URL: `https://github.com/kcheruiyot/Project2.git` 

  - Enable WSL and update to WSL2 on Windows 10
  - Install Java JDK 1.8 on Windows 10  
  - Install Ubuntu 18+
  - Install Java JDK 1.8 on Ubuntu  
  - Install Hadoop on Ubuntu
  - Install Apache-Hive on Ubuntu
  - Install Apache-Spark on ubuntu
  - install Intellij Community Edition 2021
  - Open Ubuntu Terminal:
    - `ssh localhost`
    - `~HADOOP_HOME/sbin/start-dfs.sh`
    - `~HADOOP_HOME/sbin/start-yarn.sh`  
    - `cd`
    - `mkdir /user/project2`
    - `hdfs dfs -cp /mnt/<path to data files>/covid_19_data.csv /user/project2/covid_19_data.csv`
    - `hdfs dfs -cp /mnt/<path to data files>/time_series_covid_19_confirmed.csv /user/project2/time_series_covid_19_confirmed.csv`
    - `hdfs dfs -cp /mnt/<path to data files>/time_series_covid_19_confirmed_US.csv /user/project2/time_series_covid_19_confirmed_US.csv`
    - `hdfs dfs -cp /mnt/<path to data files>/time_series_covid_19_deaths.csv /user/project2/time_series_covid_19_deaths.csv`
    - `hdfs dfs -cp /mnt/<path to data files>/time_series_covid_19_deaths_US.csv /user/project2/time_series_covid_19_deaths_US.csv`
    - `hdfs dfs -cp /mnt/<path to data files>/time_series_covid_19_recovered.csv /user/project2/time_series_covid_19_recovered.csv`
  - Clone project in intellij

## Usage
> Here, you instruct other people on how to use your project after they’ve installed it. This would also be a good place to include screenshots of your project in action.

## Contributors
- [Gary Larson](https://github.com/gary-larson)
> Todo add your name and link
## License
This project uses the following license: <license_name>.