# MapReduce 

MapReduce is a distributed programming model designed to process large-scale datasets efficiently across a cluster of machines. It divides the work into two main phases: the Map phase, where input data is split and processed into key-value pairs, and the Reduce phase, which aggregates and processes these key-value pairs to generate the final result. Performance in MapReduce is measured by execution time and other parameters.

## Description

his codebase contains Java implementations for three key tasks: word count, word co-occurrence, and sentiment analysis. The word count module counts occurrences of each word, the word co-occurrence module identifies how frequently word pairs appear together, and the sentiment analysis module determines the sentiment of text as positive, negative, or neutral. These Java programs can be tested or executed using Python by leveraging tools such as subprocess calls 

## Getting Started

### Dependencies

Before you start you need to install following on your OS
* Docker
* docker compose or docker-compose
* Python

### Installing

You can use these commands 

```
docker compose up
```
or 

```
docker-compose up
```

This will install setup hadoop and hdfs system in your server

### Executing program

Now get into the namenode server and use following commands
```
git clone https://github.com/Znerf/mapreduce-eg
cd mapreduce-eg
python count/test.py
```
## Help

Testing .txt files are not included in this repo because of it's sizes but will be provided upon request

## Help

Contact the Authors for the support

## Authors

Contributors names and contact info
* Sagar Shrestha (github.com/znerf)
* Chelsea Jones
* Sriharsha
* Harsha
* Kaleb




