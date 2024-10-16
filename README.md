# MapReduce 

MapReduce is a distributed programming model designed to process large-scale datasets efficiently across a cluster of machines. It divides the work into two main phases: the Map phase, where input data is split and processed into key-value pairs, and the Reduce phase, which aggregates and processes these key-value pairs to generate the final result. Performance in MapReduce is measured by execution time and other parameters.

## Description

his codebase contains Java implementations for three key tasks: word count, word co-occurrence, and sentiment analysis. The word count module counts occurrences of each word, the word co-occurrence module identifies how frequently word pairs appear together, and the sentiment analysis module determines the sentiment of text as positive, negative, or neutral. These Java programs can be tested or executed using Python by leveraging tools such as subprocess calls 

## Getting Started

### Dependencies

Before you start you need to install following on the platfrom
* Docker
* docker compose or docker-compose
* Python3
* git

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

Now get into the namenode server and use following commands. Make sure you have git and python installed on the server.
```
git clone https://github.com/Znerf/mapreduce-eg
cd mapreduce-eg
python3 count/test.py
```

## Test datasets

Testing .txt files are not included in this repo because of its sizes but will be provided upon request

## Overview
This document contain three mapreduce examples
1. Word Count
2. Word Cooccurance
3. Sentiment Count

## Testing each case

```
python3 count/test.py
python3 cooccur/test.py
python3 sentiment/test.py

```

## Calculating Performance
This will give you loading time, mapreduce time and unloading time

```
python3 count/performance.py
python3 cooccur/performance.py
python3 sentiment/performance.py

```
To get individual MapReduce element performance, use 
```
hadoop jar wordoccur.jar WordCoOccurrence /input /output2
```

## Help

Contact the Authors for the support

## Authors

Contributors names and contact info
* Sagar Shrestha (github.com/znerf)
* Chelsea Jones
* Sriharsha
* Harsha
* Kaleb

## Disclaimer
This is tested on ubuntu local server and might not work on Apple owned ARM based operating systems. 

Server spec:
* 4gb ram
* Intel i5 7th gen vpro
* no gpu



