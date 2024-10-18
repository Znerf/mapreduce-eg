# MapReduce 

MapReduce is a distributed programming model designed to process large-scale datasets efficiently across a cluster of machines. It divides the work into two main phases: the Map phase, where input data is split and processed into key-value pairs, and the Reduce phase, which aggregates and processes these key-value pairs to generate the final result. Performance in MapReduce is measured by execution time and other parameters.

## Description

his codebase contains Java implementations for three key tasks: word count, word co-occurrence, and sentiment analysis. The word count module counts occurrences of each word, the word co-occurrence module identifies how frequently word pairs appear together, and the sentiment analysis module determines the sentiment of text as positive, negative, or neutral. These Java programs can be tested or executed using Python by leveraging tools such as subprocess calls 

## Getting Started

Some of the instructions are based on the following article:

 https://medium.com/@guillermovc/setting-up-hadoop-with-docker-and-using-mapreduce-framework-c1cd125d4f7b

### Dependencies

Before starting, install and make sure the following work on your system 
* Docker
* docker compose or docker-compose
* git

### Installation & Setup

1. Clone the hadoop docker repository to run the hadoop server
    - `git clone https://github.com/big-data-europe/docker-hadoop`
2. Then clone our repo
    - `git clone https://github.com/Znerf/mapreduce-eg.git`
3. `cd` into the docker hadoop directory
4. Start Docker Desktop
5. Run `docker-compose up -d`
6. Wait for the containers to boot up
7. Copy our repo into the namemode server
    - `cd` into to the directory that holds the mapreduce-eg folder
    - run `docker cp mapreduce-eg namenode:/tmp`
8. Enter the namenode server with `docker exec -it namenode bash`. This will give you the terminal for the server.
9. Other (possibly) needed commands
    - Run `hdfs dfs -mkdir -p /user/root`
    - Run `hdfs dfs -mkdir /user/root/input`

### Executing (Word Count) Program
Currently the only working example is the validation script for count. The validation scripts for CoOccurrence and Sentiment are functional for running an example, but do not validate the output for the time being. 

Once you are ready to run the program, make sure you are still in the shell for the server and navigate to `/tmp/mapreduce-eg/count`

`javac ValidateCount.java` to compile the validation program (will be precompiled in future).

`java ValidateCount` to run

The output should show that the hadoop job was completed successfully, then output the mapreduce word count result for all words in the test input text file. 

It will also run a simple calculation to determine the percent match to the "groundtruth" numbers that were preliminarily calculated by this online tool https://design215.com/toolbox/wordlist.php

The validation psuedocode for similarity is as follows:
```
Make map1 of ground-truth string from file (word:number of occurences)
Make map2 of result from mapreduce

for every word in map1
    if matching word in map2
        num1 and num2 for word in map1 and map2
        max = max(num1, num2)
        absdiff = absolute value (num1 - num2)
        similarity = (1 - (absDifference / max)) * 100;
        percentage += similarity;

overall similarity = percentage / total words
```

## Test datasets

A sample dataset is included in the /txt/ directory for validation purposes. 

## Overview
This Repo Contains 3 MapReduce operations:
1. Word Count
2. Word Cooccurance
3. Sentiment Count

## Testing each case
1. `/tmp/mapreduce-eg/count` -> `javac ValidateCount.java` -> `java ValidateCount`
2. `/tmp/mapreduce-eg/coocur` -> `javac ValidateCoOccur.java` -> `java ValidateCoOccur`
3. `/tmp/mapreduce-eg/sentiment`-> `javac ValidateSentiment.java` -> `java ValidateSentiment`

You only have to run `javac` once per `*.java` file. 

## Calculating Performance
This will give you loading time, mapreduce time and unloading time. For the running of this, one must add 4.txt,8.txt,12.txt and 16.txt in txt folder.

```
python3 count/performance.py
python3 cooccur/performance.py
python3 sentiment/performance.py

```
Add some txt file in input folder. To get individual MapReduce element performance, use 
```
hadoop jar wordoccur.jar WordCoOccurrence /input /output2
```

**NOTE**: Did not get to converting and debugging performance scripts into Java due to time constraint. Running python does not work in this docker environment due to the debian version being outdated and not allowing `apt-get install` of python. 

## Further
Python variation of code are ran in ubuntu system and mightnot work in other operating system. 

## Help

Contact the Authors for the support

## Authors

Contributors names and contact info
* Sagar Shrestha (github.com/znerf)
* Chelsea Jones
* Sriharsha
* Harsha
* Kaleb (github.com/kalash-76)

<<<<<<< HEAD
=======
## Disclaimer
This is tested on ubuntu local server and might not work other system. Tested negative on Apple M series laptop and windows 

Server spec:
* 4gb ram
* Intel i5 7th gen vpro
* no gpu

>>>>>>> main


