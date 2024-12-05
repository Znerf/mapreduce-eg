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

1. In a terminal/command line, clone our repo
    - `git clone https://github.com/Znerf/mapreduce-eg.git`
4. Start Docker Desktop application
5. `cd` into the repo, then run `docker-compose up -d`
6. Wait for the containers to boot up
7. Copy our repo into the namemode server
    - `cd` into to the directory that holds the `mapreduce-eg` folder
    - run `docker cp mapreduce-eg namenode:/tmp`
8. Enter the namenode server with `docker exec -it namenode bash`. This will give you the terminal for the server.
9. cd into the `/tmp/mapreduce-eg` directory, run `bash setup.sh`
9. Other (possibly) needed commands
    - `hdfs dfs -mkdir -p /user/root`
    - `hdfs dfs -mkdir /user/root/input`

### Executing (Word Count) Program

Once you are ready to run the program, make sure you are still in the shell for the server and navigate to `/tmp/mapreduce-eg/count`

`javac ValidateCount.java` to compile the validation program.

`java ValidateCount` to run

The output should show that the hadoop job was completed successfully, then output the mapreduce word count result for all words in the test input text file and a similarity score between the expected and actual output. 

All other cases are executable with python (commands below)

## Test datasets

A sample dataset is included in the /txt/ directory for validation purposes. 

## Overview
This Repo Contains 6 MapReduce operations:
1. Word Count
2. Word Cooccurance
3. Sentiment Count
4. Max Sentence Length
5. Most Frequent Word
6. Word Search

## Validating each case
1. `python3 maxlength/validate.py`
2. `python3 mostfrequentword/validate.py`
3. `python3 sentiment/validate.py`
4. `python3 WordSearch/test.py` <- This one is different since it didn't make as much sense to make a validation script. The test uses the word "is" for search and is searching over "Hello world Hello Hadoop\nThis is a simple test\nHello again"

Each folder has a `test.py` you can execute as well to see it work.

NOTE: you must be in the parent mapreduce-eg directory for the python scripts to work properly.

## Calculating Performance
This will give you loading time, mapreduce time and unloading time. Uses the 4.txt, 8.txt, 12.txt, and 16.txt files in the `txt/` dir

```
python3 count/performance.py
python3 cooccur/performance.py
python3 sentiment/performance.py
...
```
To get individual MapReduce element performance, use 
```
hadoop jar wordoccur.jar WordCoOccurrence /input /output2
```

## Help
Sometimes the scripts do not remove certain files and will return an error. Try the following:
- `hdfs dfs -rm -r /tmp/hadoop_output`
- `hdfs dfs -rm -r /tmp/temp_input_file.txt`
Contact the Authors for support

## Authors

Contributors names and contact info
* Sagar Shrestha (github.com/znerf)
* Chelsea Jones
* Sriharsha
* Harsha
* Kaleb (github.com/kalash-76)

## Disclaimer
This is tested on ubuntu local server and might not work other system. Tested negative on Apple M series laptop and windows 

Server spec:
* 4gb ram
* Intel i5 7th gen vpro
* no gpu



