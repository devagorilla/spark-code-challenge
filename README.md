# spark-code-challenge

The code is built from this Spark Example Project
[![Build Status](https://travis-ci.org/snowplow/spark-example-project.png)](https://travis-ci.org/snowplow/spark-example-project)

## Introduction

This is a simple CSV to JSON job written in Scala for the [Spark]
User has choice to write the result to a local txt file or publish the result to a Kafka cluser

## Set up Kafka on Windows without Cygwin
Below is the steps to running a single replication Kafka server on a Windows machine

http://www.codeproject.com/Articles/1068998/Running-Apache-Kafka-on-Windows-without-Cygwin

Fllow from step a to e to start a Zookeeper and Kafka and create a Kafka Topic using the following command

```bash
C:\kafka\kafka_2.11-0.9.0.1> kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```

Topic "test" will be used in the code challenge

## Building

[Vagrant] is pre-insalled in my Windows machine to host the [Spark] cluster

Under project folder execute the following

```bash
C:\spark-code-challenge>vagrant up
```

Using Putty to SSH to the Vagrant VM

127.0.0.1 port 2222

with vagrant:vagrant credential

Once in the guest VM

```bash
guest> cd /vagrant
guest> sbt assembly
```

The 'fat jar' is now available as:

    target/spark-code-challenge-0.3.0.jar

## Unit testing

The `assembly` command above runs the test suite - but you can also run this manually with:

    $ sbt test
    <snip>
    [info] CSVToJsonTest
	[info]
	[info] A Conversion job should
	[info] + Parse correctly
	[info]
	[info]
	[info] Total for specification CSVToJsonTest
	[info] Finished in 40 ms
	[info] 1 example, 0 failure, 0 error
	[info]
	[info] Passed: Total 1, Failed 0, Errors 0, Passed 1
	
## Running

The App takes 4 arguments
input file
output file
enable/disable file dump
enable/disable kafka dump


