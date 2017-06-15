# Spark Tweet Processor

This project is a demonstration of a simple Apache Spark batch data processing task using a file of tweets.

In this demo a file of tweets collected during a World Cup are provided. These tweets are to be processed to answer various questions and understand the contents of the tweets better. The project contains two tweet processing algorithms

 - A simple algorithm to find the most common words
 - A more advanced algorithm to find useful insights on the tweets 

**NOTE**: This project is dependent on Spark and Gson projects. The simplest way to run this project is to import it as a Maven project into an IDE (such as IntelliJ) and run it from here.

### Running a Simple Tweet Processing Demo

The aim of the first demo is to find the most common words seen in the list of tweets.

The tweet processing demo can be found at `com.danosoftware.spark.demo.TweetProcessorDemo`. This will process the file of tweets held at `resources/tweets.json` as follows:

  - Extract the line of text from each tweet
  - Split out individual words
  - Create a count per unique word
  - Filter the words that appear most often
  - Print the most common words

Run the main method of the above class to see the results.


### Running the Advanced Tweet Processing Algorithm

TBC...