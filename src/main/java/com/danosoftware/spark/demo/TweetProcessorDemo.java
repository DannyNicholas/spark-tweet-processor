package com.danosoftware.spark.demo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.spark.beans.Tweet;
import com.danosoftware.spark.utilities.ParseJson;

import scala.Tuple2;

/**
 * Processor that executes all the computations required for the JSON file of
 * tweets.
 * 
 * Processor completes the following steps:
 * 
 * 1) Reports the count of all tweets in file.
 * 
 * 2) Splits tweets into single words.
 * 
 * 3) Counts the number of distinct words.
 * 
 * 4) Filters high frequency words.
 *
 */
public class TweetProcessorDemo {

	private static Logger logger = LoggerFactory.getLogger(TweetProcessorDemo.class);

	/*
	 * Find location of tweet file in resources
	 */
	private static final String TWEET_FILE;
	static {
		TWEET_FILE = TweetProcessorDemo.class.getClassLoader().getResource("tweets.json").toString();
	}

	/**
	 * Start the Spark Processing
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		logger.info("Starting Tweet Spark Processing.");
		long timeStarted = System.nanoTime();

		TweetProcessorDemo processor = new TweetProcessorDemo();
		processor.process();

		long timeFinished = System.nanoTime();
		logger.info("Completed Tweet Spark Processing.");
		logger.info("Duration '{}' milliseconds.", (timeFinished - timeStarted) / 1_000_000);
	}

	/**
	 * Process the file of tweets
	 * 
	 */
	public void process() {

		SparkConf conf = new SparkConf().setAppName("Tweet Count").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/*
		 * Load and parse the tweet file into a RDD
		 */
		JavaRDD<String> inputTweets = sc.textFile(TWEET_FILE);
		JavaRDD<Tweet> tweets = inputTweets.mapPartitions(new ParseJson());

		/*
		 * Count the number of tweets in the file
		 */
		Long count = tweets.count();
		logger.info("Number of tweets supplied is: {}.", count);

		/*
		 * Extract the contents of each tweet
		 */
		JavaRDD<String> sentences = tweets.map(tweet -> tweet.getText());
		log(sentences);

		/*
		 * Split each sentence into words
		 */
		JavaRDD<String> words = sentences.flatMap(sentance -> Arrays.asList(sentance.split("\\s+")));
		log(words);

		/*
		 * Create a pair of each word and a count
		 */
		JavaPairRDD<String, Long> wordCount = words.mapToPair(word -> new Tuple2<>(word, 1L));
		log(wordCount);

		/*
		 * Reduce the counts by the word key
		 */
		JavaPairRDD<String, Long> reducedWordCount = wordCount.reduceByKey((count1, count2) -> count1 + count2);
		log(reducedWordCount);

		/*
		 * Filter the high frequency words
		 */
		JavaPairRDD<String, Long> highFreqWords = reducedWordCount.filter(countedWord -> (countedWord._2 > 1000));
		log(highFreqWords);

		// close the spark context
		sc.close();
	}

	/**
	 * Log a RDD of strings
	 * 
	 * @param strings
	 */
	private void log(JavaRDD<String> strings) {
		for (String aString : strings.collect()) {
			logger.info(aString);
		}
	}

	/**
	 * Log of RDD Pairs containing <String, Long>
	 * 
	 * @param pairs
	 */
	private void log(JavaPairRDD<String, Long> pairs) {
		for (Tuple2<String, Long> aPair : pairs.collect()) {
			logger.info(aPair._1 + ":" + aPair._2);
		}
	}
}
