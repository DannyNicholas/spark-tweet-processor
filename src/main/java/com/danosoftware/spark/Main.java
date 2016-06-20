package com.danosoftware.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class that creates an instance of a tweet processor and begins
 * processing the wanted tweet and phrases file.
 * 
 * @author Danny
 */
public class Main {
	private static Logger logger = LoggerFactory.getLogger(Main.class);

	private static final String TWEET_FILE = "/Users/Danny/spark/tweets.json";
	private static final String PHRASES_FILE = "/Users/Danny/spark/phrases.txt";

	public static void main(String[] args) {

		logger.info("Starting Tweet Spark Processing.");

		SparkConf conf = new SparkConf().setAppName("Tweet Count").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		TweetProcessor processor = new TweetProcessor(TWEET_FILE, PHRASES_FILE);
		processor.process(sc);

		logger.info("Completed Tweet Spark Processing.");
	}
}
