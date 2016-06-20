package com.danosoftware.spark;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.danosoftware.spark.beans.Tweet;
import com.danosoftware.spark.interfaces.ISparkProcessor;
import com.danosoftware.spark.interfaces.ISparkProcessorPair;
import com.danosoftware.spark.interfaces.ISparkTransformer;
import com.danosoftware.spark.interfaces.ISparkTransformerPair;
import com.danosoftware.spark.processors.NGramGenerator;
import com.danosoftware.spark.processors.PhraseMatcher;
import com.danosoftware.spark.processors.StringCountCsvWriter;
import com.danosoftware.spark.processors.StringCounter;
import com.danosoftware.spark.processors.TextSplitter;
import com.danosoftware.spark.processors.TweetCounter;
import com.danosoftware.spark.processors.TweetTextExtractor;
import com.danosoftware.spark.processors.UniqueUsersWriter;
import com.danosoftware.spark.utilities.FileUtilities;
import com.danosoftware.spark.utilities.ParseJson;

/**
 * Processor that executes all the computations required for the JSON file of
 * tweets.
 * 
 * Processor completes the following steps:
 * 
 * 1) Reports the count of all tweets in file.
 * 
 * 2) Stores all unique users to a file.
 * 
 * 3) Stores a count of all unique words seen.
 * 
 * 4) Matches the words to a list of phrases and stores the top N phrases.
 * 
 * 5) Stores the counts of each N-gram seen.
 * 
 * Can be configured by a number of static constants. Future development may
 * require configuration via a properties instance.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class TweetProcessor implements Serializable {
	/*
	 * When matching tweeted words to phrasrs. How many top N phrases to store
	 */
	private static final int TOP_N_PHRASES = 10;

	/*
	 * How many words per n-gram
	 */
	private static final int WORDS_IN_N_GRAM = 5;

	/*
	 * Paths for output files.
	 */
	private static final String RESULTS_DIRECTORY = "/Users/Danny/spark/results/";
	private static final String UNIQUE_IDS_FILENAME = RESULTS_DIRECTORY + "unique-users.txt";
	private static final String N_GRAMS_FILENAME = RESULTS_DIRECTORY + "ngram-count.csv";
	private static final String WORD_COUNT_FILENAME = RESULTS_DIRECTORY + "word-count.csv";
	private static final String TOP_N_PHRASES_FILENAME = RESULTS_DIRECTORY + "top-phrases.csv";

	// JSON file of tweets to process
	private final String tweetsFile;

	// text file of phrases to find matches for
	private final String phrasesFile;

	public TweetProcessor(String tweetsFile, String phrasesFile) {
		this.tweetsFile = tweetsFile;
		this.phrasesFile = phrasesFile;

		// create results directory
		FileUtilities.createDirectory(RESULTS_DIRECTORY);

		// delete output files before starting
		FileUtilities.deleteIfExists(UNIQUE_IDS_FILENAME);
		FileUtilities.deleteIfExists(N_GRAMS_FILENAME);
		FileUtilities.deleteIfExists(WORD_COUNT_FILENAME);
		FileUtilities.deleteIfExists(TOP_N_PHRASES_FILENAME);
	}

	public void process(JavaSparkContext sc) {
		/*
		 * Load and parse the tweet file into a RDD
		 */
		JavaRDD<String> inputTweets = sc.textFile(tweetsFile);
		JavaRDD<Tweet> tweets = inputTweets.mapPartitions(new ParseJson());
		tweets.cache();

		/*
		 * Load the phrases file into a RDD
		 */
		JavaRDD<String> inputPhrases = sc.textFile(phrasesFile);

		/*
		 * Count the number of tweets in the file
		 */
		countTweets(tweets);

		/*
		 * Store the unique user IDs seen in the tweets
		 */
		uniqueUsers(tweets);

		/*
		 * extract the text from each tweet (i.e. the written string tweet
		 * itself)
		 */
		ISparkTransformer<Tweet, String> textExtractor = new TweetTextExtractor();
		JavaRDD<String> tweetText = textExtractor.transform(tweets);
		tweetText.cache();

		/*
		 * Get and store unique words and counts as an RDD Pair where word is
		 * the key and count is the value.
		 * 
		 * Cache the word counts as it will be used again.
		 */
		JavaPairRDD<String, Integer> wordCounts = wordCounts(tweetText);
		wordCounts.cache();

		/*
		 * Store the top N matching phrases
		 */
		topPhrases(inputPhrases, wordCounts);

		/*
		 * Count the n-grams seen in the tweets
		 */
		createNGrams(tweetText);
	}

	/**
	 * 
	 * @param tweets
	 *            - tweets to process
	 */
	private void countTweets(JavaRDD<Tweet> tweets) {
		/*
		 * Count the number of tweets in the file
		 */
		ISparkProcessor<Tweet> counter = new TweetCounter();
		counter.process(tweets);
	}

	/**
	 * Store the unique user IDs seen in the tweets
	 * 
	 * @param tweets
	 *            - tweets to process
	 */
	private void uniqueUsers(JavaRDD<Tweet> tweets) {
		// store the unique users
		ISparkProcessor<Tweet> uniqueUsersWriter = new UniqueUsersWriter(UNIQUE_IDS_FILENAME);
		uniqueUsersWriter.process(tweets);
	}

	/**
	 * Get and store unique words and counts as an RDD Pair where word is the
	 * key and count is the value.
	 * 
	 * @param tweetText
	 *            - tweets text to process
	 * @return count of each word
	 */
	private JavaPairRDD<String, Integer> wordCounts(JavaRDD<String> tweetText) {
		// split the text into individual words
		ISparkTransformer<String, String> textSplitter = new TextSplitter();
		JavaRDD<String> tweetWords = textSplitter.transform(tweetText);

		// count the instances of each word
		ISparkTransformerPair<String, String, Integer> wordCounter = new StringCounter();
		JavaPairRDD<String, Integer> wordCounts = wordCounter.transform(tweetWords);

		// Store the unique words and counts seen in the tweets
		ISparkProcessorPair<String, Integer> tweetWordCountWriter = new StringCountCsvWriter(WORD_COUNT_FILENAME);
		tweetWordCountWriter.process(wordCounts);

		return wordCounts;

	}

	/**
	 * Find the top n strings (configurable) from the tweet text that match the
	 * supplied phrases.
	 * 
	 * @param inputPhrases
	 *            - wanted phrases
	 * @param wordCounts
	 *            - unique strings and counts seen
	 */
	private void topPhrases(JavaRDD<String> inputPhrases, JavaPairRDD<String, Integer> wordCounts) {
		/*
		 * Store the top N matching phrases
		 */
		PhraseMatcher phraseMatcher = new PhraseMatcher(TOP_N_PHRASES, TOP_N_PHRASES_FILENAME);
		phraseMatcher.process(inputPhrases, wordCounts);
	}

	/**
	 * Count the instances of each n-grams seen in the tweets.
	 * 
	 * @param tweetText
	 *            - tweets text to process
	 */
	private void createNGrams(JavaRDD<String> tweetText) {
		// transform each text string into n-grams (N is configurable)
		ISparkTransformer<String, String> nGramGenerator = new NGramGenerator(WORDS_IN_N_GRAM);
		JavaRDD<String> nGrams = nGramGenerator.transform(tweetText);

		// count the instances of each n-gram seen
		ISparkTransformerPair<String, String, Integer> stringCounter = new StringCounter();
		JavaPairRDD<String, Integer> nGramCounts = stringCounter.transform(nGrams);

		// write the n-grams and counts to a CSV file
		StringCountCsvWriter writer = new StringCountCsvWriter(N_GRAMS_FILENAME);
		writer.process(nGramCounts);
	}
}
