package com.claritize.spark;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.claritize.spark.beans.Tweet;
import com.claritize.spark.interfaces.ISparkProcessor;
import com.claritize.spark.interfaces.ISparkProcessorPair;
import com.claritize.spark.interfaces.ISparkTransformer;
import com.claritize.spark.interfaces.ISparkTransformerPair;
import com.claritize.spark.processors.NGramGenerator;
import com.claritize.spark.processors.PhraseMatcher;
import com.claritize.spark.processors.StringCountCsvWriter;
import com.claritize.spark.processors.StringCounter;
import com.claritize.spark.processors.TextSplitter;
import com.claritize.spark.processors.TweetCounter;
import com.claritize.spark.processors.TweetTextExtractor;
import com.claritize.spark.processors.UniqueUsersWriter;
import com.claritize.spark.utilities.ParseJson;

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
public class TweetProcessor implements Serializable
{
    /*
     * When matching tweeted words to phrasrs. How many top N phrases to store
     */
    private static final int TOP_N_PHRASES = 10;

    /*
     * How many words per n-gram
     */
    private static final int WORDS_IN_N_GRAM = 5;

    /*
     * Directories for output files.
     */
    private static final String UNIQUE_IDS_DIRECTORY = "/Users/Danny/spark/user-ids/";
    private static final String N_GRAMS_DIRECTORY = "/Users/Danny/spark/ngram-count/";
    private static final String WORD_COUNT_DIRECTORY = "/Users/Danny/spark/word-count/";
    private static final String TOP_N_PHRASES_DIRECTORY = "/Users/Danny/spark/top-phrases/";

    // JSON file of tweets to process
    private final String tweetsFile;

    // text file of phrases to find matches for
    private final String phrasesFile;

    public TweetProcessor(String tweetsFile, String phrasesFile)
    {
        this.tweetsFile = tweetsFile;
        this.phrasesFile = phrasesFile;
    }

    public void process(JavaSparkContext sc)
    {
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
    private void countTweets(JavaRDD<Tweet> tweets)
    {
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
    private void uniqueUsers(JavaRDD<Tweet> tweets)
    {
        // store the unique users
        ISparkProcessor<Tweet> uniqueUsersWriter = new UniqueUsersWriter(UNIQUE_IDS_DIRECTORY);
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
    private JavaPairRDD<String, Integer> wordCounts(JavaRDD<String> tweetText)
    {
        // split the text into individual words
        ISparkTransformer<String, String> textSplitter = new TextSplitter();
        JavaRDD<String> tweetWords = textSplitter.transform(tweetText);

        // count the instances of each word
        ISparkTransformerPair<String, String, Integer> wordCounter = new StringCounter();
        JavaPairRDD<String, Integer> wordCounts = wordCounter.transform(tweetWords);

        // Store the unique words and counts seen in the tweets
        ISparkProcessorPair<String, Integer> tweetWordCountWriter = new StringCountCsvWriter(WORD_COUNT_DIRECTORY);
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
    private void topPhrases(JavaRDD<String> inputPhrases, JavaPairRDD<String, Integer> wordCounts)
    {
        /*
         * Store the top N matching phrases
         */
        PhraseMatcher phraseMatcher = new PhraseMatcher(TOP_N_PHRASES, TOP_N_PHRASES_DIRECTORY);
        phraseMatcher.process(inputPhrases, wordCounts);
    }

    /**
     * Count the instances of each n-grams seen in the tweets.
     * 
     * @param tweetText
     *            - tweets text to process
     */
    private void createNGrams(JavaRDD<String> tweetText)
    {
        // transform each text string into n-grams (N is configurable)
        ISparkTransformer<String, String> nGramGenerator = new NGramGenerator(WORDS_IN_N_GRAM);
        JavaRDD<String> nGrams = nGramGenerator.transform(tweetText);

        // count the instances of each n-gram seen
        ISparkTransformerPair<String, String, Integer> stringCounter = new StringCounter();
        JavaPairRDD<String, Integer> nGramCounts = stringCounter.transform(nGrams);

        // write the n-grams and counts to a CSV file
        StringCountCsvWriter writer = new StringCountCsvWriter(N_GRAMS_DIRECTORY);
        writer.process(nGramCounts);
    }
}
