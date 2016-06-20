package com.danosoftware.spark.processors;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.danosoftware.spark.interfaces.ISparkProcessorPair;
import com.danosoftware.spark.utilities.FileUtilities;

import scala.Tuple2;

/**
 * Class that writes to a CSV file with supplied strings and counts from a RDD
 * pair.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class StringCountCsvWriter implements ISparkProcessorPair<String, Integer> {
	// filename for string counts
	private final String fileName;

	public StringCountCsvWriter(String fileName) {
		this.fileName = fileName;
	}

	public void process(JavaPairRDD<String, Integer> wordCounts) {
		/*
		 * Create a CSV row of string and count
		 */
		JavaRDD<String> csvTweetCounts = wordCounts.map(new Function<Tuple2<String, Integer>, String>() {
			public String call(Tuple2<String, Integer> aWordCount) throws Exception {
				return aWordCount._1 + "," + aWordCount._2;
			}
		});

		/*
		 * Store all rows in a text file.
		 */
		List<String> tweetCounts = csvTweetCounts.collect();
		FileUtilities.appendText(fileName, tweetCounts);
	}
}
