package com.danosoftware.spark.processors;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.spark.interfaces.ISparkProcessorSingleAndPair;
import com.danosoftware.spark.utilities.FileUtilities;

import scala.Tuple2;

/**
 * Class that matches the supplied RDD of word counts with a RDD of phrases
 * looking for matches. For any matches the top N phrases are reported.
 * 
 * N is configured by the constructor argument 'maxRows'.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class PhraseMatcher implements ISparkProcessorSingleAndPair<String, String, Integer> {
	private static Logger logger = LoggerFactory.getLogger(PhraseMatcher.class);

	/*
	 * maximum number of phrases that will be written to the file.
	 */
	private final Integer maxRows;

	// directory for output file
	private final String fileName;

	public PhraseMatcher(Integer maxRows, String fileName) {
		this.maxRows = maxRows;
		this.fileName = fileName;
	}

	public void process(JavaRDD<String> phrases, JavaPairRDD<String, Integer> wordCounts) {

		/*
		 * In order to join our phrases to our word count, we need to make the
		 * phrases a paired RDD. Use a value of zero.
		 */
		JavaPairRDD<String, Integer> phrasesPair = phrases.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String aPhrase) throws Exception {
				return new Tuple2<String, Integer>(aPhrase, 0);
			}
		});

		/*
		 * Join the word counts to the phrases in order to find word counts that
		 * also exist in the list of wanted phrases.
		 */
		JavaPairRDD<String, Tuple2<Integer, Integer>> joined = wordCounts.join(phrasesPair);

		/*
		 * Re-map the values so we are only left with the wanted words and
		 * original counts per word.
		 */
		JavaPairRDD<String, Integer> joinedMappedValues = joined
				.mapValues(new Function<Tuple2<Integer, Integer>, Integer>() {
					public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
						return v1._1 + v1._2;
					}
				});

		/*
		 * We now need to sort in descending word count order. To do this we
		 * swap keys and values so the counts become the key. We can then sort
		 * by the counts in descending order.
		 */
		JavaPairRDD<Integer, String> sorted = joinedMappedValues
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
						return new Tuple2<Integer, String>(tuple._2, tuple._1);
					}
				}).sortByKey(false);

		/*
		 * Take the top N phrases and output to a file.
		 */
		List<Tuple2<Integer, String>> list = sorted.take(maxRows);

		/*
		 * Output top phrases to a file.
		 */
		List<String> rows = new ArrayList<>();
		for (Tuple2<Integer, String> match : list) {
			rows.add(match._2 + "," + match._1);
		}
		FileUtilities.appendText(fileName, rows);
	}
}
