package com.danosoftware.spark.processors;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.danosoftware.spark.beans.Tweet;
import com.danosoftware.spark.interfaces.ISparkProcessor;
import com.danosoftware.spark.utilities.FileUtilities;

/**
 * Find and store the unique user IDs seen in an RDD of tweets.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class UniqueUsersWriter implements ISparkProcessor<Tweet> {

	// output file path
	private final String unqiueUsersFile;

	public UniqueUsersWriter(String unqiueUsersFile) {
		this.unqiueUsersFile = unqiueUsersFile;
	}

	public void process(JavaRDD<Tweet> tweets) {
		/*
		 * extract the user ids from the tweets
		 */
		JavaRDD<String> userIds = tweets.map(new Function<Tweet, String>() {
			public String call(Tweet aTweet) throws Exception {
				return aTweet.getId();
			}
		});

		/*
		 * find all unique users from the user ids.
		 */
		JavaRDD<String> unqiueUsers = userIds.distinct();

		/*
		 * Append all unique users to the text file.
		 */
		List<String> users = unqiueUsers.collect();
		FileUtilities.appendText(unqiueUsersFile, users);
	}
}
