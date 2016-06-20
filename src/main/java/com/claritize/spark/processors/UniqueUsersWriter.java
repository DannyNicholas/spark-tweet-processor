package com.claritize.spark.processors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.claritize.spark.beans.Tweet;
import com.claritize.spark.interfaces.ISparkProcessor;

/**
 * Find and store the unique user IDs seen in an RDD of tweets.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class UniqueUsersWriter implements ISparkProcessor<Tweet>
{
    // output directory
    private final String uniqueUsersDirectory;

    public UniqueUsersWriter(String uniqueUsersDirectory)
    {
        this.uniqueUsersDirectory = uniqueUsersDirectory;
    }

    public void process(JavaRDD<Tweet> tweets)
    {
        /*
         * extract the user ids from the tweets
         */
        JavaRDD<String> userIds = tweets.map(new Function<Tweet, String>()
        {
            public String call(Tweet aTweet) throws Exception
            {
                return aTweet.getId();
            }
        });

        /*
         * find all unique users from the user ids.
         */
        JavaRDD<String> unqiueUsers = userIds.distinct();

        /*
         * Store all unique users in a text file. Repartition into a single
         * partition first to ensure all users go into a single file.
         */
        unqiueUsers.repartition(1).saveAsTextFile(uniqueUsersDirectory);
    }
}
