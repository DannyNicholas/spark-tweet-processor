package com.claritize.spark.processors;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.claritize.spark.beans.Tweet;
import com.claritize.spark.interfaces.ISparkProcessor;

/**
 * Simple class that counts and reports the number of tweets supplied.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class TweetCounter implements ISparkProcessor<Tweet>
{
    private static Logger logger = LoggerFactory.getLogger(TweetCounter.class);

    public void process(JavaRDD<Tweet> tweets)
    {
        // count the number of tweets
        Long count = tweets.count();

        logger.info("Number of tweets supplied is: {}.", count);
    }
}
