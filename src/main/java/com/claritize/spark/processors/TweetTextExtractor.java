package com.claritize.spark.processors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.claritize.spark.beans.Tweet;
import com.claritize.spark.interfaces.ISparkTransformer;

/**
 * Transforms a RDD of tweets into a RDD of strings representing the tweet's
 * text.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class TweetTextExtractor implements ISparkTransformer<Tweet, String>
{

    @Override
    public JavaRDD<String> transform(JavaRDD<Tweet> tweets)
    {
        /*
         * Extract the tweet text from the tweet
         */
        JavaRDD<String> tweetText = tweets.map(new Function<Tweet, String>()
        {
            public String call(Tweet v1) throws Exception
            {
                return v1.getText();
            }
        });

        return tweetText;
    }

}
