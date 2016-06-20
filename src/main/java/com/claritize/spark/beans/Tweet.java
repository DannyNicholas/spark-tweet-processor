package com.claritize.spark.beans;

import java.io.Serializable;

/**
 * Class that represents a single tweet.
 * 
 * This is a limited implementation that only holds user IDs and tweet text.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class Tweet implements Serializable
{
    private String id;
    private String text;

    public String getId()
    {
        return id;
    }

    public String getText()
    {
        return text;
    }
}
