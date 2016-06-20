package com.claritize.spark.processors;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.claritize.spark.interfaces.ISparkTransformer;

/**
 * Class that splits a RDD of text strings into RDD of individual words seen
 * across all text.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class TextSplitter implements ISparkTransformer<String, String>
{
    public JavaRDD<String> transform(JavaRDD<String> text)
    {
        /*
         * Split string of text into individual words
         */
        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>()
        {
            public Iterable<String> call(String text) throws Exception
            {
                // split text into individual words
                return Arrays.asList(text.split("\\s+"));
            }
        });

        return words;
    }
}
