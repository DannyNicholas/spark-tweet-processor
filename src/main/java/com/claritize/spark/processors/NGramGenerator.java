package com.claritize.spark.processors;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.claritize.spark.interfaces.ISparkTransformer;

/**
 * Generates configurable n-grams from the RDD of strings.
 * 
 * For more details on n-grams see:
 * http://www.text-analytics101.com/2014/11/what-are-n-grams.html
 *
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class NGramGenerator implements ISparkTransformer<String, String>
{
    // number of words in each n-gram
    private final Integer N;

    public NGramGenerator(Integer n)
    {
        N = n;
    }

    @Override
    public JavaRDD<String> transform(JavaRDD<String> text)
    {
        JavaRDD<String> nGrams = text.flatMap(new FlatMapFunction<String, String>()
        {
            @Override
            public Iterable<String> call(String t) throws Exception
            {
                List<String> ngrams = new ArrayList<>();

                String[] tokens = t.split("\\s+");

                for (int k = 0; k < (tokens.length - N + 1); k++)
                {
                    StringBuilder s = new StringBuilder();
                    int start = k;
                    int end = k + N;
                    for (int j = start; j < end; j++)
                    {
                        if (s.length() > 0)
                        {
                            s.append(" ");
                        }
                        s.append(tokens[j]);
                    }
                    // Add n-gram to a list
                    ngrams.add(s.toString());
                }

                return ngrams;
            }
        });

        return nGrams;
    }
}
