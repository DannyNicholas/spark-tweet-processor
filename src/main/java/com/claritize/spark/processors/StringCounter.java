package com.claritize.spark.processors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.claritize.spark.interfaces.ISparkTransformerPair;

/**
 * Transfomer that takes a RDD of strings and applies a map reduce function to
 * return a RDD pair representing the unique strings and count of number of
 * times each was seen.
 * 
 * @author Danny
 */
@SuppressWarnings("serial")
public class StringCounter implements ISparkTransformerPair<String, String, Integer>
{

    @Override
    public JavaPairRDD<String, Integer> transform(JavaRDD<String> strings)
    {
        /*
         * Use Map-Reduce to the count the instances of each string. Uses a
         * PairRDD where the string is the key and the count is the value.
         */
        JavaPairRDD<String, Integer> stringCounts = strings.mapToPair(new PairFunction<String, String, Integer>()
        {
            public Tuple2<String, Integer> call(String word) throws Exception
            {
                // count 1 instance of this string
                return new Tuple2<String, Integer>(word, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            public Integer call(Integer count1, Integer count2) throws Exception
            {
                // combine string counts by adding
                return count1 + count2;
            }
        });

        return stringCounts;
    }

}
