package com.danosoftware.spark.interfaces;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * Spark processor interface. Implementations will process supplied RDD and pair
 * RDD of a specified type.
 * 
 * @author Danny
 *
 * @param <T>
 * @param <K>
 * @param <V>
 */
public interface ISparkProcessorSingleAndPair<T, K, V> extends Serializable
{
    /**
     * Process supplied RDD.
     * 
     * @param rdd
     */
    public void process(JavaRDD<T> rdd, JavaPairRDD<K, V> rddPair);
}