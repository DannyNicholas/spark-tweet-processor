package com.claritize.spark.interfaces;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * Spark processor interface. Implementations will process supplied pair RDDs of
 * a specified type.
 * 
 * @author Danny
 *
 * @param <K>
 * @param <V>
 */
public interface ISparkProcessorPair<K, V> extends Serializable
{
    /**
     * Process supplied RDD.
     * 
     * @param rdd
     */
    public void process(JavaPairRDD<K, V> rddPair);
}