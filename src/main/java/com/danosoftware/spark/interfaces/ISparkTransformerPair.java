package com.danosoftware.spark.interfaces;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * Spark processor interface. Implementations will transform supplied RDD of a
 * specified type and return a pair RDD of another specified type.
 * 
 * @author Danny
 *
 * @param <T>
 * @param <K>
 * @param <V>
 */
public interface ISparkTransformerPair<T, K, V> extends Serializable
{
    /**
     * Transform supplied RDD of type <T> and return RDD Pair with key <K> and
     * value <V>.
     * 
     * @param rdd
     * @return
     */
    public JavaPairRDD<K, V> transform(JavaRDD<T> rdd);
}