package com.claritize.spark.interfaces;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

/**
 * Spark processor interface. Implementations will transform supplied RDD of a
 * specified type and return a RDD of another specified type.
 * 
 * @author Danny
 *
 * @param <T>
 * @param <R>
 */
public interface ISparkTransformer<T, R> extends Serializable
{
    /**
     * Transform supplied RDD of type <T> and return RDD od type <R>
     * 
     * @param rdd
     * @return
     */
    public JavaRDD<R> transform(JavaRDD<T> rdd);
}