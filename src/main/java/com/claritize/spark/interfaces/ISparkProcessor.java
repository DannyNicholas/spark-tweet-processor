package com.claritize.spark.interfaces;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

/**
 * Spark processor interface. Implementations will process supplied RDDs of a
 * specified type.
 * 
 * @author Danny
 *
 * @param <T>
 */
public interface ISparkProcessor<T> extends Serializable
{
    /**
     * Process supplied RDD.
     * 
     * @param rdd
     */
    public void process(JavaRDD<T> rdd);
}