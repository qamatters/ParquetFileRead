package com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHelper {
    public static void runBasicDataFrameExample(SparkSession spark)  {
        Dataset<Row> df = spark.read().parquet("data//userdata1.parquet");
//        df.show();
//        df.printSchema();
        df.createOrReplaceTempView("last_name");
        Dataset<Row> sqlDF = spark.sql("SELECT country FROM last_name");
        sqlDF.show();
    }
}

