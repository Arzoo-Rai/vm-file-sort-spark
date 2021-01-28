
package com.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.AnalysisException;



public class ViewingFigures {

  public static void main(String[] args) throws AnalysisException {

	  SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	  SparkSession spark  = SparkSession.builder()
			    .master("local[1]")
			    .config(conf)
			    .appName("Fsfsdf")
			    .getOrCreate();

	  Dataset<Row> df = spark.read().csv("E:\\arzoo_3gb_file.csv");
	  df.show();
	  df.printSchema();
	  df.groupBy("_c1").count().show();
	  df.createOrReplaceTempView("monu");

	  Dataset<Row> sqlDF = spark.sql("SELECT * FROM monu order by _c4");
	  sqlDF.show();
	
  }


}
