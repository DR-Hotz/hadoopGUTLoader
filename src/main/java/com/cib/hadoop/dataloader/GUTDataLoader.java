package com.cib.hadoop.dataloader;

import com.cib.hadoop.dataloader.util.MetaStore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.*;

/**
 * Load the data to hadoop cluster
 * Current loader only support loading GUT data as parquet file. 
 * 
 * @version 0.1
 * 
 * @author Zhang Qi
 *
 */
public class GUTDataLoader implements DataLoad {
	
	private MetaStore metaStore;
	
	public GUTDataLoader(String source, String gutS_path, String loadS_path ){
		
	
	}
	

	public void load(String source, String from, String to) {
		
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("local[4]");
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		SQLContext sqlctx = new SQLContext(ctx);
		
		
		
		
	}
	
	

}
