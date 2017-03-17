package com.cib.hadoop.dataloadertest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class DecodingFunction implements 
		Function<Tuple2<LongWritable,Text>, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2227784643552969546L;
	private String charsetName;
	
	public DecodingFunction(String charset){
		charsetName = charset;
	}

	public String call(Tuple2<LongWritable, Text> line) throws Exception {
		
		return new String(line._2.getBytes(),charsetName);
	}
}
