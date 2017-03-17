package com.cib.hadoop.dataloadertest;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.junit.*;

import scala.Tuple2;



public class TestDataEncoding {

	private JavaSparkContext ctx;
	
	@Before
	public void init(){
		SparkConf conf = new SparkConf()
				.setAppName("EncodingTest")
				.setMaster("local");
		ctx = new JavaSparkContext(conf);
	}
	
	/**
	 *  This method won't work since textFile, the String in RDD
	 *  already has been encoded wrongly
	 */
	public void encodingTest() throws UnsupportedEncodingException{
		
		
		JavaRDD<String> lines = ctx.textFile("hbb.20160630.000000.0000.dat.gz");
		
		List<String> collect = lines.take(100);
		
		
		int i = 0;
		while( i < collect.size()){
			System.out.println(collect.get(i));
			i++;
		}
		
	}
	
	@Test
	public void GBKDecodingTest(){
		
		long start = System.currentTimeMillis();
		
		JavaPairRDD<LongWritable,Text> rawBytes = ctx.newAPIHadoopFile(
				"hbb.20160630.000000.0000.dat.gz"
				, TextInputFormat.class
				, LongWritable.class
				, Text.class
				, new Configuration());
		
		List<String> out = rawBytes.map(new DecodingFunction("GBK")).take(100);
		
		for(String line : out){
			System.out.println(line);
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Time Consuming : " + (start - end) + " ms");
	}
	
	
	@After
	public void last(){
		ctx.stop();
	}
}
