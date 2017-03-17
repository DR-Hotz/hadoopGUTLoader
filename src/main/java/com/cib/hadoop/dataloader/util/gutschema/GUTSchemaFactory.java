package com.cib.hadoop.dataloader.util.gutschema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.cib.hadoop.dataloader.util.GUTSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import java.util.regex.*;

public class GUTSchemaFactory{
	
	private static final String SQL = "SQL=";
	private static final String COLUMNCOUNT = "COLUMNCOUNT=";
	private static final String COLUMNDESP = "COLUMNDESCRIPTION=";
	
	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
	private static final JsonParser PARSER = new JsonParser();
	private static final GUTSchema SCHEMA = new GUTSchema().init();
	
	private static final Pattern NAME = Pattern.compile("\\s[fF][rR][oO][mM]\\s(\\S+)\\s");
	private static final Pattern COLUMNINFO = Pattern.compile("^\\d+$$([^$]+)$$");

	public static void jsonSchemaProducer(String flgFile, String toPath){
		
		BufferedReader br = null;
		BufferedWriter bw = null;
		try{
			br = new BufferedReader(new FileReader(flgFile));
			
			String line = null;
			while((line = br.readLine()) != null){
				
				if(line.startsWith(SQL)){
					SCHEMA.setSql(line.substring(4));
					SCHEMA.setName(getName(line));
					
				}else if(line.startsWith(COLUMNCOUNT)){
					SCHEMA.setColumns(Integer.parseInt(line.substring(12)));
					
				}else if(line.startsWith(COLUMNDESP)){
					parseColumnInfo(br);
				}
			}
			
			/**
			 * Got the GUTSchema Object after parsing
			 * Produce json format schema
			 */
			String file = toPath + SCHEMA.getName() + ".json";
			bw = new BufferedWriter(new FileWriter(file));
			
			
			
		}catch(FileNotFoundException fne){
			System.err.println("File " + flgFile + " Not Found!");
			System.exit(1);
		}catch(IOException ioe){
			System.err.println("Reading File " + flgFile + " error!");
		}catch(IllegalStateException ile){
			System.err.println("Match State incorrect when trying to parse file " + flgFile);
			System.out.println("Match error occur when parsing the file " 
					+ flgFile + ". This file has been skipped.");
			return;
		}catch(IndexOutOfBoundsException iobe){
			System.err.println("Group Match failed, the parsing of the file " 
					+ flgFile + " has been skipped!" );
			System.out.println("Group Match failed, the parsing of the file " 
					+ flgFile + " has been skipped!");
			return;
		}finally{
			close(br);
			close(bw);
		}
	}
	
	public static void batchJsonSchemaProducer(String fromPath, String toPath){
		
		
	}
	
	private static String getName(String sql){
		Matcher matcher = NAME.matcher(sql);
		return matcher.group(1);
	}
	
	private static String parseColumnInfo(BufferedReader br){
		return "";
	}
	
	private static void close(Closeable c){
		
		if(c == null){
			return;
		}
		try{
			c.close();
		}catch(IOException e){
			//do nothing
		}
	}
}
