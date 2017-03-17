package com.cib.hadoop.dataloader.util;

import java.io.FileNotFoundException;
import java.io.FileReader;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * Main abstraction for the DataLoader to grab any meta info of the data to be loaded.
 * @author Zhang Qi
 *
 */

public class MetaStore {
	
	private String tabName;
	private String gutPath;
	private String loadPath;
	private GUTSchema gutSchema = null;
	private DataLoadSchema loadSchema = null;
	
	public MetaStore(String tab_name){
		
		this.tabName = tab_name;
	}
	
	public void init(){
		if( gutSchema != null && loadSchema != null ){
			return;
		}
		Gson gson = new Gson();
		JsonParser parser = new JsonParser();
		
		try{
			gutSchema = gson.fromJson(
					parser.parse(new FileReader(gutPath))
					, GUTSchema.class );
			loadSchema = gson.fromJson(parser.parse(new FileReader(loadPath))
					, DataLoadSchema.class);
			
		}catch(FileNotFoundException fne){
			System.err.println("GUT Schema or Load Schema file not found!");
			System.exit(1);
		}catch(JsonIOException jie){
			System.err.println("json file IO exception");
			System.exit(1);
		}catch(JsonSyntaxException jse){
			System.err.println("json file syntax error, please check the json file");
			System.exit(1);
		}
		
	}

	public String getTabName() {
		return tabName;
	}
	
	
	
	
	
	
	

}
