package com.cib.hadoop.dataloader.util;

import java.util.ArrayList;
import java.util.List;


/**
 * Main abstraction of the original GUT data schema
 * It contains necessary metadata of a specific GUT file
 * 
 * @author Zhang Qi
 *
 */

public class GUTSchema {
	
	private String sql; 
	private String name;
	private int columns;
	private List<String> column_names;
	private List<String> column_types;
	private List<Integer> column_lengths;
	private List<Integer> column_offsets;
	
	public GUTSchema(){
		// left blank intentionally.
	}
	
	public GUTSchema init(){
		if(column_names == null){
			column_names = new ArrayList<String>();
		}
		if(column_types == null){
			column_types = new ArrayList<String>();
		}
		if(column_lengths == null){
			column_lengths = new ArrayList<Integer>();
		}
		if(column_offsets == null){
			column_offsets = new ArrayList<Integer>();
		}
		return this;
	}
	
	public String getSql() {
		return sql;
	}
	public String getName() {
		return name;
	}
	public int getColumns() {
		return columns;
	}
	public List<String> getColumn_names() {
		return column_names;
	}
	public List<String> getColumn_types() {
		return column_types;
	}
	public List<Integer> getColumn_lengths() {
		return column_lengths;
	}
	public List<Integer> getColumn_offsets() {
		return column_offsets;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setColumns(int columns) {
		this.columns = columns;
	}
	
	/**
	 * @param cName column name
	 * @param cType column type
	 * @param cLength column length
	 * @param cOffset column offset
	 */
	public void add(String cName, String cType, int cLength, int cOffset){
		column_names.add(cName);
		column_types.add(cType);
		column_lengths.add(new Integer(cLength));
		column_offsets.add(new Integer(cOffset));
	}
	
	/**
	 * Reset this instance for re-usage.
	 * Do NOT have to invoke {@code init()} after {@code reset()}
	 */
	public void reset(){
		sql = null;
		name = null;
		columns = 0;
		column_names.clear();
		column_types.clear();
		column_lengths.clear();
		column_offsets.clear();
	}
}
