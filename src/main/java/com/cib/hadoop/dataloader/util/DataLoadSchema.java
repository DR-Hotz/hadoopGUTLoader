package com.cib.hadoop.dataloader.util;

import java.util.List;

/**
 * Represents the actual schema of the data to be loaded rather than the GUT schema,
 * it's pretty common that the dataset loaded is a subset of the entire GUT dataset,
 * hence this class is a basic abstraction of a subset data.   
 * Also the definition of this subset data is json format.
 *
 * @author Zhang Qi
 *
 */

public class DataLoadSchema {

	/**
	 * column name array of the dataset to be loaded
	 */
	private List<String> loadArray;

	public List<String> getLoadArray() {
		return loadArray;
	}
	
}
