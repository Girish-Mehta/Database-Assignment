package DBEngine.DBEngine;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;

import javax.tools.DocumentationTool.Location;

public class DataBaseManager {

	private ArrayList<String> data;
	private ArrayList<String> dataType;
	private ArrayList<String> fields;
	private ArrayList<String> header;
	private ArrayList<String> result;
	
	public DataBaseManager() {
		this.data = new ArrayList<String>();
		this.dataType = new ArrayList<String>();
		this.fields = new ArrayList<String>();
		this.header = new ArrayList<String>();
		this.result = new ArrayList<String>();
	}
	
	public boolean executeQuery(QueryParameter queryParameter) throws Exception{
		String[] paramvalue = null;
		String parameter = "";
		String value = "";
		
		if(queryParameter.getFileName() == null)
			return false;
		else if(queryParameter.getFields() == null)
			return false;
		
		try{
			//LoadFields()
			loadFields(queryParameter.getFileName());
			//LoadDataTypes()
			loadDataTypes();
		} catch(FileNotFoundException e){
			throw e;
		}
		
		// get rows in array list
		loadData();
		
		// group by the result
		for(String field: queryParameter.getGroups()){
			sortData(field);
		}
				
		// order by the result
		for(String field: queryParameter.getOrder()){
			sortData(field);
		}
		
		// TODO code for 'and' 'or'
			if(queryParameter.getOperators().contains("or"))
				this.result = this.data;

		// filter result
		for(String condition: queryParameter.getConditions()) {
			if(condition.contains(" <= ")) {
				paramvalue = condition.split(" <= ");
				parameter = paramvalue[0]; 
				value = paramvalue[1];
				if(queryParameter.getFilter().indexOf(condition) == (queryParameter.getFilter().indexOf(" or ")+4)) {
					loadData();
					processCondition(parameter, value, "<=",this.result);					
				}
				else
					processCondition(parameter, value, "<=",this.data);
			} else if(condition.contains(" >= ")) {
				paramvalue = condition.split(" >= ");
				parameter = paramvalue[0]; 
				value = paramvalue[1];
				if(queryParameter.getFilter().indexOf(condition) == (queryParameter.getFilter().indexOf(" or ")+4)) {
					loadData();
					processCondition(parameter, value, ">=", this.result);					
				}
				else
					processCondition(parameter, value, ">=", this.data);				
			} else if(condition.contains(" < ")) {
				paramvalue = condition.split(" < ");
				parameter = paramvalue[0]; 
				value = paramvalue[1];
				if(queryParameter.getFilter().indexOf(condition) == (queryParameter.getFilter().indexOf(" or ")+4)) {
					loadData();
					processCondition(parameter, value, "<", this.data);
				}
				else
					processCondition(parameter, value, "<", this.result);				
			} else if(condition.contains(" > ")) {
				paramvalue = condition.split(" > ");
				parameter = paramvalue[0]; 
				value = paramvalue[1];
				if(queryParameter.getFilter().indexOf(condition) == (queryParameter.getFilter().indexOf(" or ")+4)) {
					loadData();
					processCondition(parameter, value, ">", this.result);					
				}
				else
					processCondition(parameter, value, ">", this.data);				
			} else if(condition.contains(" = ")) {
				paramvalue = condition.split(" = ");
				parameter = paramvalue[0]; 
				value = paramvalue[1];
				if(queryParameter.getFilter().indexOf(condition) == (queryParameter.getFilter().indexOf(" or ")+4)) {
					loadData();
					processCondition(parameter, value, "=", this.result);
				}
				else
					processCondition(parameter, value, "=", this.data);		
			} 
			
			/* TODO		
			 else if(condition.toLowerCase().contains(" in(")) {
			 	paramvalue = condition.toLowerCase().split(" in(");
				parameter = paramvalue[0]; 
				value = paramvalue[1];
				processCondition(parameter, value, "in");				
			 else if(condition.toLowerCase().contains(" between(")) {
			 	paramvalue = condition.toLowerCase().split(" between( ");
				parameter = paramvalue[0]; 
				value = paramvalue[1];
				processCondition(parameter, value, "btw");				
			 */

			} 
			
		// TODO		
		// perform function operations
		for(String function: queryParameter.getFunctions()) {
			if(function.contains("min(")) {
				parameter = "min";
				value = function.substring(function.indexOf("(")+1, function.indexOf(")"));
				processFunction(value, parameter);
			}
			else if(function.contains("max(")) {
				parameter = "max";
				value = function.substring(function.indexOf("(")+1, function.indexOf(")"));
				processFunction(value, parameter);
			}
			else if(function.contains("avg(")) {
				parameter = "avg";
				value = function.substring(function.indexOf("(")+1, function.indexOf(")"));
				processFunction(value, parameter);
			}
			else if(function.contains("sum(")) {
				parameter = "sum";
				value = function.substring(function.indexOf("(")+1, function.indexOf(")"));
				processFunction(value, parameter);
			}
			else if(function.contains("count(")) {
				parameter = "count";
				value = function.substring(function.indexOf("(")+1, function.indexOf(")"));
				processFunction(value, parameter);
			}
		}
		
		// get required fields
		filterFields(queryParameter.getFields());
		
		
		for(String row: this.result) {
			System.out.println(row);
		}
		
		// load the output in JSON file
		try{
			loadOutput();
		} catch(Exception e) {
			throw e;
		}
				
		return true;
	}

	private void loadFields(String fileName) throws FileNotFoundException{
		// create object
		FileManager filemanager = new FileManager();
		try{
			// load file
			filemanager.load(fileName);
		} catch(FileNotFoundException e){
			throw e;
		}
		// read first row and save column names in list
		this.fields = filemanager.getFirstRow();
	}

	private void loadDataTypes() {
		String column = "";

		Iterator<String> iterator = fields.iterator();
		while(iterator.hasNext()){
			column = iterator.next();
			try{
				Date date = new Date(column);
				dataType.add("Date");
			} catch(Exception e){
				try{
					Integer num = (Integer)Integer.parseInt(column);
					dataType.add("Number");
				} catch(Exception ee){
					String string = column;
					dataType.add("String");
				}
			}
		}
	}

	
	private void loadData() {
		// create file manager object
		FileManager filemanager = new FileManager();
		// get every column in an arrayList
		this.data = filemanager.getRows();		
	}
	
	private void sortData(String field) {
		// variable to find location of field
		int looper = 0;
		// variable to store location of field 
		final int pos;

		// iterate every columns and find the right column
		for(String col: this.fields) {
			if(field.equalsIgnoreCase(col)) {
				break;
			}
			looper++;
		}
		// assign the position of column required
		pos = looper;
		
		// sort the list based upon the value at the location
		Collections.sort(this.data, new Comparator<String>() {

			@Override
			public int compare(String r1, String r2) {
				String[] cols1 = r1.split(",");
				String[] cols2 = r2.split(",");
			
				return cols1[pos].compareToIgnoreCase(cols2[pos]);
			}
		});
	
	}
	
	private void processCondition(String param, String val, String op, ArrayList<String> dataSet) {
		// variable to store the value to be checked with
		int value = 0;
		try{
			value = Integer.parseInt(val);
		} catch(Exception e) {
			
		}
		// list to store row location to remove from the result list
		ArrayList<String> locations = new ArrayList<String>(dataSet.size());
		// variables to store positions
		int pos = 0;
		
		// find the position of column to match
		for(String col: this.fields) {
			if(col.equalsIgnoreCase(param))
				break;
			pos++;
		}
		
		switch(op) {
		case "<=":
			// evaluate the condition for required column in every row
			for(String row: dataSet) {
				String[] cols = row.split(",");
				// if condition is not met then add the row to the deletion list
				if(!(Integer.parseInt(cols[pos]) <= value))
					locations.add(row);
			}
			break;
		case ">=":
			// evaluate the condition for required column in every row
			for(String row: dataSet) {
				String[] cols = row.split(",");
				// if condition is not met then add the row to the deletion list
				if(!(Integer.parseInt(cols[pos]) >= value))
					locations.add(row);
			}
			break;
		case "<":
			// evaluate the condition for required column in every row
			for(String row: dataSet) {
				String[] cols = row.split(",");
				// if condition is not met then add the row to the deletion list
				if(!(Integer.parseInt(cols[pos]) < value)) {
					locations.add(row);
				}
			}
			break;
		case ">":			
			// evaluate the condition for required column in every row
			for(String row: dataSet) {
				String[] cols = row.split(",");
				// if condition is not met then add the row to the deletion list
				if(!(Integer.parseInt(cols[pos]) > value))
					locations.add(row);
			}
			break;
		case "=":
			// evaluate the condition for required column in every row
			for(String row: dataSet) {
				String[] cols = row.split(",");
				// if condition is not met then add the row to the deletion list
				if(!(cols[pos].equals(val))) {
					locations.add(row);
				}				
			}
			break;
		default: break;
		}		

		if(dataSet == this.result) {
			// remove the rows that did not meet the condition
			for(String line: dataSet) {
				if(!(this.result.contains(line)))
					this.result.add(line);
			}
			dataSet.removeAll(locations);
		} else {
			// remove the rows that did not meet the condition
			for(String line: dataSet) {
				if(!(this.result.contains(line)))
					this.result.add(line);
			}
			this.result.removeAll(locations);		
			
		}
	}
	
	private void filterFields(ArrayList<String> columns) {
		// array list to store the positions required
		ArrayList<Integer> pos = new ArrayList<Integer>(columns.size());
		// variable to generate comma separated rows of required fields
		StringBuilder stringBuilder = new StringBuilder();
		
		if(columns.get(0).equals("*")) {
			this.header = this.fields;
			for(int looper = 0; looper < this.fields.size(); looper++)
				pos.add(looper);
		} else {
			for(String column: columns) {
				pos.add(this.fields.indexOf(column));
				this.header.add(column);				
			}			
		}

		// for every row
		for(String row: this.result) {
			// get columns
			String[] cols = row.split(",");
			// for every position required
			for(Integer loc: pos) {
				try {
					// append the required column
					stringBuilder.append(cols[loc]+",");					
				} catch(ArrayIndexOutOfBoundsException e) {
					stringBuilder.append(" ,");
				}
			}
			// remove the comma at the end
			stringBuilder.delete(stringBuilder.length(), stringBuilder.length());
			//save the row in the result array list
			this.result.set(this.result.indexOf(row), stringBuilder.toString());
//			this.result.add(stringBuilder.toString());
			// reset string builder value
			stringBuilder.delete(0, stringBuilder.length());
		}
	}
	
	// TODO
	private void processFunction(String column, String op) {
		switch(op) {
		case "min":
			break;
		case "max":
			break;
		case "sum":
			break;
		case "avg":
			break;
		case "count":
			break;
		}
	}
	

	
	private void loadOutput() throws Exception {
		// create file manager object
		FileManager filemanager = new FileManager();
		try {
			filemanager.createJSON(this.result, this.header);			
		} catch(Exception e) {
			throw e;
		}
	}
	
	// TODO: delete objects and clear memory
}
