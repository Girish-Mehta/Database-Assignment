package DBEngine.DBEngine;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
import java.util.Iterator;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

public class FileManager {
  private BufferedReader bufferedReader;
  private ArrayList<String> colList;
  private static String fileName;
  private FileReader fileReader;
  private FileWriter fileWriter;
  private ArrayList<String> rows;

  FileManager(){
    this.bufferedReader = null;
    this.colList = new ArrayList<String>();
    this.fileReader = null;
    this.fileWriter = null;
    this.rows = new ArrayList<String>();
  }

  public void load(String fileName) throws FileNotFoundException{
    try{
      // check if file exists
      fileReader = new FileReader(fileName);
      FileManager.fileName = fileName;
    } catch(FileNotFoundException e){
      // throw exception if file does not exist
      throw e;
    } 
  }

  public ArrayList<String> getFirstRow() throws FileNotFoundException{
    String[] columns = null;
    String row = "";
    try{
      // load file
      fileReader = new FileReader(FileManager.fileName);
      // initialize buffer reader to read lines
      bufferedReader = new BufferedReader(fileReader);
        while((row = bufferedReader.readLine()) != null){
          // split first line based on comma
          columns = row.split(",");
          for(String column:columns)
            // add every column to list
            colList.add(column);
          // exit after reading first row
          break;
        }
      } catch(FileNotFoundException e){
          throw e;
      } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
      } 
	return colList;
  }

  
  public ArrayList<String> getRows(){
	    String row = "";
	    try{
		  // load file
	      fileReader = new FileReader(FileManager.fileName);
	      // initialize buffer reader to read lines
	      bufferedReader = new BufferedReader(fileReader);
	      String temp = bufferedReader.readLine();
	      	while((row = bufferedReader.readLine()) != null){
	            // add every row to list
	        	rows.add(row);
	        }
	      } catch(FileNotFoundException e){
	    	  // TODO Auto-generated catch block
	    	  e.printStackTrace();
	      } catch (IOException e) {
	    	  // TODO Auto-generated catch block
	    	  e.printStackTrace();
		} 
	    
		return rows;
	  }
  
  
//  	public void createJSON(ArrayList<String> result, ArrayList<String> fields) throws Exception{
//  		int pos = 0;
//  		// create a json object
//  		JSONObject obj = new JSONObject();
//  		// create a json array for every row
//  		JSONArray rows = new JSONArray();
//  		// create a json object for every column
//  		JSONObject row = new JSONObject();
//
//  		// for every row in result
//  		for(String line: result) {
//  			pos = 0;
//  			// get every column and put in json
//  			while(pos < fields.size()) {
//  	  			row.put(fields.get(pos), result.get(pos));
//  	  			pos++;
//  	  		}
//  			// add that row to json
//  			rows.put(row);
//  		}
//  		// add the rows to json
//  		obj.put("Output",rows);
//  		
//  		try {
//  			// create json file and write in it
//  			fileWriter = new FileWriter("output.json");
//  			fileWriter.write(obj.toJSONString());
//  	  		fileWriter.flush();
//  		} catch(IOException e) {
//  			throw e;
//  		} finally {
//  			// close resource
//  			fileWriter.close();
//  		}
//  	}
}
