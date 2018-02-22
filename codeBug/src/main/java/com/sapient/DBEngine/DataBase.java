package com.sapient.DBEngine;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class DataBase {
	// variable to store my csv file name written in query 
    private String csvFile;
    // variable to store column names
    private String[] headers;
    // variable to store a single row
    private String[] col;
    // variable to store data types of individual column
    private String[] dataType;

    // constructor
    DataBase(){}

    // initialize object with file name from QueryParameter class
    public DataBase(String csvFile){
        this.csvFile = csvFile;
    }

    // function to read the column names from file and store them in variable
    public void readFileHeader(){
    	// reader to read my csv file
    	BufferedReader br = null;
        // variable to store my individual row
        String row = "";
        
        // read only the first row of my file
        // split by comma and store columns 
        // header variable
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((row = br.readLine()) != null) {
            	this.headers = row.split(",");
               break;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    // function to store the data type of columns by reading from first column
    public void getHeaderType(){
    	// reader to read csv file
    	BufferedReader br = null;
        //variable to store individual row
        String row = "";
        // variable to store current index of array list
        int pos= 0;
        // variable to skip the header row
        int skip = 0;
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((row = br.readLine()) != null) {
            	// skip the header row
            	if(skip == 0){
                    skip++;
                    continue;
                }
            	
            	// condition to not read more than one column
                if(skip == 2)
                    break;

                // read individual column, compare with expression to check for data type
                // use comma as separator
                this.col = row.split(",");
                this.dataType = new String[this.headers.length];
                for(String sub: this.col){
                    if(sub.matches("-?\\d+(\\.\\d+)?"))
                        dataType[pos] = "Number";
                    else if(sub.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"))
                        dataType[pos] = "Date";
                    else
                        dataType[pos] = "String";
                    pos++;
                }
                // mark the completion of reading a row 
                skip++;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // function to execute query
    public void executeQuery(QueryParameter query) {
    	// to store position of array list iteration
    	int pos = 0;
    	// used for flagging in execution
    	int flag = 0;
    	// to store index of list
    	int loc = 0;
    	// to store the count of output files created
    	Integer count = 1;
    	
    	// to read files to execute query upon
    	FileWriter fw = null;
    	
    	// to store logical operations required in query
    	ArrayList<String> logicalOp = query.getLogicalOp();
    	// to store required field names in query
    	ArrayList<String> fields = query.getFields();
    	// to store the index of field names to print
    	ArrayList<Integer> locations = new ArrayList<Integer>(fields.size());

    	// to store the name of final output file
        String outFile = "temp.csv";
        // to generate output file name for step by step execution cycle
        StringBuilder stringbuilder = new StringBuilder();
        stringbuilder.append("temp0.csv");
        // to store the file name of intermediate executions
        String fileName = stringbuilder.toString();

        BufferedReader br = null;
        
        // execute every condition in query
        // for every query print output in new file 
        if(query.getConditions().size() != 0) {
            for(String condition: query.getConditions()) {
            	System.out.println(fileName+" "+condition);
        		executeQuery(condition, fileName);
        		// if previous query worked on and condition
        		// then delete previous output file so as 
        		// to intersect the outputs of two conditions
        		if(flag == 1) {
                	count = count-2;
                	stringbuilder.replace(4, 5, count.toString());        	
        			Path path = Paths.get(stringbuilder.toString());

        			try {
    					Files.delete(path);
    				} catch (IOException e) {
    					e.printStackTrace();
    				}
        			flag = 0;
        		}
        		//check if 'and' or 'or', accordingly decide
        		// the file to work upon
        		if(pos < logicalOp.size()) {
        			if(logicalOp.get(pos).equals(" or ")) {
                		// if or, execute query on file given by user
        				this.csvFile = query.getFileName();
                		pos++;
            		}
                	else if(logicalOp.get(pos).equals(" and ")) {
                		// if and, execute query on previous query output file
                		this.csvFile = fileName;
                		pos++;
                		flag = 1;
                	}
        			
        		}
       	        // generate new output file name
       	        count++;      			
       	        System.out.println("Count:___"+count);
       	        stringbuilder.replace(4, 5, count.toString());
       	        fileName = stringbuilder.toString();
            }        	
        } else {
        	outFile = query.getFileName();
        }

        // get all output files generated and combine in one file
        // delete other output files
    	if(query.getConditions().size() != 0) {
	        try {
	        	String row;
	        	fw = new FileWriter(outFile);
	        	br = null;
	        	for(String condition: query.getConditions()) {
	        		// for every condition, read previous output file
	            	stringbuilder.replace(4, 5, count.toString());     
	            	Path path = Paths.get(stringbuilder.toString());
	            	try {
	                	br = new BufferedReader(new FileReader(stringbuilder.toString()));            		
	            	} catch(FileNotFoundException e) {
	            		count--;
	            		continue;           		
	            	}
	            	while ((row = br.readLine()) != null) {
	            		// store the output in final output file -> outFile
	            		fw.write(row+"\n");
	            	}
	            	// delete the not required file
	            	Files.delete(path);
	            	count--;
	            	if(count <= 0) {
	            		break;
	            	}
	        	}
	
	        } catch (IOException e) {
				System.out.println(e);
			} finally {
	        	// close resources
	        	try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			}
    	}

    	// store the index of fields required by user
    	pos = 0;
        Iterator<String> it = fields.iterator();
        while(it.hasNext()){
        	String field = it.next();
        	if(field.equals("*")) {
        		// if *, store all fields index
        		for(pos = 0; pos < this.headers.length; pos++) {
        			locations.add(pos);
        		}
        	} else {
            	for(String header:headers) {
            		if(field.equals(header)) {
            			// match requested field with column names to get index
                		locations.add(pos);
                    	pos++;
                	}	
            	}        		
        	}
        }

        // if *, store all column names
        if(fields.get(0).equals("*")) {
    		for(pos = 0; pos < this.headers.length; pos++) {
    			fields.add(this.headers[pos]);
    		}
        }
        Collections.sort(locations);

        // print the fields from the final output file       
        try {
        	pos = 0;
        	loc = 0;
        	String row;  	
        	br = new BufferedReader(new FileReader(outFile));
        	while ((row = br.readLine()) != null) {
        		// clear everything in stringbuilder
    			stringbuilder.delete(0, stringbuilder.length());
    			// read column by column
    			String[] field = row.split(",");
        		for(String fld: field) {
        			// if reached end of row, break loop to move to next row
        			if(loc == fields.size())
        				break;
        			if(pos == locations.get(loc)) {
        				// if at requested index, store column value
        				stringbuilder.append(fld+",");
        				loc++;
        			}
        			pos++;
        		}
        		// remove the ',' appended in the end
        		stringbuilder.delete(stringbuilder.length()-1, stringbuilder.length());
        		// print the output
        		System.out.println(stringbuilder+"\n");
        		// reset index and position
        		pos = 0;
        		loc = 0;
        	}        
        } catch (IOException e) {
			System.out.println(e);
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    }

    
    public void executeQuery(String condition, String fileName){    	
    	// to read the file upon which to execute query
    	FileWriter fw = null;
    	// to store condition into field and value pair
    	String[] con;
    	// to store the field to compare with in condition
    	String parameter;
    	// declaring two variables for storing locations and positions of fields in csv file
        int loc = 0;
        int pos = 0;
		System.out.println(condition);
  
        
        try{    
            fw=new FileWriter(fileName);
           }catch(Exception e){
        	   System.out.println(e);
           }    
   	
        	
        	// check if condition is checking for less than or equal to
        	if(condition.contains("<=")){
        		pos = condition.indexOf("<=")-1;
        		
        		// split condition into field name and value
        		con = condition.split(" <= ");
        		// store field name to compare with
        		parameter = con[0];
        		// store value to compare with
        		int value = Integer.parseInt(con[1]);

        		pos = 0;
        		for(String head: this.headers){
        			pos++;
        			if(head.equals(parameter)){
        					loc = pos-1;
        					break;
        			}
        		}
	            BufferedReader br = null;
	            String row = "";
	
	            try {
	                br = new BufferedReader(new FileReader(csvFile));
	                while ((row = br.readLine()) != null) {
	                   this.col = row.split(",");
	                   pos = 0;
	                   for(String val: col){
	                     if(pos == loc){
	                    	 try {
		                    	 if(Integer.parseInt(val) <= value){
//		                    		 fw.write(row+"\n");
		                    		 pos = 0;
			                     }	                    		 
	                    	 }
	                    	 catch(Exception e) {
	                        	 continue;
	                         }
                           break;
	                     } else{
	                       pos++;
	                     }
	                   }
	                }	
	            } catch (FileNotFoundException e) {
	                e.printStackTrace();
	            } catch (IOException e) {
	                e.printStackTrace();
	            } finally {
	                if (br != null) {
	                    try {
	                        br.close();
	                    } catch (IOException e) {
	                        e.printStackTrace();
	                    }
	                }
	            }
        	}
        	
        	// check if condition is checking for greater than or equal to
        	else if(condition.contains(">=")){
        		pos = condition.indexOf(">=")-1;
        		
        		// get the field parameter to compare from query
        		con = condition.split(" >= ");
        		parameter = con[0];
        		int value = Integer.parseInt(con[1]);

        		pos = 0;
        		for(String head: this.headers){
        			pos++;
        			if(head.equals(parameter)){
        					loc = pos-1;
        					break;
        			}
        		}
	            BufferedReader br = null;
	            String row = "";
	
	            try {
	                br = new BufferedReader(new FileReader(csvFile));
	                while ((row = br.readLine()) != null) {
	                   this.col = row.split(",");
	                   pos = 0;
	                   for(String val: col){
	                     if(pos == loc){
	                    	 try {
	                    		 if(Integer.parseInt(val) >= value){
	  		                       fw.write(row+"\n");
	  	                           pos = 0;
	  	                         }	 
	                    	 }
	                    	 catch(Exception e) {
	                    		 continue;
	                    	 }
	                         
                           break;
	                     } else{
	                       pos++;
	                     }
	                   }
	                }	
	            } catch (FileNotFoundException e) {
	                e.printStackTrace();
	            } catch (IOException e) {
	                e.printStackTrace();
	            } finally {
	                if (br != null) {
	                    try {
	                        br.close();
	                    } catch (IOException e) {
	                        e.printStackTrace();
	                    }
	                }
	            }
        	}
        	// check if condition is checking for equality
        	else if(condition.contains("=")){
        		pos = condition.indexOf("=")-1;
        		
        		// get the field parameter to compare from query
        		con = condition.split(" = ");
        		parameter = con[0];
        		String value = con[1];

        		pos = 0;
        		for(String head: this.headers){
        			pos++;
        			if(head.equals(parameter)){
        					loc = pos-1;
        					break;
        			}
        		}
	            BufferedReader br = null;
	            String row = "";
	
	            try {
	                br = new BufferedReader(new FileReader(csvFile));
	                while ((row = br.readLine()) != null) {
	                   this.col = row.split(",");
	                   pos = 0;
	                   for(String val: col){
	                     if(pos == loc){
	                         if(val.equals(value)){
	                           fw.write(row+"\n");
	                           pos = 0;
	                         }
                           break;
	                     } else{
	                       pos++;
	                     }
	                   }
	                }	
	            } catch (FileNotFoundException e) {
	                e.printStackTrace();
	            } catch (IOException e) {
	                e.printStackTrace();
	            } finally {
	                if (br != null) {
	                    try {
	                        br.close();
	                    } catch (IOException e) {
	                        e.printStackTrace();
	                    }
	                }
	            }
        	}
        	// check if condition is checking for less than
        	else if(condition.contains("<")){
        		pos = condition.indexOf("<")-1;
        		
        		// get the field parameter to compare from query
        		con = condition.split(" < ");
        		parameter = con[0];
        		int value = Integer.parseInt(con[1]);

        		pos = 0;
        		for(String head: this.headers){
        			pos++;
        			if(head.equals(parameter)){
        					loc = pos-1;
        					break;
        			}
        		}
	            BufferedReader br = null;
	            String row = "";
	
	            try {
	                br = new BufferedReader(new FileReader(csvFile));
	                while ((row = br.readLine()) != null) {
	                   this.col = row.split(",");
	                   pos = 0;
	                   for(String val: col){
	                     if(pos == loc){
	                    	 try {
		                         if(Integer.parseInt(val) < value){
				                       fw.write(row+"\n");
			                           pos = 0;
			                         } 
	                    	 }
	                    	 catch(Exception e) {
	                    		 continue;
	                    	 }
                           break;
	                     } else{
	                       pos++;
	                     }
	                   }
	                }	
	            } catch (FileNotFoundException e) {
	                e.printStackTrace();
	            } catch (IOException e) {
	                e.printStackTrace();
	            } finally {
	                if (br != null) {
	                    try {
	                        br.close();
	                    } catch (IOException e) {
	                        e.printStackTrace();
	                    }
	                }
	            }
        	}
        	// check if condition is checking for greater than
        	else if(condition.contains(">")){
        		pos = condition.indexOf(">")-1;
        		
        		// get the field parameter to compare from query
        		con = condition.split(" > ");
        		parameter = con[0];
        		int value = Integer.parseInt(con[1]);

        		pos = 0;
        		for(String head: this.headers){
        			pos++;
        			if(head.equals(parameter)){
        					loc = pos-1;
        					break;
        			}
        		}
	            BufferedReader br = null;
	            String row = "";
	
	            try {
	                br = new BufferedReader(new FileReader(csvFile));
	                while ((row = br.readLine()) != null) {
	                   this.col = row.split(",");
	                   pos = 0;
	                   for(String val: col){
	                     if(pos == loc){
	                    	 try {
		                         if(Integer.parseInt(val) > value){
				                       fw.write(row+"\n");
			                           pos = 0;	                    		 
		                         }
	                    	 }
	                         catch(Exception e) {
	                        	 continue;
	                         }
                           break;
	                     } else{
	                       pos++;
	                     }
	                   }
	                }	
	            } catch (FileNotFoundException e) {
	                e.printStackTrace();
	            } catch (IOException e) {
	                e.printStackTrace();
	            } finally {
	                if (br != null) {
	                    try {
	                        br.close();
	                    } catch (IOException e) {
	                        e.printStackTrace();
	                    }
	                }
	            }
        	}       	
        	
        	try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
    }
}
