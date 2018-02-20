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

public class DataBase {
    private String csvFile;
    private String[] headers;
    private String[] col;
    private String[] dataType;

    DataBase(){}

    public DataBase(String csvFile){
        this.csvFile = csvFile;
    }

    
    public void readFileHeader(){
        // System.out.println("\n**************reading header of file");
        BufferedReader br = null;
        String row = "";

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
    
    
    public void getHeaderType(){
        System.out.println("\nGetting type of header\n\n");
        BufferedReader br = null;
        String row = "";
        int pos= 0;

        int skip = 0;
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((row = br.readLine()) != null) {
                if(skip == 0){
                    skip++;
                    continue;
                }

                if(skip == 2)
                    break;

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
                    System.out.println(this.headers[pos]+" - "+this.dataType[pos]);
                    pos++;
                }
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

    
    public void executeQuery(QueryParameter query) {
    	int pos = 0;
        int flag = 0;
        int loc = 0;
    	Integer count = 1;

        ArrayList<String> logicalOp = query.getLogicalOp();
        ArrayList<String> fields = query.getFields();
        int[] locations = new int[fields.size()];

        BufferedReader br = null;
        
    	StringBuilder stringbuilder = new StringBuilder();
        stringbuilder.append("temp0.csv");
        String fileName = stringbuilder.toString();

        for(String condition: query.getConditions()) {
    		executeQuery(condition, fileName);
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
    		if(pos < logicalOp.size()) {
    			if(logicalOp.get(pos).equals(" or ")) {
            		this.csvFile = query.getFileName();
            		pos++;
        		}
            	else if(logicalOp.get(pos).equals(" and ")) {
            		this.csvFile = fileName;
            		pos++;
            		flag = 1;
            	}
    			
    		}
   	        count++;      			
   	        stringbuilder.replace(4, 5, count.toString());
   	        fileName = stringbuilder.toString();
        }
        
        try {
        	String row;
       	  	FileWriter fw = new FileWriter("temp.csv");
        	br = null;
        	for(String condition: query.getConditions()) {
            	stringbuilder.replace(4, 5, count.toString());     
            	Path path = Paths.get(stringbuilder.toString());
            	try {
                	br = new BufferedReader(new FileReader(stringbuilder.toString()));            		
            	} catch(FileNotFoundException e) {
            		count--;
            		continue;           		
            	}
            	while ((row = br.readLine()) != null) {
            		fw.write(row+"\n");
            	}
            	Files.delete(path);
            	count--;
            	if(count <= 0) {
            		break;
            	}
        	}
        	// close resources
        	fw.close();
        } catch (IOException e) {
			System.out.println(e);
		}

        loc = 0;
        pos = 0;

        for(String header:headers) {
        	if(pos == fields.size())
        		break;
        	if(fields.get(pos).equals(header)) {
        		locations[loc] = pos;
            	loc++;
            	pos++;
        	}
        }
        
        try {
        	pos = 0;
        	loc = 0;
        	String row;  	
        	br = new BufferedReader(new FileReader("temp.csv"));
        	while ((row = br.readLine()) != null) {
    			stringbuilder.delete(0, stringbuilder.length());
        		String[] field = row.split(",");
        		for(String fld: field) {
        			if(loc == fields.size())
        				break;
        			if(pos == locations[loc]) {
        				stringbuilder.append(fld+",");
        				loc++;
        			}
        			pos++;
        		}
        		stringbuilder.delete(stringbuilder.length()-1, stringbuilder.length());
        		System.out.println(stringbuilder+"\n");
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
    	FileWriter fw = null;

    	// declaring two variables for storing locations and positions of fields in csv file
        int loc = 0;
        int pos = 0;
  
        
        try{    
            fw=new FileWriter(fileName);
           }catch(Exception e){
        	   System.out.println(e);
           }    
   	
        	
        	// check if condition is checking for less than or equal to
        	if(condition.contains("<=")){
        		pos = condition.indexOf("<=")-1;
        		
        		// get the field parameter to compare from query
        		String[] con = condition.split(" <= ");
        		String parameter = con[0];
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
        		String[] con = condition.split(" >= ");
        		String parameter = con[0];
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
        		String[] con = condition.split(" = ");
        		String parameter = con[0];
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
        		String[] con = condition.split(" < ");
        		String parameter = con[0];
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
        		String[] con = condition.split(" > ");
        		String parameter = con[0];
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
