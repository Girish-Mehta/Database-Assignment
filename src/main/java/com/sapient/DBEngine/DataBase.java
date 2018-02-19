package com.sapient.DBEngine;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
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

            // for(String sub: headers){
            //     System.out.println(sub);
            // }

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


    public void executeQuery(QueryParameter query){
        ArrayList<String> fields = new ArrayList<String>();
        FileWriter fw = null;
        // declaring two variables for storing locations and positions of fields in csv file
        int loc = 0;
        int pos = 0;
        fields = query.getFields();
        
        
        try{    
            fw=new FileWriter("temp.csv");
//            fw.close();    
           }catch(Exception e){
        	   System.out.println(e);
           }    
        
        // store the positions of fields that user wants from select query
        int[] targetField = new int[fields.size()];
        // match required fields with header from csv to get location of each field
        for(String field:fields){
            pos = 0;
            for(String header:this.headers){
                if(field.equals(header))
                    targetField[loc++] = pos;
                pos++;
            }
        }

        // get conditions in query
        for(String condition: query.getConditions()){
        	
        	
        	
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
				                       fw.write(row+"\n");
		 	                           System.out.println(row+"\n");
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
	   	                           System.out.println(row+"\n");
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
	                           System.out.println(row+"\n");
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
		                        	   System.out.println(row+"\n");
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
		 	                           System.out.println(row+"\n");
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
        	
        }
        try {
        	//delete temp file
        	Path path = Paths.get("temp.csv");
        	Files.delete(path);
        	// close resources
        	fw.close();
		} catch (IOException e) {
			System.out.println(e);
		}    
    }
}
