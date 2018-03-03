package DBEngine.DBEngine;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParameter {
	// variable to store query
	private String query;
	// variable to store file name
	private String fileName;
	// variable to store string after 'where'
	private String filter = null;
	// variable to store string before 'where'
	private String base = null;
	// List to store conditions in query
	private ArrayList<String> conditions;
	// list to store required fields in query
	private ArrayList<String> fields;
	// list to store functions like 'min()' 'max()' 'avg()' 'sum()'
	private ArrayList<String> functions;
	// list to store group by from query
	private ArrayList<String> groups;
	// list to store operators like 'or' 'and'
	private ArrayList<String> operators;
	// variable to store order by from query
	private ArrayList<String> order;

	// Default Constructor
	QueryParameter(){
		this.base = "";
		this.conditions = new ArrayList<String>();
		this.fields = new ArrayList<String>();
		this.fileName = "";
		this.filter = "";
		this.functions = new ArrayList<String>();
		this.groups = new ArrayList<String>();
		this.operators = new ArrayList<String>();
		this.order = new ArrayList<String>();
	}

	// Parameterized constructor to initialize variables form query
	QueryParameter(String query){
		this();
		this.query = query;
		setFileName();
		setBase();
		setConditions();
		setFields();
		setFilters();
		setFunctions();
		setGroups();
		setOperators();
		setOrder();
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName() {
		// sarch query for any .csv or .txt file
		Pattern pattern = Pattern.compile("[a-zA-Z0-9]+\\.(csv)|(txt)");
    	Matcher matcher = pattern.matcher(query);
    	// if found then store name in variable
    	if (matcher.find())
		{
		    this.fileName = matcher.group();
		}
	}

	public String getBase() {
		return base;
	}

	public void setBase() {
		// split the query before 'where' and store in variable
		this.base = query.substring(0,query.indexOf(" where "));
	}

	public ArrayList<String> getConditions() {
		return conditions;
	}

	public void setConditions() {
		// get a sub-string from query starting after 'where' till the end
		// as conditions come after 'where'
		String sub = query.substring(query.toLowerCase().indexOf("where")+6,query.length());
		// match with pattern
		Pattern pattern = Pattern.compile("(\\w+[ ]?)(<>|>=|<=|>|<|=|like|in|not like|not in|(between?\\d[ ]?and\\d))(([ ]?\\w+)|([\'|(]?\\w+[ ]?\\w+[\'|)]?))");
		Matcher matcher = pattern.matcher(sub);
		while (matcher.find())
		{
			// if condition found then store in string
			String str = matcher.group();
			Character c = str.charAt(str.length()-1);
			// if condition contains extra spacing then remove
			if(c.compareTo(' ') == 0)
				this.conditions.add(str.substring(0, str.length()-1));
			// else save the condition
			else
				this.conditions.add(str);
		}
	}

	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields() {
		// functions usually lie between 'select' and 'from'

		// save starting position to pull out sub string from query
		// size of 'select' + 1 = 7
		int startPos = 7;
		// save ending position
		int endPos = query.toLowerCase().indexOf("from")-1;
        String sub = query.substring(startPos, endPos);
        // split the sub query based on comma
        String[] field = sub.split(",");
        for(String str: field) {
        	// if field does not contain a function then save in variable
        	if(!(str.contains("(") && str.contains(")"))){
               	this.fields.add(str);
        	}
        }
	}

	public String getFilter() {
		return filter;
	}

	public void setFilters() {
		// get sub string from query after 'where' part
		this.filter = query.substring(query.indexOf(" where ")+7, query.length());
	}

	public ArrayList<String> getFunctions() {
		return functions;
	}

	public void setFunctions() {
		// save starting position to get sub query
		// length of 'select' = 6
		int startPos = 6;
		// functions are between 'where' and 'from'
		int endPos = query.toLowerCase().indexOf(" from ");
        String sub = query.substring(6, endPos).toLowerCase();
        StringBuilder subString = new StringBuilder();
        if(sub.contains("avg(")){
        	// get starting position of function
        	startPos = sub.indexOf("avg(");
            endPos = startPos+1;
            Character c;
            // go character by character until you find the closing bracket
            // ')'
            do{
                c = sub.charAt(endPos);
                if(!(c.compareTo(')') == 0))
                    endPos++;
                else
                	// when found, break out of loop
                    break;
            }
            while(endPos < sub.length());
            // save the function in a new line
            subString.append(sub.substring(startPos,endPos+1)+"\n");
        }

        if(sub.contains("min(")){
        	// get starting position of function
        	startPos = sub.indexOf("min(");
            endPos = startPos+1;
            Character c;
            // go character by character until you find the closing bracket
            // ')'
            do{
            	c = sub.charAt(endPos);
                if(!(c.compareTo(')') == 0))
                    endPos++;
                else
                	// when found, break out of loop
                	break;
            }
            while(endPos < sub.length());
            // save the function in a new line
            subString.append(sub.substring(startPos,endPos+1)+"\n");
        }

        if(sub.contains("max(")){
            startPos = sub.indexOf("max(");
            endPos = startPos+1;
            Character c;
            do{
                c = sub.charAt(endPos);
                if(!(c.compareTo(')') == 0))
                    endPos++;
                else
                    break;
            }
            while(endPos < sub.length());
            subString.append(sub.substring(startPos,endPos+1)+"\n");
        }

        if(sub.contains("count(")){
            startPos = sub.indexOf("count(");
            endPos = startPos+1;
            Character c;
            do{
                c = sub.charAt(endPos);
                if(!(c.compareTo(')') == 0))
                    endPos++;
                else
                    break;
            }
            while(endPos < sub.length());
            subString.append(sub.substring(startPos,endPos+1)+"\n");
        }

       if(sub.contains("sum(")){
            startPos = sub.indexOf("sum(");
            endPos = startPos+1;
            Character c;
            do{
                c = sub.charAt(endPos);
                if(!(c.compareTo(')') == 0))
                    endPos++;
                else
                    break;
            }
            while(endPos < sub.length());
            subString.append(sub.substring(startPos,endPos+1)+"\n");
        }

       // find all function based upon new line
       // split the string and store every function to the list
       if(!(subString.toString().equals(" ") || subString.toString().equals(""))) {
           String[] fnc = subString.toString().split("\n");
           for(String str: fnc)
        	   this.functions.add(str);
       }
	}

	public ArrayList<String> getGroups() {
		return groups;
	}

	public void setGroups() {
        if(query.contains("group by")){
        	// get starting position of group by
        	int startPos = query.toLowerCase().indexOf("group by")+8;
            String sub = query.substring(startPos, query.length());
            String[] commaSeparated = sub.split(",");
            StringBuilder string = new StringBuilder();
            for(String s:commaSeparated){
            	// if group by does not contain space save it
            	if(!s.toLowerCase().contains(" ")){
                    string.append(s+"\n");
                } else{
                	// if contains space, remove the space and save
                	String[] ch = s.split(" ");
                    if(ch[1].contains(" "))
                        string.append(ch[0]+"\n");
                    else
                        string.append(ch[1]+"\n");
                }
            }
            String[] group = string.toString().split("\n");
            for(String str: group) {
            	this.groups.add(str);
            }
        }
	}

	public ArrayList<String> getOperators() {
		return operators;
	}

	public void setOperators() {
    	Pattern p3 = Pattern.compile("( and )|( or )|( not )");
		Matcher m3 = p3.matcher(query.substring(query.indexOf("where")+5, query.length()-1));
		while (m3.find()) {
			this.operators.add(m3.group());
		}
	}

	public ArrayList<String> getOrder() {
		return order;
	}

	public void setOrder() {
        if(query.contains("order by")){
        	// save starting position of order by
        	int startPos = query.toLowerCase().indexOf("order by")+9;
            String sub = query.substring(startPos, query.length());
            String[] commaSeparated = sub.split(",");
            StringBuilder string = new StringBuilder();
            for(String s:commaSeparated){
            	// if id does not contain space then save
            	if(!s.toLowerCase().contains(" ")){
                    string.append(s+"\n");
                } else{
                	// else split based upon space and save the part without space
                    String[] ch = s.split(" ");
                    if(ch[1].toLowerCase().contains("asc") || ch[1].toLowerCase().contains("desc"))
                        string.append(ch[0]+"\n");
                    else
                        string.append(ch[1]+"\n");
                }
            }
            String[] orders = string.toString().split("\n");
            for(String str: orders)
            	this.order.add(str);
        }
	}
}
