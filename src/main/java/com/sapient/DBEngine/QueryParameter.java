package com.sapient.DBEngine;

import java.util.ArrayList;
import java.util.regex.*;

public class QueryParameter {
    private ArrayList<String> base;
	private ArrayList<String> conditions;
	private ArrayList<String> fields;
    private ArrayList<String> filter;
    private ArrayList<String> functions;
    private ArrayList<String> groups;
    private ArrayList<String> orders;
    private ArrayList<String> logicalOps;
    private String query;

    public QueryParameter() {
		// TODO Auto-generated constructor stub
    	this.base = new ArrayList<String>();
    	this.conditions = new ArrayList<String>();
    	this.fields = new ArrayList<String>();
    	this.filter = new ArrayList<String>();
    	this.functions = new ArrayList<String>();
    	this.groups = new ArrayList<String>();
    	this.orders = new ArrayList<String>();
    	this.logicalOps = new ArrayList<String>();
    }

    public void setQuery(String query){
        this.query = query;
    }

    public String getQuery(){
        return this.query;
    }

    public String[] split(){
        return query.split("\\s");
    }

    public ArrayList<String> getBase(String point){
        String sub = query.substring(0,query.toUpperCase().indexOf(point.toUpperCase()));
        String[] base = sub.split("\\s");
        for(String str: base) {
        	this.base.add(str);
        }
        return this.base;
    }

    public ArrayList<String> getFilter(){
        String sub = query.substring(query.toLowerCase().indexOf("where")+6,query.length());
        String[] filter = sub.split("\\s");
        for(String str: filter) {
        	this.filter.add(str);
        }
        return this.filter;
    }


	public ArrayList<String> getConditions() {
		String sub = query.substring(query.toLowerCase().indexOf("where")+6,query.length());
		Pattern pattern = Pattern.compile("(\\w+[ ]?)(<>|>=|<=|>|<|=|like|in|not like|not in|(between[ ]?\\d[ ]?and\\d))([ ]?['(]?\\w+[')]?)");
		Matcher matcher = pattern.matcher(sub);
		while (matcher.find())
		{
			this.conditions.add(matcher.group());
		}
		return this.conditions;
	}

    public ArrayList<String> getLogicalOp(){
        if(query.contains("and")){
            this.logicalOps.add("and");
        }

        if(query.contains("or")){
        	this.logicalOps.add("or");
        }

        if(query.contains("not")){
        	this.logicalOps.add("not");
        }
        return this.logicalOps;
    }

    public ArrayList<String> getFields(){
   		int startPos = query.toLowerCase().indexOf("select")+7;
		int endPos = query.toLowerCase().indexOf("from")-1;
        String sub = query.substring(startPos, endPos);
        String[] field = sub.split(",");
        for(String str: field)
        	this.fields.add(str);
        return this.fields;
    }

    public ArrayList<String> getOrderBy(){
        if(query.contains("order by")){
            int startPos = query.toLowerCase().indexOf("order by")+9;
            String sub = query.substring(startPos, query.length());
            String[] commaSeparated = sub.split(",");
            StringBuilder string = new StringBuilder();
            for(String s:commaSeparated){
                if(!s.toLowerCase().contains(" ")){
                    string.append(s+"\n");
                } else{
                    String[] ch = s.split(" ");
                    if(ch[1].toLowerCase().contains("asc") || ch[1].toLowerCase().contains("desc"))
                        string.append(ch[0]+"\n");
                    else
                        string.append(ch[1]+"\n");
                }
            }
            String[] orders = string.toString().split("\n");
            for(String str: orders)
            	this.orders.add(str);
        }

        return this.orders;
    }


    public ArrayList<String> getGroupBy(){
        if(query.contains("group by")){
            int startPos = query.toLowerCase().indexOf("group by")+9;
            String sub = query.substring(startPos, query.length());
            String[] commaSeparated = sub.split(",");
            StringBuilder string = new StringBuilder();
            for(String s:commaSeparated){
                if(!s.toLowerCase().contains(" ")){
                    string.append(s+"\n");
                } else{
                    String[] ch = s.split(" ");
                    if(ch[1].contains(" "))
                        string.append(ch[0]+"\n");
                    else
                        string.append(ch[1]+"\n");
                }
            }
            String[] group = string.toString().split("\n");
            for(String str: group)
            	this.groups.add(str);
        }
        return this.groups;
    }

    public ArrayList<String> getFunctions(){
        int startPos = 6;
        int endPos = query.toLowerCase().indexOf("from");
        String sub = query.substring(6, endPos).toLowerCase();
        StringBuilder subString = new StringBuilder();
        if(sub.contains("avg(")){
            startPos = sub.indexOf("avg(");
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

        if(sub.contains("min(")){
            startPos = sub.indexOf("min(");
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

       String[] fnc = subString.toString().split("\n");
       for(String str: fnc)
    	   this.functions.add(str);

       return this.functions;
    }
}
