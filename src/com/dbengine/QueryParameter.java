package com.dbengine;

import java.util.regex.*;

public class QueryParameter{
    private String[] base;
	  private String[] conditions;
	  private String[] fields;
    private String[] filter;
    private String[] functions;
    private String[] groups;
    private String[] orders;
    private String[] logicalOps;
    private String query;


    public void setQuery(String query){
        this.query = query;
    }

    public String getQuery(){
        return this.query;
    }

    public String[] split(){
        return query.split("\\s");
    }

    public String[] getBase(String point){
        String sub = query.substring(0,query.toUpperCase().indexOf(point.toUpperCase()));
        this.base = sub.split("\\s");
        return this.base;
    }

    public String[] getFilter(){
        String sub = query.substring(query.toLowerCase().indexOf("where")+6,query.length());
        this.filter = sub.split("\\s");
        return this.filter;
    }


	public String[] getConditions() {
		int i = 0;
    String sub = query.substring(query.toLowerCase().indexOf("where")+6,query.length());
		Pattern pattern = Pattern.compile("(\\w+[ ]?)(<>|>=|<=|>|<|=|like|in|not like|not in|(between[ ]?\\d[ ]?and\\d))([ ]?['(]?\\w+[')]?)");
		Matcher matcher = pattern.matcher(sub);
		// System.out.println("\nConditions:");
		while (matcher.find())
		{
      this.conditions = matcher.group().split(",");
			// this.conditions[i++] = matcher.group();
		}
		return this.conditions;
	}

    public String[] getLogicalOp(){
        StringBuilder ops = new StringBuilder();
        if(query.contains("and")){
            ops.append("and\n");
        }

        if(query.contains("or")){
            ops.append("or\n");
        }

        if(query.contains("not")){
            ops.append("not\n");
        }
        this.logicalOps = ops.toString().split("\n");
        return this.logicalOps;
    }

    public String[] getFields(){
   		int startPos = query.toLowerCase().indexOf("select")+7;
		int endPos = query.toLowerCase().indexOf("from")-1;
        String sub = query.substring(startPos, endPos);
        this.fields = sub.split(",");
        return this.fields;
    }

    public String[] getOrderBy(){
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
            this.orders = string.toString().split("\n");
        }

        return this.orders;
    }


    public String[] getGroupBy(){
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
            this.groups = string.toString().split("\n");
        }
        return this.groups;
    }

    public String[] getFunctions(){
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

        this.functions = subString.toString().split("\n");
        return this.functions;
    }

}
