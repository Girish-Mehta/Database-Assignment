package DBEngine.DBEngine;

/**
 * Author: Girish Mehta
 *
 */
public class App
{
    public static void main( String[] args )
    {
      String query = "Select * from ipl.csv where city = Chandigarh group by city";
    	QueryParameter queryParameter = new QueryParameter(query);
    	DataBaseManager baseManager = new DataBaseManager();
      try{
        if(baseManager.executeQuery(queryParameter))
      		System.out.println("Success");
      	else
      		System.out.println("Failed");
      } catch(Exception e){
        System.out.println(e.toString());
        e.printStackTrace();
      }
    }

}




/*
	System.out.println(queryParameter.getBase());
	System.out.println(queryParameter.getFileName());
	System.out.println(queryParameter.getFilter());
	ArrayList<String> test = new ArrayList<String>();
	test = queryParameter.getConditions();
	for(String con: test)
		System.out.println("Cond:"+con);

	test = queryParameter.getFields();
	for(String con: test)
		System.out.println("Field:"+con);

	test = queryParameter.getFunctions();
	for(String con: test)
		System.out.println("Func:"+con);

	test = queryParameter.getGroups();
	for(String con: test)
		System.out.println("Groups:"+test);

	test = queryParameter.getOperators();
	for(String con: test)
		System.out.println("OP:"+con);

	test = queryParameter.getOrder();
	for(String con: test)
		System.out.println("Order:"+con);
*/
