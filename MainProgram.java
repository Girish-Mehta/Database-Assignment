import java.util.Scanner;
class MainProgram{
    public static void main(String[] args){
        DataBase db = new DataBase("ipl.csv");
        QueryParameter queryParameter = new QueryParameter();
        String query = "Select id,season,city,date,team1,team2 from ipl.csv where season = 2008";
        queryParameter.setQuery(query);
        db.readFileHeader();
        db.getHeaderType();
        
        String[] ssub = queryParameter.getConditions();
        for(String a:ssub){
            System.out.println(a);
        }

        // db.executeQuery(queryParameter);
    }
}





        // Scanner sc = new Scanner(System.in);
        // QueryParameter queryParameter = new QueryParameter();
        // String query = null;
        // String[] sub;
        // query = sc.nextLine();
        // queryParameter.setQuery(query);
        // sub = queryParameter.split();
        // String[] ssub = queryParameter.getBase("where");
        // for(String a:ssub){
        //     System.out.println(a);
        // }
        // ssub = queryParameter.getFilter("where");
        // for(String a:ssub){
        //     System.out.println(a);
        // }

        // // ssub = queryParameter.getConditions("where");
        // // for(String a:ssub){
        // //     System.out.println(a);
        // // }

        // System.out.println(queryParameter.getLogicalOp());
        // ssub = queryParameter.getFields();
        // for(String a:ssub){
        //     System.out.println(a);
        // }

        // System.out.println("Order By: "+queryParameter.getOrderBy());
        // System.out.println("Group By: "+queryParameter.getGroupBy());
        // System.out.println("Functions: "+queryParameter.getFunctions());