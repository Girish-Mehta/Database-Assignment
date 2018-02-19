import java.util.Scanner;
class MainProgram{
    public static void main(String[] args){
        DataBase db = new DataBase("ipl.csv");
        QueryParameter queryParameter = new QueryParameter();
        String query = "Select id,season,city,date,team1,team2 from ipl.csv where season = 2010 and win_by_runs = 140";
        queryParameter.setQuery(query);
        db.readFileHeader();
        db.getHeaderType();
        db.executeQuery(queryParameter);
    }
}
