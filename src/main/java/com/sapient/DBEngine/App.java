package com.sapient.DBEngine;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataBase database = new DataBase("ipl.csv");
        QueryParameter queryParameter = new QueryParameter();
        // work on date condition
        String query = "Select id,season,city,date,team1,team2 from ipl.csv where id <= 5";
        queryParameter.setQuery(query);
        database.readFileHeader();
        database.getHeaderType();
        System.out.println("\nYour query: "+queryParameter.getQuery()+"\nOutput:\n");
        database.executeQuery(queryParameter);
	}
}
