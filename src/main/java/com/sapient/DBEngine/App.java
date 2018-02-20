package com.sapient.DBEngine;

public class App {

	public static void main(String[] args) {
        QueryParameter queryParameter = new QueryParameter();
		// TODO work on date condition
        String query = "Select id,season,city,date,team1,team2 from ipl.csv where id < 5 and city = Chandigarh or city = Bangalore";
        queryParameter.setQuery(query);
        queryParameter.setFileName();
        DataBase database = new DataBase(queryParameter.getFileName());
        database.readFileHeader();
        database.getHeaderType();
        System.out.println("\nYour query: "+queryParameter.getQuery()+"\nOutput:\n");
        database.executeQuery(queryParameter);
	}
}
