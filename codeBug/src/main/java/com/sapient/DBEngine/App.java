package com.sapient.DBEngine;

public class App {

	public static void main(String[] args) {
        QueryParameter queryParameter = new QueryParameter();
		// TODO work on date condition
        String query = "Select * from ipl.csv where id < 10 and city = Bangalore";
        queryParameter.setQuery(query);
        queryParameter.setFileName();
        DataBase database = new DataBase(queryParameter.getFileName());
        database.readFileHeader();
        database.getHeaderType();
        System.out.println("\nYour query: "+queryParameter.getQuery()+"\nOutput:\n");
        database.executeQuery(queryParameter);
	}
}
