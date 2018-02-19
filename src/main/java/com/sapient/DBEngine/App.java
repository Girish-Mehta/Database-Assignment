package com.sapient.DBEngine;

import java.util.ArrayList;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataBase database = new DataBase("ipl.csv");
        QueryParameter queryParameter = new QueryParameter();
        String query = "Select id,season,city,date,team1,team2 from ipl.csv where win_by_runs = 140 and season = 2008";
        queryParameter.setQuery(query);
        database.readFileHeader();
        database.getHeaderType();
        database.executeQuery(queryParameter);
	}
}
