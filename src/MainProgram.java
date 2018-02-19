package com.dbengine;

import java.util.Scanner;
import DataBase;
import QueryParameter;

class MainProgram{
    public static void main(String[] args){
        DataBase db = new DataBase("ipl.csv");
        QueryParameter queryParameter = new QueryParameter();
        String query = "Select id,season,city,date,team1,team2 from ipl.csv where win_by_runs = 140";
        queryParameter.setQuery(query);
        db.readFileHeader();
        db.getHeaderType();
        db.executeQuery(queryParameter);
    }
}
