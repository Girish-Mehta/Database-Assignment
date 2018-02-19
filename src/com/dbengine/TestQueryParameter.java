package com.dbEngine.Test;

import org.junit.jupiter.api.Test;

import com.dbEngine.queryParameter.QueryParameters;

import org.junit.jupiter.api.*;

@DisplayName("TestQueryParameter")
class TestQueryParameter {
	QueryParameters queryParameter = null;
   
	@Before
    void before() {
    	queryParameter = new QueryParameters();
        System.out.println("Set Up done");
    }
    
    @Test
    @DisplayName("Test setQuery")
    void setQueryTest() {
        queryParameter.setQuery();
        Assertions.assertEquals("select * from ipl.csv", queryParameter.getQuery());
    }
 
    @After
    void after() {
        queryParameter = null;
        System.out.println("Completed");
    }
}