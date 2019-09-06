package com.graphaware.graphhack;

import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseTest {

    protected GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {

        // With all the index manipulation it is easier to create completely new database in each test to have independent tests
        db = Neo4jSupport.neo4j();
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    protected List<Map<String, Object>> execute(String statement) {
        return db.execute(statement).stream().collect(Collectors.toList());
    }

    protected List<Map<String, Object>> execute(String statement, Map<String, Object> params) {
        return db.execute(statement, params).stream().collect(Collectors.toList());
    }
}
