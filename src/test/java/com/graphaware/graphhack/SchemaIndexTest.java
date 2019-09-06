package com.graphaware.graphhack;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.TestGraphDatabaseBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for normal indexes that we didn't break them
 *
 * We don't need this if we use explicit indexes
 */
public class SchemaIndexTest {
/*

    @ClassRule
    public static Neo4jRule neo4j = new Neo4jRule();
*/

    private GraphDatabaseService service;

    @Before
    public void setUp() throws Exception {
//        service = neo4j.getGraphDatabaseService();
        service = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @Test
    public void testName() {
        service.execute("CREATE INDEX ON :Person(name)").close();
        service.execute("CREATE (p:Person {name:'Frantisek'}) ").close();
        service.execute("CREATE (p:Person {name:'Michal'}) ").close();
        service.execute("CREATE (p:Person {name:'Will'}) ").close();
        service.execute("CREATE (p:Person {name:'Sergio'}) ").close();

        Result result = service.execute("MATCH (p:Person) WHERE p.name = $name RETURN p", of("name", "Frantisek"));

        List<Map<String, Object>> list = newArrayList(result);
        assertThat(list).hasSize(1);
    }

}
