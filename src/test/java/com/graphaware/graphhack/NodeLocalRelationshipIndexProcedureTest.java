package com.graphaware.graphhack;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NodeLocalRelationshipIndexProcedureTest extends BaseTest {

    @Test
    public void shouldLookupRelationshipUnderThreshold() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");


        try (Transaction tx = db.beginTx()) {
            List<Map<String, Object>> result = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                    "RETURN r,other", ImmutableMap.of("date", "2018-04-20"));

            assertThat(result).hasSize(1);

            Map<String, Object> first = (Map<String, Object>) result.get(0);

            Node other = (Node) first.get("other");
            assertThat(other.getProperty("name")).isEqualTo("Prague");
        }
    }

    @Test
    public void shouldNotFindRelationshipWithDifferentProperty() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");

        List<Map<String, Object>> result = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2012-10-01"));

        assertThat(result).hasSize(0);
    }

    @Test
    public void shouldLookupRelationshipOverThreshold() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2019-09-12'}]->(:Place {name:'Bratislava'})");

        try (Transaction tx = db.beginTx()) {
            List<Map<String, Object>> result = execute("MATCH (n:Person {name:'Frantisek'}) " +
                    "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                    "RETURN r,other", ImmutableMap.of("date", "2018-04-20"));

            assertThat(result).hasSize(1);

            Map<String, Object> first = (Map<String, Object>) result.get(0);

            Node other = (Node) first.get("other");
            assertThat(other.getProperty("name")).isEqualTo("Prague");
        }

        // We should be able to look up both
        List<Map<String, Object>> second = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2019-09-12"));

        assertThat(newArrayList(second)).hasSize(1);
    }

    @Test
    public void lookupShouldNotAcceptNulls() {
        assertThatThrownBy(() -> execute("CALL ga.index.lookup(null, 'VISITED', 'date', '2010')"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);

        execute("CREATE (p:Person {name:'Frantisek'})");

        assertThatThrownBy(() -> execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], null, 'date', '2010') YIELD r,other " +
                "RETURN r,other"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', null, '2010') YIELD r,other " +
                "RETURN r,other"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', null) YIELD r,other " +
                "RETURN r,other"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void lookupEmptyNodeArrayShouldReturnEmptyResult() {
        List<Map<String, Object>> result = execute("CALL ga.index.lookup([], 'VISITED', 'date', '2010')");

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldIndexRelationshipsWithSameValue() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Bratislava'})");

        List<Map<String, Object>> result = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2018-04-20"));

        assertThat(result).hasSize(2);
    }
}