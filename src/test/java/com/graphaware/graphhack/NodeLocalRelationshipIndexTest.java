package com.graphaware.graphhack;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class NodeLocalRelationshipIndexTest extends BaseTest {

    @Test
    public void shouldNotCreateIndexWhenRelationshipUnderThreshold() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");

        List<Map<String, Object>> result = db.execute("CALL db.index.explicit.list()").stream().collect(Collectors.toList());
        assertThat(result).isEmpty();
    }

    @Test
    public void shouldCreateIndexWhenRelationshipsOverThreshold() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2019-09-12'}]->(:Place {name:'Bratislava'})");

        List<Map<String, Object>> result = db.execute("CALL db.index.explicit.list()").stream().collect(Collectors.toList());
        assertThat(result).hasSize(1);

        Long id = (Long) db.execute("MATCH (p:Person {name:'Frantisek'}) RETURN id(p) AS id").next().get("id");

        Map<String, Object> index = result.get(0);
        assertThat(index.get("name")).isEqualTo("Person|VISITED|date|" + id);
    }

    @Test
    public void shouldNotLookupDeletedRelationship() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2019-09-12'}]->(:Place {name:'Bratislava'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2014-07-01'}]->(:Place {name:'London'})");

        List<Map<String, Object>> x = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2014-07-01"));

        assertThat(x).hasSize(1);

        execute("MATCH (p:Person {name:'Frantisek'})-[r:VISITED {date:'2014-07-01'}]->(:Place {name:'London'}) DELETE r");

        List<Map<String, Object>> result = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2014-07-01"));

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldUpdateIndexWhenPropertyIsRemoved() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2019-09-12'}]->(:Place {name:'Bratislava'})");

        execute("MATCH (p:Person {name:'Frantisek'})-[r:VISITED]->(:Place {name:'Prague'}) REMOVE r.date");

        List<Map<String, Object>> result = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2018-04-20"));

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldNotLookupRelationshipWithOldValueAfterUpdate() {
        execute("CALL ga.index.create('Person', 'VISITED', 'date')");

        execute("CREATE (p:Person {name:'Frantisek'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2018-04-20'}]->(:Place {name:'Prague'})");
        execute("MATCH (p:Person {name:'Frantisek'}) CREATE (p)-[:VISITED {date:'2019-09-12'}]->(:Place {name:'Bratislava'})");

        execute("MATCH (p:Person {name:'Frantisek'})-[r:VISITED]->(:Place {name:'Prague'}) SET r.date = '2018-04-21'");

        List<Map<String, Object>> original = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2018-04-20"));

        assertThat(original).isEmpty();

        List<Map<String, Object>> updated = execute("MATCH (n:Person {name:'Frantisek'}) " +
                "CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other " +
                "RETURN r,other", ImmutableMap.of("date", "2018-04-21"));

        assertThat(updated).hasSize(1);
    }

}
