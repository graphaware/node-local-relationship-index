package com.graphaware.graphhack;

import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.tuple;

public class IndexManagementTest extends BaseTest {

    @Test
    public void createAndListIndexes() {
        db.execute("CALL ga.index.create('Person', 'VISITED', 'date')").close();

        Result r = db.execute("CALL ga.index.list()");

        List<Map<String, Object>> result = newArrayList(r);

        assertThat(result).extracting(item -> tuple(item.get("label"), item.get("relationshipType"), item.get("property")))
                .containsExactly(tuple("Person", "VISITED", "date"));
    }

    @Test
    public void dropIndex() {
        fail("TODO Implement drop index");
    }

    @Test
    public void createProcedureShouldCheckParameters() {
        assertThatThrownBy(() -> db.execute("CALL ga.index.create(null, 'VISITED', 'date')"))
                .isInstanceOf(QueryExecutionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> db.execute("CALL ga.index.create('Person', null, 'date')"))
                .isInstanceOf(QueryExecutionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> db.execute("CALL ga.index.create('Person', 'VISITED', null)"))
                .isInstanceOf(QueryExecutionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }
}
