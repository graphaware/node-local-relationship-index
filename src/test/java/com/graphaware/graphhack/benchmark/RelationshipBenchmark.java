package com.graphaware.graphhack.benchmark;

import com.google.common.collect.ImmutableMap;
import com.graphaware.graphhack.NodeLocalRelationshipIndexProcedure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.internal.InProcessServerBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.Math.min;

@State(Scope.Benchmark)
public class RelationshipBenchmark {

    private ServerControls controls;
    private long nodeId;

//    @Param({"1000"})
    @Param({"1000", "5000", "10000", "50000", "100000", "500000", "1000000"})
    private int relationships;

    @Param({"1000000"})
//    @Param({"5", "10", "50", "100", "500", "1000"})
    private int values;

    private int matchValue;

    @Setup
    public void setup() {
        controls = new InProcessServerBuilder(new File("target/graph.db"))
                .withProcedure(NodeLocalRelationshipIndexProcedure.class)
                .newServer();

        GraphDatabaseService db = controls.graph();

        createNode(db);

        // create few relationships first so the node becomes dense node
        createRelationships(db, nodeId, 0, 100);
        createRelationships(db, nodeId, 100, relationships);
        System.out.println("Creating relationships committed, setup done");

        // It shouldn't matter which value we match
        matchValue = min(values, relationships) / 2;
        System.out.println("Match value " + matchValue);
    }

    private void createNode(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            consume(db.execute("CALL ga.index.create('Node', 'MY_REL', 'i')"));

            Result result = db.execute("CREATE (node:Node) RETURN node");
            Node node = (Node) result.next().get("node");
            nodeId = node.getId();
            result.close();

            tx.success();
        }
    }

    private void consume(Result result) {
        while (result.hasNext()) {
            result.next();
        }
        result.close();
    }

    private void createRelationships(GraphDatabaseService db, long nodeId, int start, int end) {
        try (Transaction tx = db.beginTx()) {
            for (int i = start; i < end; i++) {

                int value = i % values;

                consume(db.execute("MATCH (node) WHERE id(node) = $id " +
                                "CREATE (node)-[:MY_REL {i:$i}]->(other:Other {name:'other' + $i})",
                        of("id", nodeId, "i", value)));
            }
            tx.success();
        }

    }

    @TearDown
    public void tearDown() {
        controls.close();
    }

    @Benchmark
    public List<Map<String, Object>> cypherMatch() {
        GraphDatabaseService db = controls.graph();

        List<Map<String, Object>> result = db.execute("MATCH (node)-[r:MY_REL {i:$i}]-(other) WHERE id(node) = $id RETURN r,other",
                of("id", nodeId, "i", matchValue))
                .stream().collect(Collectors.toList());

        return result;
    }

    @Benchmark
    public List<Map<String, Object>> lookupProcedure() {
        GraphDatabaseService db = controls.graph();

        List<Map<String, Object>> result = db.execute("MATCH (node) WHERE id(node) = $id " +
                        "CALL ga.index.lookup([node], 'MY_REL', 'i', $i) YIELD r,other RETURN r,other",
                of("id", nodeId, "i", matchValue))
                .stream().collect(Collectors.toList());

        return result;
    }

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(RelationshipBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .forks(1)
                .resultFormat(ResultFormatType.CSV)
                .build();


        new Runner(opt).run();

    }

}
