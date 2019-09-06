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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
public class RelationshipBenchmark {

    public static final int RELATIONSHIPS = 10_000;
    public static final int I = RELATIONSHIPS / 2;


    ServerControls controls;
    long id;

    @Setup
    public void setup() {
        controls = new InProcessServerBuilder()
                .withProcedure(NodeLocalRelationshipIndexProcedure.class)
                .newServer();

        GraphDatabaseService db = controls.graph();

        try (Transaction tx = db.beginTx()) {
            db.execute("CALL ga.index.create('Node', 'MY_REL', 'i')").stream().collect(Collectors.toList());

            Result result = db.execute("CREATE (node:Node) RETURN node");
            Node node = (Node) result.next().get("node");
            result.close();
            id = node.getId();

            System.out.println("Creating relationships");
            for (int i = 0; i < RELATIONSHIPS; i++) {
                db.execute("MATCH (node) WHERE id(node) = $id " +
                        "CREATE (node)-[:MY_REL {i:$i}]->(other:Other {name:'other' + $i})",
                        ImmutableMap.of("id", node.getId(), "i", i)).stream().collect(Collectors.toList());
            }
            System.out.println("Creating relationships finished");
            tx.success();
        }
        System.out.println("Creating relationships committed, setup done");
    }

    @TearDown
    public void tearDown() {
        controls.close();
    }

    @Benchmark
    public List<Map<String, Object>> getRelationship() {
        GraphDatabaseService db = controls.graph();

        List<Map<String, Object>> result = db.execute("MATCH (node)-[r:MY_REL {i:$i}]-(other) WHERE id(node) = $id RETURN r,other",
                ImmutableMap.of("id", id, "i", I))
                .stream().collect(Collectors.toList());

        return result;
    }

    @Benchmark
    public List<Map<String, Object>> getRelationshipUsingLookup() {
        GraphDatabaseService db = controls.graph();

        List<Map<String, Object>> result = db.execute("MATCH (node) WHERE id(node) = $id " +
                        "CALL ga.index.lookup([node], 'MY_REL', 'i', $i) YIELD r,other RETURN r,other",
                ImmutableMap.of("id", id, "i", I))
                .stream().collect(Collectors.toList());

        return result;
    }




    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(RelationshipBenchmark.class.getSimpleName())
//                .include(RelationshipBenchmark.class.getSimpleName() + ".getRelationshipUsingLookup")
                .warmupIterations(1)
                .measurementIterations(2)
                .forks(0)
                .jvmArgs("-ea")
                .build();


        new Runner(opt).run();

    }

}
