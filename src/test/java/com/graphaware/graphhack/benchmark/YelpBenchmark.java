package com.graphaware.graphhack.benchmark;

import com.google.common.collect.ImmutableMap;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.Serializable;
import java.util.List;

@State(Scope.Benchmark)
public class YelpBenchmark {

    public static final int RELATIONSHIPS = 10_000;
    public static final int I = RELATIONSHIPS / 2;
    public static final double STARS = 6.0;


    Driver driver;
    long id;

    @Setup
    public void setup() {

        driver = GraphDatabase.driver("bolt://localhost");

    }


    @TearDown
    public void tearDown() {
        driver.close();
    }

    @Benchmark
    public List<Record> getRelationship() {
        try (Session session = driver.session()) {
            ImmutableMap<String, Object> params = ImmutableMap.of("id", "4JNXUYY8wbaaDmk3BPzlWw",
                    "stars", STARS);
            return session.run("match (n:Business {id:$id})<-[r:REVIEWED]-(u:User) where r.stars = $stars return count(u)",
                    params)
            .list();
        }
    }

    @Benchmark
    public List<Record> getRelationshipUsingLookup() {
        try (Session session = driver.session()) {
            ImmutableMap<String, Object> params = ImmutableMap.of("id", "4JNXUYY8wbaaDmk3BPzlWw",
                    "stars", STARS);
            return session.run("MATCH (n:Business {id:$id})  CALL ga.index.lookup([n], 'REVIEWED', 'stars', $stars) YIELD r,other RETURN count(other)",
                    params).list();
        }
    }


    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(YelpBenchmark.class.getSimpleName())
//                .include(RelationshipBenchmark.class.getSimpleName() + ".getRelationshipUsingLookup")
                .warmupIterations(1)
//                .mode(Mode.AverageTime)
                .measurementIterations(3)
                .forks(1)
                .jvmArgs("-ea")
                .build();


        new Runner(opt).run();

    }

}
