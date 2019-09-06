package com.graphaware.graphhack;

import com.google.common.collect.ImmutableMap;
import com.graphaware.graphhack.config.IndexConfig;
import com.graphaware.graphhack.factory.IndexManagerFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

public class Neo4jSupport {

    public static GraphDatabaseService neo4j() {
        try {
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .setUserLogProvider(FormattedLogProvider.toOutputStream(System.out))
                    .addKernelExtension(new IndexManagerFactory())
                    .newImpermanentDatabase(ImmutableMap.of(IndexConfig.threshold, "2"));

            Procedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
            procedures.registerProcedure(NodeLocalRelationshipIndexProcedure.class);

            return db;
        } catch (KernelException e) {
            throw new RuntimeException("Could not create database", e);
        }
    }
}
