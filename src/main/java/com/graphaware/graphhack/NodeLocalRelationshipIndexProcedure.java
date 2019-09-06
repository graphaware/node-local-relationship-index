package com.graphaware.graphhack;

import com.graphaware.graphhack.nlri.IndexDescriptor;
import com.graphaware.graphhack.nlri.IndexManager;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class NodeLocalRelationshipIndexProcedure {

    @Context
    public GraphDatabaseAPI db;

    private IndexManager indexManager() {
        return db.getDependencyResolver().resolveTypeDependencies(IndexManager.class).iterator().next();
    }

    @Procedure(value = "ga.index.create", mode = Mode.WRITE)
    public void create(@Name("label") String label,
                       @Name("relationshipType") String relationshipType,
                       @Name("property") String property) {
        checkParam("label", label);
        checkParam("relationshipType", relationshipType);
        checkParam("property", property);

        indexManager().create(new IndexDescriptor(label, relationshipType, property));
    }

    private void checkParam(String name, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Parameter " + name + " cannot be null");
        }
    }

    @Procedure("ga.index.list")
    public Stream<IndexResult> list() {
        return indexManager().stream()
                .map(descriptor -> new IndexResult(descriptor.getLabel(), descriptor.getRelationshipType(), descriptor.getProperty()));
    }


    @Procedure("ga.index.lookup")
    public Stream<LookupResult> lookup(@Name("nodes") List<Node> nodes,
                                       @Name("relationshipType") String relationshipType,
                                       @Name("property") String property,
                                       @Name("value") Object value) {
        checkParam("nodes", nodes);
        checkParam("relationshipType", relationshipType);
        checkParam("property", property);
        checkParam("value", value);

        List<Stream<LookupResult>> results = new ArrayList<>();
        for (Node node : nodes) {
            for (Label label : node.getLabels()) {
                IndexDescriptor descriptor = new IndexDescriptor(label.name(), relationshipType, property);

                String localIndexName = descriptor.localName(node.getId());
                if (db.index().existsForRelationships(localIndexName)) {
                    RelationshipIndex index = db.index().forRelationships(localIndexName);
                    IndexHits<Relationship> hits = index.get(property, value);

                    results.add(hits.stream().map(r -> new LookupResult(r, r.getOtherNode(node))));
                } else {
                    List<LookupResult> scanResult = new ArrayList<>();
                    for (Relationship relationship : node.getRelationships(RelationshipType.withName(relationshipType))) {
                        if (value.equals(relationship.getProperty(property))) {
                            scanResult.add(new LookupResult(relationship, relationship.getOtherNode(node)));
                        }
                    }

                    results.add(scanResult.stream());

                }
            }
        }

        // TODO return unique results?
        return results.stream().flatMap(Function.identity());
    }


    public static class LookupResult {

        public final Relationship r;
        public final Node other;

        public LookupResult(Relationship r, Node other) {
            this.r = r;
            this.other = other;
        }
    }

    public static class IndexResult {

        public final String label;
        public final String relationshipType;
        public final String property;


        public IndexResult(String label, String relationshipType, String property) {
            this.label = label;
            this.relationshipType = relationshipType;
            this.property = property;
        }
    }

}
