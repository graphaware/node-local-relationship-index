package com.graphaware.graphhack.nlri;

import com.graphaware.graphhack.config.IndexConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class LabelBasedIndexManager extends LifecycleAdapter implements IndexManager {

    private final Log log;
    private final Config config;
    private final GraphDatabaseService db;

    private final Set<IndexDescriptor> indexes = ConcurrentHashMap.newKeySet();

    public LabelBasedIndexManager(GraphDatabaseService db, LogService logService, Config config) {
        this.db = db;
        this.log = logService.getUserLog(LabelBasedIndexManager.class);
        this.config = config;

        log.info("Created instance of %s", LabelBasedIndexManager.class);
    }

    @Override
    public void create(IndexDescriptor descriptor) {
        log.info("Create index %s", descriptor);
        Map<String, Object> params = new HashMap<>();
        params.put("label", descriptor.getLabel());
        params.put("relType", descriptor.getRelationshipType());
        params.put("property", descriptor.getProperty());
        Result result = db.execute("MERGE (i:Index {label: $label, relationshipType:$relType, property:$property})",
                params
        );

        int nodesCreated = result.getQueryStatistics().getNodesCreated();
        if (nodesCreated == 1) {

            createNodeLocalIndexes(descriptor);
        }
        result.close();
        indexes.add(descriptor);
    }

    private void createNodeLocalIndexes(IndexDescriptor descriptor) {
        Result nodes = nodesOverThreshold(descriptor);

        RelationshipType relType = RelationshipType.withName(descriptor.getRelationshipType());
        String propertyName = descriptor.getProperty();
        for (Result it = nodes; it.hasNext(); ) {
            Node node = (Node) it.next().get("n");

            String localIndexName = descriptor.localName(node.getId());
            RelationshipIndex index = db.index().forRelationships(localIndexName);
            for (Relationship rel : node.getRelationships(relType)) {
                index.add(rel, propertyName, rel.getProperty(propertyName));
            }
        }

        nodes.close();
    }

    @Override
    public void drop(IndexDescriptor descriptor) {
        log.info("Deleting index %s", descriptor);
        Map<String, Object> params = new HashMap<>();
        params.put("label", descriptor.getLabel());
        params.put("relType", descriptor.getRelationshipType());
        params.put("property", descriptor.getProperty());

        Result result = db.execute("MATCH (i:Index {label: $label, relationshipType:$relType, property:$property}) DETACH DELETE i", params);
        result.close();
        dropNodeLocalIndexes(descriptor);
        indexes.remove(descriptor);
    }

    private void dropNodeLocalIndexes(IndexDescriptor descriptor) {
        Result nodes = nodesOverThreshold(descriptor);

        for (Result it = nodes; it.hasNext(); ) {
            Node node = (Node) it.next().get("n");

            String localIndexName = descriptor.localName(node.getId());
            if (db.index().existsForRelationships(localIndexName)) {
                db.index().forRelationships(localIndexName).delete();
            }
        }
        nodes.close();
    }

    private Result nodesOverThreshold(IndexDescriptor descriptor) {
        Map<String, Object> params = new HashMap<>();
        params.put("threshold", config.get(IndexConfig.threshold));
        return db.execute(nodesOverThresholdQuery(descriptor.getLabel(), descriptor.getRelationshipType()), params);
    }

    private String nodesOverThresholdQuery(String label, String relType) {
        return "MATCH (n:" + label + ") WHERE size((n)-[:" + relType + "]-()) >= $threshold RETURN n";
    }

    @Override
    public Set<IndexDescriptor> indexes() {
        // TODO this is not very nice, I tried to load it in start() method of the Lifecycle, but it blocks the database start
        if (indexes.isEmpty()) {
            load();
        }

        return indexes;
    }

    @Override
    public Stream<IndexDescriptor> stream() {
        if (indexes.isEmpty()) {
            load();
        }

        return indexes.stream();
    }

    private void load() {
        db.execute("MATCH (i:Index) RETURN i.label,i.relationshipType, i.property")
                .stream()
                .map(r -> new IndexDescriptor(
                        (String) r.get("i.label"),
                        (String) r.get("i.relationshipType"),
                        (String) r.get("i.property")
                )).forEach(indexes::add);
    }

}
