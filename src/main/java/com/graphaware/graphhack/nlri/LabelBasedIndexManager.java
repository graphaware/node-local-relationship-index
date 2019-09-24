package com.graphaware.graphhack.nlri;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
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
    private final GraphDatabaseService db;

    private final Set<IndexDescriptor> indexes = ConcurrentHashMap.newKeySet();

    public LabelBasedIndexManager(GraphDatabaseService db, LogService logService) {
        this.db = db;
        this.log = logService.getUserLog(LabelBasedIndexManager.class);

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
        result.close();
        indexes.add(descriptor);
    }

    @Override
    public void drop(IndexDescriptor descriptor) {
        log.info("Deleting index %s", descriptor);
        Map<String, Object> params = new HashMap<>();
        params.put("label", descriptor.getLabel());
        params.put("relType", descriptor.getRelationshipType());
        params.put("property", descriptor.getProperty());

        Result result = db.execute("MATCH (i:Index {label: $label, relationshipType:$relType, property:$property}) DETACH DELETE i",
                params);
        result.close();
        indexes.remove(descriptor);
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
