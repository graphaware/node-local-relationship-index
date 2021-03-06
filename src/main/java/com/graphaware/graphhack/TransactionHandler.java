package com.graphaware.graphhack;

import com.graphaware.graphhack.config.IndexConfig;
import com.graphaware.graphhack.nlri.IndexDescriptor;
import com.graphaware.graphhack.nlri.IndexManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class TransactionHandler extends TransactionEventHandler.Adapter implements Lifecycle {

    private final GraphDatabaseService db;
    private final Log log;
    private final IndexManager indexManager;
    private final Config config;

    public TransactionHandler(GraphDatabaseService db, Log log, IndexManager indexManager, Config config) {
        this.db = db;
        this.log = log;
        this.indexManager = indexManager;
        this.config = config;

        log.info("Created");
    }

    @Override
    public Object beforeCommit(TransactionData data) throws Exception {
        log.debug("Before commit");

        Set<IndexDescriptor> indexes = indexManager.indexes();
        Set<String> relPropNames = indexes.stream().map(IndexDescriptor::getProperty).collect(toSet());

        iterateOverChanges(indexes, relPropNames, new UpdatedProperty(), data.assignedRelationshipProperties());
        iterateOverChanges(indexes, relPropNames, new RemoveProperty(), data.removedRelationshipProperties());

        return super.beforeCommit(data);
    }


    private void iterateOverChanges(Set<IndexDescriptor> indexes,
                                    Set<String> relPropNames,
                                    IndexUpdater updater,
                                    Iterable<PropertyEntry<Relationship>> entries) {

        for (PropertyEntry<Relationship> entry : entries) {
            Relationship relationship = entry.entity();

            // If there is no index with this property there is no point in checking all label - rel type - prop name combinations
            if (relPropNames.contains(entry.key())) {
                for (Node node : relationship.getNodes()) {
                    for (Label label : node.getLabels()) {
                        IndexDescriptor descriptor = new IndexDescriptor(label.name(), relationship.getType().name(), entry.key());
                        String localIndexName = descriptor.localName(node.getId());

                        // This is ugly as *
                        // node.getDegree() should be fast, but isn't in transactions with lot of added relationships to single node
                        // so we pre-check if the index exists and if it does then don't call getDegree at all
                        if (db.index().existsForRelationships(localIndexName) ||
                                // the getDegree() is (maybe?) faster than node.getDegree(type) so call that first
                                ((node.getDegree() >= threshold()) && (node.getDegree(relationship.getType()) >= threshold()))) {

                            if (indexes.contains(descriptor)) {
                                // Index exists

                                updater.accept(entry, node, relationship, descriptor, localIndexName);
                            }
                        }
                    }
                }
            }
        }
    }

    interface IndexUpdater {

        void accept(PropertyEntry<Relationship> entry, Node node, Relationship relationship, IndexDescriptor descriptor, String localIndexName);
    }

    class UpdatedProperty implements IndexUpdater {

        @Override
        public void accept(PropertyEntry<Relationship> entry, Node node, Relationship relationship, IndexDescriptor descriptor, String localIndexName) {
            boolean indexExists = db.index().existsForRelationships(localIndexName);
            if (!indexExists) {
                // Index doesn't exists - we have reached the threshold
                // need to create one and index all pre-existing relationships

                RelationshipIndex index = db.index().forRelationships(localIndexName);
                for (Relationship rel : node.getRelationships(relationship.getType())) {
                    index.add(rel, entry.key(), rel.getProperty(entry.key()));
                }
            }

            RelationshipIndex index = db.index().forRelationships(localIndexName);
            index.add(relationship, entry.key(), entry.value());

            if (indexExists && entry.previouslyCommitedValue() != null) {
                index.remove(relationship, entry.key(), entry.previouslyCommitedValue());
            }
        }
    }

    class RemoveProperty implements IndexUpdater {

        @Override
        public void accept(PropertyEntry<Relationship> entry, Node node, Relationship relationship, IndexDescriptor descriptor, String localIndexName) {
            if (db.index().existsForRelationships(localIndexName)) {
                RelationshipIndex index = db.index().forRelationships(localIndexName);
                index.remove(entry.entity(), entry.key(), entry.previouslyCommitedValue());
            }
        }
    }

    private int threshold() {
        return config.get(IndexConfig.threshold);
    }

    @Override
    public void init() throws Throwable {

    }

    @Override
    public void start() throws Throwable {

    }

    @Override
    public void stop() throws Throwable {

    }

    @Override
    public void shutdown() throws Throwable {

    }
}
