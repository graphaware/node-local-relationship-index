package com.graphaware.graphhack.factory;

import com.graphaware.graphhack.TransactionHandler;
import com.graphaware.graphhack.nlri.LabelBasedIndexManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

public class IndexManagerFactory extends KernelExtensionFactory<IndexManagerFactory.Dependencies> {

    public IndexManagerFactory() {
        super(ExtensionType.DATABASE, "node-local-relationship-index-manager");
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) {
        LabelBasedIndexManager indexManager = new LabelBasedIndexManager(dependencies.db(), dependencies.logService());

        TransactionHandler handler = new TransactionHandler(dependencies.db(),
                dependencies.logService().getUserLog(TransactionHandler.class),
                indexManager,
                dependencies.config());

        dependencies.db().registerTransactionEventHandler(handler);

        return indexManager;
    }

    public interface Dependencies {

        GraphDatabaseService db();

        LogService logService();

        Config config();
    }
}
