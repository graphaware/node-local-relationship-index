package com.graphaware.graphhack.nlri;

import java.util.Set;
import java.util.stream.Stream;

public interface IndexManager {

    void create(IndexDescriptor descriptor);

    Set<IndexDescriptor> indexes();

    Stream<IndexDescriptor> stream();

    void drop(IndexDescriptor indexDescriptor);
}
