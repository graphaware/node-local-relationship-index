package com.graphaware.graphhack.nlri;

import java.util.Objects;

public class IndexDescriptor {

    private final String label;
    private final String relationshipType;
    private final String property;

    private final String name;

    public IndexDescriptor(String label, String relationshipType, String property) {
        this.label = label;
        this.relationshipType = relationshipType;
        this.property = property;
        name = label + "|" + relationshipType + "|" + property;
    }

    public String getLabel() {
        return label;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    public String getProperty() {
        return property;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor that = (IndexDescriptor) o;
        return Objects.equals(label, that.label) &&
                Objects.equals(relationshipType, that.relationshipType) &&
                Objects.equals(property, that.property);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, relationshipType, property);
    }

    public String name() {
        return name;
    }

    public String localName(long l) {
        return name + "|" + l;
    }

    @Override
    public String toString() {
        return "IndexDescriptor{" +
                "label='" + label + '\'' +
                ", relationshipType='" + relationshipType + '\'' +
                ", property='" + property + '\'' +
                '}';
    }
}
