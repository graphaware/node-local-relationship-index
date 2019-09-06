# Node Local Relationship Indexes (nlri)


## Setup

Deploy the node-local-relationship-index.jar to plugins directory.

## Usage

Define the index

```
CALL ga.index.create('Person', 'VISITED', 'date')
```

Lookup using the index:
```
MATCH (n:Person {name:'Frantisek'}) 
CALL ga.index.lookup([n], 'VISITED', 'date', $date) YIELD r,other 
RETURN r,other
```

## Configuration

`com.graphaware.nlri.threshold` - configures threshold for creation of node local indexes, 
when number of relationships from a node with a given label is over this threshold only then is the index created





