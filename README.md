# Node Local Relationship Indexes (nlri)

## Motivation

The supernode problem is a common issue in graph database space. 
Being able to do following query efficiently on nodes with thousands, millions or more relationships would make a whole new range of use cases possible with Neo4j.  

```
MATCH (node)-[r:MY_REL {i: $i}]-(other)
 WHERE id(node) = $id
RETURN r, other
```

This is a proof-of-concept which shows it is possible to make such lookups fast.

## Usage

Deploy the node-local-relationship-index.jar to plugins directory.

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
(this is a dynamic config property and can be changed at runtime via `CALL dbms.setConfigValue` on EE)

## Implementation

1. A transaction handler checks all modifications to relationships in each transaction before commit.
2. For each node with more relationships than defined threshold it creates an explicit index where it indexes all relationships of that node.
**The index is local to that node.**
3. The lookup procedure then checks if a node local index exists, and if it does it uses the index to get the relationships,
otherwise it iterates over all the neighbours.

## Index Free Adjacency

Does this break index free adjacency? We argue that it doesn't. 
With index free adjacency you can get from one node to other node in constant time for single relationship.
If you traverse n relationships it will take linear time with respect to number of relationships - _O(n)_.
Most importantly total number of elements in the whole graph doesn't matter.
With node local indexes it will make an index lookup, such operations are usually performed in logarithmic time - _O(log(n))_.
The _n_ is the number of relationships of the node, again total number of elements in the whole graph doesn't matter.

## Known issues

- Index creation hasn't been checked or tested for concurrency issues.

- Lookup with large number of results - when querying data with small number of distinct values (e.g. only 1 to 5 - stars in a review) 
the lookup is slower than normal traversal. The lookup becomes more efficient around 9-10 distinct values. 

- More efficient data structures could be used in certain places (e.g. HashSet used when number of elements is very small)

- Extensive allocation of `IndexDescriptor` class when iterating over changes in a transaction, we preferred readability there for this POC

The performance impact of these is not exactly clear. Would need to create focused microbenchmarks. 

## More work to do

- Move index definition from the db itself (node with special label) to a better place

- Support direction of the relationship - now the relationship is indexed regardless of the direction, provide ability 
to define an index on outgoing/incoming direction

- Use new native indexes instead of explicit indexes - we considered this at the beginning, but decided against it due 
to the complexity of such solution. We hope Neo4j will expose more of the native indexing functionality in 4.0.

- Support range queries - due to the limitation of explicit indexes we didn't implement range queries, it should also 
support returning results in sorted order  