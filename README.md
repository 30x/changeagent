# ChangeAgent

ChangeAgent is a small distributed database that is especially well-suited for 
tracking changes to data and distributing them to other systems. It has the
following characteristics:

## Data Model

Each record in the database is also referred to as a "change". Each change
has the following fields:

* Index: A global sequential ordering among all changes in the system
* Tenant ID: An optional unique identifier that groups records for a tenant
* Collection ID: An optional identifier that groups records within a tenant
* Key: An optional unique key that identifies a record inside a collection
* Data: Arbitrary JSON data

Using these records, the data model presents three fundamental collections.

### Global Change List

All records, upon insert, are added to the global change list. This puts them
in a sequential order across all records in the system. No two records will
ever have the same index, and once inserted, the index of a record will
never change.

Clients may query the global change list by index, and ask for any set of
records across the range. This is the primary mechanism for replicating
sets of records between database instances and to clients.

Clients may use an optional long-polling mechanism, in which the client asks
for records that are higher than a certain sequence, and waits for a period
of time until a record is inserted. This allows clients to quickly and 
efficiently replicate their view of the database even if the server is
periodically unavailable or the network is unreliable.

### Tenant-Specific Change List

Records that contain a tenant ID are also added to a tenant-specific change
list. This change list works just like the global change list. Using this
mechanism, a central database can be used to efficiently replicate sets
of data to a large group of clients, without having to send all the data
to each client.

### Collections

Records may optionally contain a collection ID and a key. Records that have
these fields are added to the global and tenant-specific change lists just
like everything else. In addition, query, update, and delete operations may
be performed just like in a traditional key-value store. 

## Design

ChangeAgent is designed to be configured as a cluster. Each server in the 
cluster contains a complete replica of all the data. Clients may query
any replica at any time and ese the most recent set of changes that
have been replicated to all the servers. Clients may modify the database
only when a quorum of servers are operating.
 
The replication model uses the Raft algorithm. This has the following 
implications:

* The servers in the cluster will elect a leader within a few seconds
as long as a quorum ((N / 2) + 1) of nodes are able to talk to one
another.
* If there is no leader, or if an election is in progress, then writes
will fail and clients must be prepared to retry.
* If the leader is lost, as along as a quorum is present a new leader will
be elected within a few seconds
* When new nodes join the cluster, they will be available for reads right
away, but it will take some time to replicate all the data to them.
* All changes will be propagated to all replicas in the same order, no
matter what.
* Raft will work no matter how many nodes are in the cluster. It will
eventually find a leader, even in a two-node cluster.
* However, Raft functions best when there are an odd number of replicas. A three-node
cluster requires two servers to form a quorum, and a five-node cluster 
requires three. However, a four-node cluster also requires three nodes,
and a two-node cluster requires that both nodes be available in order for
any writes to succeed.

## Use Cases



## API

### Using ChangeAgent as a Simple Log

Post a new change to the database:

    POST /changes
    { "data": { "This": "is", "some": "JSON" } }
    
    Response:
    { "_id": 123 }
    
Retrieve the first 100 changes in the system:

    GET /changes?limit=100
    
Retrieve the first 100 changes after change 123:

    GET /changes?since=123&limit=100
    
Retrieve up to 100 changes since change 123, but wait if there are no 
changes available for up to 60 seconds before returning:

    GET /changes?since=123&limit=100&block=60
    
### Using ChangeAgent as a Multi-Tenant Log

Create a tenant:

    POST /tenants
    { "name": "foo" }
    
    { "tenant": "AFA5CA50-1F64-4E45-B95A-CEC797787C7D" }
    
Insert a change to that tenant's log:

    POST /tenants/AFA5CA50-1F64-4E45-B95A-CEC797787C7/changes
    { "data": { "This": "is", "some": "JSON" } }
    
(The change also now appears in the top-level /changes log.)    
   
Get changes specifically for that tenant.

    GET /tenant/AFA5CA50-1F64-4E45-B95A-CEC797787C7/changes?limit=100
    
Address the tenant using a name instead of a UUID:

    GET /tenant/foo/changes?limit=100
    
### Using Collections

Create a collection:

    POST /tenants/foo/collections
    { "name": "bar" }
    
    { "collection": "9E9A8ED4-4A6B-41F2-A68C-698B6F17E8D4" }
    
Insert into the collection:

    POST /collections/9E9A8ED4-4A6B-41F2-A68C-698B6F17E8D4/keys
    { "key": "baz", "data": { "This": "is", "some": "JSON" } }
    
(The data now appears in the top-level "/changes" log as well as the tenant's
change log.)

Retrieve a single item by key:

   GET /collections/9E9A8ED4-4A6B-41F2-A68C-698B6F17E8D4/keys/baz
   
Retrieve collection items in sorted order by key:

    GET /collections/9E9A8ED4-4A6B-41F2-A68C-698B6F17E8D4/keys

## Rationale

## Plans

* Snapshot replication so that new nodes come online more quickly
* Read-only replicas
* Local cached copies