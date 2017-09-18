# Introduction
==============

[![Join the chat at https://gitter.im/linkedin/databus](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/linkedin/databus?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

In Internet architectures, data systems are typically categorized into source-of-truth systems that serve as primary stores for the user-generated writes, and derived data stores or indexes which serve reads and other complex queries. The data in these secondary stores is often derived from the primary data through custom transformations, sometimes involving complex processing driven by business logic. Similarly, data in caching tiers is derived from reads against the primary data store, but needs to get invalidated or refreshed when the primary data gets mutated. A fundamental requirement emerging from these kinds of data architectures is the need to reliably capture, flow and process primary data changes.

We have built Databus, a source-agnostic distributed change data capture system, which is an integral part of LinkedIn's data processing pipeline. The Databus transport layer provides latencies in the low milliseconds and handles throughput of thousands of events per second per server while supporting infinite look back capabilities and rich subscription functionality. 

# Use-cases
*****
Typically, Primary OLTP data-stores take user facing writes and some reads, while other specialized systems serve complex queries or accelerate query results through caching. The most common data systems found in these architectures include relational databases, NoSQL data stores, caching engines, search indexes and graph query engines. This specialization has in turn made it critical to have a reliable and scalable data pipeline that can capture these changes happening for primary source-of-truth systems and route them through the rest of the 
complex data eco-system. There are two families of solutions that are typically used for building such a pipeline.

### Application-driven Dual Writes:
In this model, the application layer writes to the database and in parallel, writes to another messaging system. This looks simple to implement since the application code writing to the database is under our control. However it introduces a consistency problem because without a complex co-ordination protocol (e.g. Paxos or 2-Phase Commit ) it is hard to ensure that both the database and the messaging system are in complete lock step with each other in the face of failures. Both systems need to process exactly the same writes and need to serialize them in exactly the same order. Things get even more complex if the writes are conditional or have partial update semantics.

### Database Log Mining: 
In this model, we make the database the single source-of-truth and extract changes from its transaction or commit log. This solves our consistency issue, but is practically hard to implement
because databases like Oracle and MySQL (the primary data stores in use at LinkedIn) have transaction log formats and replication solutions that are proprietary and not guaranteed to have 
stable on-disk or on-the-wire representations across version upgrades.  Since we want to process the data changes with application code and then write to secondary data stores,
we need the replication system to be user-space and source-agnostic. This independence from the data source is especially important in fast-moving technology companies, because it avoids 
technology lock-in and tie-in to binary formats throughout the application stack.

After evaluating the pros and cons of the two approaches, we decided to pursue the log mining option, prioritizing consistency and "single source of truth" over ease of implementation. In this paper, we introduce Databus, Change Data Capture pipeline at LinkedIn, which supports Oracle sources and a wide range of downstream applications. The Social Graph Index which serves all graph queries at LinkedIn, the People Search Index that powers all searches for members at LinkedIn and the various read replicas for our Member Profile data are all fed and kept consistent via Databus.

More details about the architecture, usecases and performance evaluation can be obtained from a paper that got accepted for publication at the ACM Symposium on Cloud Computing - 2012. The slides for the presentation are available [here](http://www.slideshare.net/ShirshankaDas/databus-socc-2012)

# How to build ?
*****
Databus requires a library distributed by Oracle Inc under Oracle Technology Network License. Please accept that license [here](http://www.oracle.com/technetwork/licenses/distribution-license-152002.html), and download ojdbc6.jar with version at 11.2.0.2.0 [here](http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html). Once you download the driver jar, please copy it under sandbox-repo/com/oracle/ojdbc6/11.2.0.2.0/ and name it ojdbc6-11.2.0.2.0.jar as shown below. We have provided a sample .ivy file to facilitate the build.

Databus will **NOT** build without this step. After downloading the jars, they may be copied under the directory sandbox-repo as :
* sandbox-repo/com/oracle/ojdbc6/11.2.0.2.0/ojdbc6-11.2.0.2.0.jar
* sandbox-repo/com/oracle/ojdbc6/11.2.0.2.0/ojdbc6-11.2.0.2.0.ivy

# Build System
*****
Databus currently needs gradle version 1.0 or above to build. The commands to build are :
* `gradle -Dopen_source=true assemble` -- builds the jars and command line package
* `gradle -Dopen_source=true clean`    -- cleans the build directory
* `gradle -Dopen_source=true test`     -- runs all the unit-tests that come packaged with the source

# Licensing
*****
Databus will be licensed under Apache 2.0 license.

# Full Documentation
*****
See our [wiki](https://github.com/linkedin/databus/wiki) for full documentation and examples.

# Example Relay
*****
An example of writing a DatabusRelay is available at PersonRelayServer.java. To be able to start a relay process, the code is packaged into a startable command-line package. The tarball may be obtained from build/databus2-example-relay-pkg/distributions/databus2-example-relay-pkg.tgz. This relay is configured to get changestreams for a view "Person".

After extracting to a directory, please cd to that directory and start the relay using the following command :
* `./bin/start-example-relay.sh person`

If the relay is started successfully, the output of the following curl command would look like :
* $ `curl http://localhost:11115/sources`
* `[{“name”:“com.linkedin.events.example.person.Person”,“id”:40}]`

# Example Client
*****
An example of writing a DatabusClient is available at PersonClientMain.java. To easily be able to start the client process, the code is packaged into a startable command-line package. The tarball may be obtained from build/databus2-example-client-pkg/distributions/databus2-example-client-pkg.tgz. This client is configured to get data from the relay started previously, and configured to susbscribe for table Person.

After extracting to a directory, please cd to that directory and start the client using the following command :
* `./bin/start-example-client.sh person`

If the client successfully connects to the relay we created earlier, the output of the following curl command would look like below ( indicating a client from localhost has connected to the relay ):
* $`curl http://localhost:11115/relayStats/outbound/http/clients`
* `["localhost"]`
