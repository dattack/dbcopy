This directory contains the configuration files for the JNDI resources availables under the _"jdbc"_ context. You must create a "_*.properties_" file for each datasource you want to use. The filename determines the name of the JNDI resource created. So, if the file "_jdbc/example-sqlite.properties_" exists, then a JNDI resource named "_jdbc/example-sqlite_" will be created.

The properties that you can specify are displayed below:

###### Base configuration

- **type**: this is the JNDI resource type. The value for a JDBC datasource must be  _javax.sql.DataSource_. This property is _**MANDATORY**_.

- **driverClassName**: the fully qualified Java class name of the JDBC driver to be used. This property is _**MANDATORY**_.

    **IMPORTANT**: the JAR file containing the driver-class must be copied into the path _$DBTOOLS/lib/ext_ to be available to the _classloader_.

- **url**: the connection URL to be passed to our JDBC driver to establish a connection. This property is _**MANDATORY**_.

- **username**: the connection username to be passed to our JDBC driver to establish a connection. This property is _**optional**_ and database-dependent.

- **password**: the connection password to be passed to our JDBC driver to establish a connection. This property is _**optional**_ and database-dependent.


###### Extendend configuration

- **connectionProperties**: the connection properties that will be sent to our JDBC driver when establishing new connections.Format of the string must be _[propertyName=property;]*_

- **defaultAutoCommit**: the default auto-commit state of connections created by this datasource. If not set then the setAutoCommit method will not be called.

- **defaultReadOnly**: the default read-only state of connections created by this datasource. If not set then the setReadOnly method will not be called.

- **defaultTransactionIsolation**: the default TransactionIsolation state of connections created by this pool. One of the following:

    - NONE
    - READ_COMMITTED
    - READ_UNCOMMITTED
    - REPEATABLE_READ
    - SERIALIZABLE

- **defaultCatalog**: the default catalog of connections created by this pool.

###### Pool configuration

- **initialSize**: the initial number of connections that are created when the pool is started (default: 0).

- **maxTotal**: the maximum number of active connections that can be allocated from this pool at the same time, or negative for no limit (default: 8)

- **maxIdle**: the maximum number of connections that can remain idle in the pool, without extra ones being released, or negative for no limit (default: 8)

- **minIdle**: the minimum number of connections that can remain idle in the pool, without extra ones being created, or zero to create none (default: 0)

- **maxWaitMillis**: the maximum number of milliseconds that the pool will wait (when there are no available connections) for a connection to be returned before throwing an exception, or -1 to wait indefinitely (default: indefinitely)

- **validationQuery**: the SQL query that will be used to validate connections from this pool before returning them to the caller. If specified, this query MUST be an SQL SELECT statement that returns at least one row.

- **testOnCreate**: the indication of whether objects will be validated after creation. If the object fails to validate, the borrow attempt that triggered the object creation will fail (default: false)

- **testOnBorrow**: the indication of whether objects will be validated before being borrowed from the pool. If the object fails to validate, it will be dropped from the pool, and we will attempt to borrow another (default: true)

- **testOnReturn**: the indication of whether objects will be validated before being returned to the pool (default: false)

- **testWhileIdle**: the indication of whether objects will be validated by the idle object evictor (if any). If an object fails to validate, it will be dropped from the pool (default: false)

- **timeBetweenEvictionRunsMillis**: the number of milliseconds to sleep between runs of the idle object evictor thread. When non-positive, no idle object evictor thread will be run (default: -1)

- **numTestsPerEvictionRun**: the number of objects to examine during each run of the idle object evictor thread (default: 3) 

- **minEvictableIdleTimeMillis**: the minimum amount of time an object may sit idle in the pool before it is eligable for eviction by the idle object evictor (default: 1000 * 60 * 30 = 1800000)

- **poolPreparedStatements**: enable prepared statement pooling for this pool (default: false)

- **maxOpenPreparedStatements**: the maximum number of open statements that can be allocated from the statement pool at the same time, or negative for no limit.

- **removeAbandoned**: flag to remove abandoned connections if they exceed the removeAbandonedTimout (default: false)

- **removeAbandonedTimeout**: timeout in seconds before an abandoned connection can be removed (default: 300)

- **logAbandoned**: flag to log stack traces for application code which abandoned a Statement or Connection (default: false)

###### Example

For example, if we need to access a MySQL database, we could create a JNDI resource named _"jdbc/example-mysql"_:

1.- Create a properties file named _example-mysql.properties_ into the directory _jdbc_.

    $DBTOOLS_HOME/etc/jndi/jdbc/example-mysql.properties

2.- Edit the properties file _example-mysql.properties_ and configure the needed properties:

    type=javax.sql.DataSource
    
    # basic configuration
    driverClassName=com.mysql.jdbc.Driver
    url=jdbc:mysql://<dbserver>:3306
    username=<dbuser>
    password=<changeit>
    
    # pool configuration
    initialSize=0
    maxTotal=8
    maxIdle=1
    minIdle=0
    maxWaitMillis=1000
    
3.- Finally, copy the JAR that contains the JDBC driver classes into the directory _"$DBTOOLS_HOME/lib/ext"_.
