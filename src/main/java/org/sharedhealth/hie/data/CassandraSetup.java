package org.sharedhealth.hie.data;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.toddfast.mutagen.Plan.Result;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.impl.CassandraMutagenImpl;

import java.io.IOException;
import java.util.Properties;

public class CassandraSetup {

    public static final int MILLIS_IN_MIN = 60 * 1000;

    public void applyScripts(String scriptDirPath, Properties properties) throws IOException {
        String keyspace = properties.getProperty("cassandra.keyspace");
        Cluster cluster = connectCluster(properties);
        CassandraMutagen mutagen = new CassandraMutagenImpl(keyspace);

        try (Session session = cluster.connect(keyspace)) {
            mutagen.initialize(scriptDirPath);
            Result<Integer> result = mutagen.mutate(new CassandraSubject(session, keyspace));

            if (result.getException() != null) {
                throw new RuntimeException(result.getException());
            } else if (!result.isMutationComplete()) {
                throw new RuntimeException("Failed to apply cassandra migrations");
            }
        } finally {
            cluster.close();
        }
    }

    protected Cluster connectCluster(Properties properties) {
        Cluster.Builder clusterBuilder = new Cluster.Builder();

        QueryOptions queryOptions = new QueryOptions();
        clusterBuilder
                .withClusterName(properties.getProperty("cassandra.keyspace"))
                .addContactPoint(properties.getProperty("cassandra.host"))
                .withPort(Integer.parseInt(properties.getProperty("cassandra.port")))
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withPoolingOptions(new PoolingOptions())
                .withQueryOptions(queryOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(MILLIS_IN_MIN));
        queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);

        return clusterBuilder.build();

    }
}
