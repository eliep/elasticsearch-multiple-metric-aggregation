package fr.v3d.elasticsearch.plugin.multiplemetric;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MultipleMetricAggregationTestCase extends Assert {


    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");

    protected final String CLUSTER = "test-cluster-" + NetworkUtils.getLocalAddress().getHostName();

    private Node node;
    private Node node2;

    protected Client client;
    

    @BeforeClass
    public void startNode() {
        ImmutableSettings.Builder finalSettings = settingsBuilder()
                .put("cluster.name", CLUSTER)
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("node.local", true)
                .put("gateway.type", "none")
                .put("script.disable_dynamic", false);
        node = nodeBuilder().settings(finalSettings.put("node.name", "node1").build()).build().start();
        node2 = nodeBuilder().settings(finalSettings.put("node.name", "node2").build()).build().start();

        client = node.client();
    }

    @AfterClass
    public void stopNode() {
        node.close();
        node2.close();
    }
    

    public void createIndex(int numberOfShards, String indexName) {
        client.admin().indices()
        .prepareCreate(indexName)
        .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards))
        .execute().actionGet();
    }

    public void deleteIndex(String indexName) {
    	try {
	        client.admin().indices()
	        .prepareDelete(indexName)
	        .execute().actionGet();
    	} catch (IndexMissingException ime) { /* ignoring */ }
    }
    
    public void buildTestDataset(int numberOfShards, String indexName, String typeName, int size, Map<String, Integer> termsFactor) {
    	deleteIndex(indexName);
        createIndex(numberOfShards, indexName);
        
        
        for (int i=0; i < size; i++) {
            for (Map.Entry<String, Integer> entry: termsFactor.entrySet()) {
                for (int j = 0; j < 10; j++) {
                    Map<String, Object> doc = new HashMap<String, Object>();
                    doc.put("field0", entry.getKey());
                    doc.put("value1", j * entry.getValue());
                    doc.put("value2", j * 10 * entry.getValue());
                    client.prepareIndex(indexName, typeName, ("doc"+entry.getKey()+i)+j).setSource(doc).setRefresh(true).execute().actionGet();
                }
            }
        }
    }
}
