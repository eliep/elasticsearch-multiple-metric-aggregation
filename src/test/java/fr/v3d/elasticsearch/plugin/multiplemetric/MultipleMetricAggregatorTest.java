package fr.v3d.elasticsearch.plugin.multiplemetric;

import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.CountBuilder;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.MultipleMetric;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.MultipleMetricBuilder;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.ScriptBuilder;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.SumBuilder;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.junit.Test;

public class MultipleMetricAggregatorTest  extends MultipleMetricAggregationTestCase {

	@Test
    public void assertPluginLoaded() {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo()
                .clear().setPlugins(true).get();
        logger.info("{}", nodesInfoResponse);
        assertNotNull(nodesInfoResponse.getNodes()[0].getPlugins().getPluginInfos());
        boolean hasGroovy = false;
        boolean hasMultipleMetricAggregation = false;
        for (PluginInfo info: nodesInfoResponse.getNodes()[0].getPlugins().getPluginInfos()) {
            if (info.getName().equals("lang-groovy"))
                hasGroovy = true;
            if (info.getName().equals("multiple-metric-aggregation"))
                hasMultipleMetricAggregation = true;
        }
        assertTrue(hasGroovy);
        assertTrue(hasMultipleMetricAggregation);
    }

	@Test
    public void assertMultipleMetricAggregation() {
        String indexName = "test1";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 / value2")))
                		.field(new SumBuilder("value1").field("value1"))
                		.field(new CountBuilder("value2").field("value2")))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 45.0 * size, 0.0);
        assertEquals(metrics.getValue("value2"), 10.0 * size, 0.0);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"), 0.0);
        
        assertEquals(metrics.getDocCount("value1"), 10);
        assertEquals(metrics.getDocCount("value2"), 10);
    }

	@Test
    public void assertMultipleMetricAggregationWithScriptError() {
        String indexName = "test1";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 / value2")))
                		.field(new SumBuilder("value1").field("value1"))
                		.field(new CountBuilder("value2").field("value2").filter(new RangeQueryBuilder("value1").gt(1000))))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 45.0 * size, 0.0);
        assertEquals(metrics.getValue("value2"), 0.0 * size, 0.0);
        assertEquals(metrics.getValue("ratio"), Double.POSITIVE_INFINITY, 0.0);
        
        assertEquals(metrics.getDocCount("value1"), 10);
        assertEquals(metrics.getDocCount("value2"), 0);
    }

	@Test
    public void assertMultipleMetricAggregationWithScriptedField() {
        String indexName = "test1";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 / value2")))
                		.field(new SumBuilder("value1").script(new Script("doc['value1'].value + doc['value2'].value")))
                		.field(new CountBuilder("value2").field("value2")))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 495.0 * size, 0.0);
        assertEquals(metrics.getValue("value2"), 10.0 * size, 0.0);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"), 0.0);
        
        assertEquals(metrics.getDocCount("value1"), 10);
        assertEquals(metrics.getDocCount("value2"), 10);
    }
    

	@Test
    public void assertMultipleMetricAggregationWithScriptParams() {
        String indexName = "test1";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 * p / value2")).param("p", 2))
                		.field(new SumBuilder("value1").script(new Script("doc['value1'].value + doc['value2'].value")))
                		.field(new CountBuilder("value2").field("value2")))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 495.0 * size, 0.0);
        assertEquals(metrics.getValue("value2"), 10.0 * size, 0.0);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") * 2 / metrics.getValue("value2"), 0.0);
        
        assertEquals(metrics.getDocCount("value1"), 10);
        assertEquals(metrics.getDocCount("value2"), 10);
    }

	@Test
    public void assertMultipleMetricAggregationWithFilter() {
        String indexName = "test2";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 / value2")))
                		.field(new SumBuilder("value1").field("value1").filter(new RangeQueryBuilder("value1").gt(5)))
                		.field(new CountBuilder("value2").field("value2")))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 30.0 * size, 0.0);
        assertEquals(metrics.getValue("value2"), 10.0 * size, 0.0);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"), 0.0);
        
        assertEquals(metrics.getDocCount("value1"), 4);
        assertEquals(metrics.getDocCount("value2"), 10);
    }

	@Test
    public void assertMultipleMetricAggregationWithUnmappedField() {
        String indexName = "test3";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 + value2")))
                		.field(new SumBuilder("value1").field("value4").filter(new RangeQueryBuilder("value1").gt(5)))
                		.field(new CountBuilder("value2").field("value5")))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 0.0, 0.0);
        assertEquals(metrics.getValue("value2"), 0.0, 0.0);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") + metrics.getValue("value2"), 0.0);
        
        assertEquals(metrics.getDocCount("value1"), 0);
        assertEquals(metrics.getDocCount("value2"), 0);
    }

	@Test
    public void assertMultipleMetricAsTermsSubAggregation() {
        String indexName = "test4";
        int size = 1;
        int numberOfShards = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        termsFactor.put("bar", 10);
        termsFactor.put("baz", 100);
        
        buildTestDataset(numberOfShards, indexName, "type1", size, termsFactor);
        
        TermsBuilder termsBuilder = new TermsBuilder("group_by")
                .field("field0")
                .order(Order.aggregation("metrics.ratio", true))
                .subAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 / value2")))
                		.field(new SumBuilder("value1").field("value1"))
                		.field(new CountBuilder("value2").field("value2")));

        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(termsBuilder)
                .execute().actionGet();
        
        Terms terms = searchResponse.getAggregations().get("group_by");
        assertNotNull(terms);
        assertEquals(terms.getBuckets().size(), termsFactor.size());
        
        for (Map.Entry<String, Integer> entry: termsFactor.entrySet()) {
            String term = entry.getKey();
            assertNotNull(terms.getBucketByKey(term));
            assertNotNull(terms.getBucketByKey(term).getAggregations());
            assertNotNull(terms.getBucketByKey(term).getAggregations().get("metrics"));

            MultipleMetric metrics = terms.getBucketByKey(term).getAggregations().get("metrics");
            assertEquals(metrics.getValue("value1"), 45.0 * size * entry.getValue(), 0.0);
            assertEquals(metrics.getValue("value2"), 10.0 * size, 0.0);
            assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"), 0.0);
            
            assertEquals(metrics.getDocCount("value1"), 10);
            assertEquals(metrics.getDocCount("value2"), 10);
        }
    }

	@Test
    public void assertMultipleMetricAsEmptyTermsSubAggregation() {
        String indexName = "test4";
        int size = 1;
        int numberOfShards = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        termsFactor.put("bar", 10);
        termsFactor.put("baz", 100);
        
        buildTestDataset(numberOfShards, indexName, "type1", size, termsFactor);
        
        TermsBuilder termsBuilder = new TermsBuilder("group_by")
                .field("field0")
                .size(termsFactor.size())
                .minDocCount(0L) // we force empty bucket to be returned
                .order(Order.aggregation("metrics.ratio", true))
                .subAggregation(new MultipleMetricBuilder("metrics")
                		.script(new ScriptBuilder("ratio").script(new Script("value1 / value2")))
                		.field(new SumBuilder("value1").field("value1"))
                		.field(new CountBuilder("value2").field("value2")));

        SearchResponse searchResponse = client().prepareSearch(indexName)
                .setQuery(new TermQueryBuilder("field0", "buz"))
                .addAggregation(termsBuilder)
                .execute().actionGet();
        
        Terms terms = searchResponse.getAggregations().get("group_by");
        assertNotNull(terms);
        assertEquals(terms.getBuckets().size(), termsFactor.size());
        
        for (Map.Entry<String, Integer> entry: termsFactor.entrySet()) {
            String term = entry.getKey();
            assertNotNull(terms.getBucketByKey(term));
            assertNotNull(terms.getBucketByKey(term).getAggregations());
            assertNotNull(terms.getBucketByKey(term).getAggregations().get("metrics"));

            MultipleMetric metrics = terms.getBucketByKey(term).getAggregations().get("metrics");
            assertEquals(metrics.getValue("value1"), 0.0, 0.0);
            assertEquals(metrics.getValue("value2"), 0.0, 0.0);
            assertEquals(0.0, metrics.getValue("ratio"), 0.0);
            
            assertEquals(metrics.getDocCount("value1"), 0);
            assertEquals(metrics.getDocCount("value2"), 0);
        }
    }
    
    
}
