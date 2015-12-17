package fr.v3d.elasticsearch.plugin.multiplemetric;

import org.testng.annotations.Test;

import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.MultipleMetric;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.MultipleMetricBuilder;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.MultipleMetricParser;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

public class MultipleMetricAggregatorTest  extends MultipleMetricAggregationTestCase {

    @Test
    public void assertMultipleMetricAggregation() {
        String indexName = "test1";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                        .script("ratio", "value1 / value2")
                        .field("value1", "value1", MultipleMetricParser.SUM_OPERATOR)
                        .field("value2", "value2", MultipleMetricParser.COUNT_OPERATOR))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 45.0 * size);
        assertEquals(metrics.getValue("value2"), 10.0 * size);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"));
    }
    
    @Test
    public void assertMultipleMetricAggregationWithFilter() {
        String indexName = "test2";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                        .script("ratio", "value1 / value2")
                        .field("value1", "value1", MultipleMetricParser.SUM_OPERATOR, new RangeFilterBuilder("value1").gt(5))
                        .field("value2", "value2", MultipleMetricParser.COUNT_OPERATOR))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 30.0 * size);
        assertEquals(metrics.getValue("value2"), 10.0 * size);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"));
    }
    
    @Test
    public void assertMultipleMetricAggregationWithUnmappedField() {
        String indexName = "test3";
        int size = 1;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        
        buildTestDataset(1, indexName, "type1", size, termsFactor);
        
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setQuery(matchAllQuery())
                .addAggregation(new MultipleMetricBuilder("metrics")
                        .script("ratio", "value1 + value2")
                        .field("value1", "value4", MultipleMetricParser.SUM_OPERATOR, new RangeFilterBuilder("value1").gt(5))
                        .field("value2", "value5", MultipleMetricParser.COUNT_OPERATOR))
                .execute().actionGet();
        
        MultipleMetric metrics = searchResponse.getAggregations().get("metrics");
        assertEquals(metrics.getValue("value1"), 0.0);
        assertEquals(metrics.getValue("value2"), 0.0);
        assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") + metrics.getValue("value2"));
    }
    
    @Test
    public void assertMultipleMetricAsTermsSubAggregationOneShard() {
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
                                        .script("ratio", "value1 / value2")
                                        .field("value1", "value1", MultipleMetricParser.SUM_OPERATOR)
                                        .field("value2", "value2", MultipleMetricParser.COUNT_OPERATOR));

        SearchResponse searchResponse = client.prepareSearch(indexName)
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
            assertEquals(metrics.getValue("value1"), 45.0 * size * entry.getValue());
            assertEquals(metrics.getValue("value2"), 10.0 * size);
            assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"));
        }
    }
    
    @Test
    public void assertMultipleMetricAsTermsSubAggregationTwoShard() {
        String indexName = "test5";
        int size = 50;
        int numberOfShards = 2;

        Map<String, Integer> termsFactor = new HashMap<String, Integer>();
        termsFactor.put("foo", 1);
        termsFactor.put("bar", 10);
        termsFactor.put("baz", 100);
        
        buildTestDataset(numberOfShards, indexName, "type1", size, termsFactor);
        
        TermsBuilder termsBuilder = new TermsBuilder("group_by")
                .field("field0")
                .order(Order.aggregation("metrics.ratio", true))
                .subAggregation(new MultipleMetricBuilder("metrics")
                                        .script("ratio", "value1 / value2")
                                        .field("value1", "value1", MultipleMetricParser.SUM_OPERATOR)
                                        .field("value2", "value2", MultipleMetricParser.COUNT_OPERATOR));

        SearchResponse searchResponse = client.prepareSearch(indexName)
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
            assertEquals(metrics.getValue("value1"), 45.0 * size * entry.getValue());
            assertEquals(metrics.getValue("value2"), 10.0 * size);
            assertEquals(metrics.getValue("ratio"), metrics.getValue("value1") / metrics.getValue("value2"));
        }
    }
    
    
}
