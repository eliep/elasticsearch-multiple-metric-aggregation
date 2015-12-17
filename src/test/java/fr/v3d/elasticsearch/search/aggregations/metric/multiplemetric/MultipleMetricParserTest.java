package fr.v3d.elasticsearch.search.aggregations.metric.multiplemetric;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.testng.annotations.Test;

import fr.v3d.elasticsearch.plugin.multiplemetric.MultipleMetricAggregationTestCase;

public class MultipleMetricParserTest extends MultipleMetricAggregationTestCase {

	@Test(expectedExceptions=SearchPhaseExecutionException.class)
    public void assertMissingFieldOrScript() throws Exception {
		String indexName = "index0";
		int numberOfShards = 1;
		
		createIndex(numberOfShards, indexName);
        
        client.prepareSearch("index0").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject("metrics")
                    .startObject("value1")
                        .field("operator", "sum")
                    .endObject()
                .endObject()
            .endObject()).execute().actionGet();
    }

	@Test(expectedExceptions=SearchPhaseExecutionException.class)
    public void assertMissingOperator() throws Exception {
		String indexName = "index1";
		int numberOfShards = 1;
		
		createIndex(numberOfShards, indexName);
        
        client.prepareSearch("index1").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject("metrics")
                    .startObject("value1")
                        .field("field", "value1")
                    .endObject()
                .endObject()
            .endObject()).execute().actionGet();
    }

	@Test(expectedExceptions=SearchPhaseExecutionException.class)
    public void assertUnknownOperator() throws Exception {
		String indexName = "index2";
		int numberOfShards = 1;
		
		createIndex(numberOfShards, indexName);
        
        client.prepareSearch("index1").setAggregations(JsonXContent.contentBuilder()
            .startObject()
                .startObject("metrics")
                    .startObject("value1")
                        .field("operator", "div")
                    .endObject()
                .endObject()
            .endObject()).execute().actionGet();
    }
}
