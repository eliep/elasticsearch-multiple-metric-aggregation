package fr.v3d.elasticsearch.plugin.multiplemetric;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;

import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.InternalMultipleMetric;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.MultipleMetricParser;

public class MultipleMetricPlugin extends Plugin {

    public String name() {
        return "multiple-metric-aggregation";
    }

    public String description() {
        return "Multiple Metric Aggregation for Elasticsearch";
    }
    
    public void onModule(SearchModule module) {
    	module.registerAggregatorParser(MultipleMetricParser.class);
        InternalMultipleMetric.registerStreams();
    }

}
