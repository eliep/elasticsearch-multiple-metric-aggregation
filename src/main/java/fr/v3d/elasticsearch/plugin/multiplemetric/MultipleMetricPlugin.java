package fr.v3d.elasticsearch.plugin.multiplemetric;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.aggregations.AggregationModule;

import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.InternalMultipleMetric;
import fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric.MultipleMetricParser;

public class MultipleMetricPlugin extends AbstractPlugin {

    public String name() {
        return "multiple-metric-aggregation";
    }

    public String description() {
        return "Multiple Metric Aggregation for Elasticsearch";
    }
    
    public void onModule(AggregationModule module) {
        module.addAggregatorParser(MultipleMetricParser.class);
        InternalMultipleMetric.registerStreams();
    }

}
