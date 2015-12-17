package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

public class MultipleMetricBuilder extends AggregationBuilder<MultipleMetricBuilder> {
    

    private Map<String, Metric> metrics = new HashMap<String, Metric>();

    private static class Metric {
    	public String field;
    	public String operator;
    	public FilterBuilder filter;
    	public String script;
    	
    	public static Metric field(String field, String operator, FilterBuilder filter) {
    		Metric metric = new Metric();
    		metric.field = field;
    		metric.operator = operator;
    		metric.filter = filter;
    		return metric;
    	}
    	
    	public static Metric script(String script) {
    		Metric metric = new Metric();
    		metric.script = script;
    		return metric;
    	}
    }
    
    public MultipleMetricBuilder(String name) {
        super(name, InternalMultipleMetric.TYPE.name());
    }
    
    public MultipleMetricBuilder field(String var, String field, String operator, FilterBuilder filter) {
        this.metrics.put(var, Metric.field(field, operator, filter));
        return this;
    }
    
    public MultipleMetricBuilder field(String var, String field, String operator) {
        return this.field(var, field, operator, null);
    }
    
    public MultipleMetricBuilder script(String var, String script) {
    	this.metrics.put(var, Metric.script(script));
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Metric> entry: this.metrics.entrySet()) {
            builder.startObject(entry.getKey());
            Metric metric = entry.getValue();
        	if (metric.field != null)
        		builder.field("field", metric.field);
        	if (metric.operator != null)
        		builder.field("operator", metric.operator);
        	if (metric.filter != null)
    			builder.field("filter", metric.filter);
        	if (metric.script != null)
    			builder.field("script", metric.script);
        	
            builder.endObject();
        }
        return builder.endObject();
    }

}
