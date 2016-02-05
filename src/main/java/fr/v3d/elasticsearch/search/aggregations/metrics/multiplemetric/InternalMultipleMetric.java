package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

public class InternalMultipleMetric extends InternalNumericMetricsAggregation.MultiValue implements MultipleMetric {

    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");
    public final static Type TYPE = new Type("multiple-metric");

    public Map<String, MultipleMetricParam> metricsMap;
    public Map<String, Double> paramsMap;
    public Map<String, Long> countsMap;

    InternalMultipleMetric() {} // for serialization

    InternalMultipleMetric(String name, Map<String, MultipleMetricParam> metricsMap, Map<String, Long> countsMap, 
    		List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.metricsMap = metricsMap;
        this.countsMap = countsMap;
        
        this.paramsMap = new HashMap<String, Double>();
    }
    
    InternalMultipleMetric(String name, Map<String, MultipleMetricParam> metricsMap, Map<String, Double> paramsMap, Map<String, Long> countsMap, 
    		List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.metricsMap = metricsMap;
        this.paramsMap = paramsMap;
        this.countsMap = countsMap;
    }
    
    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double getValue(String name) {
        return value(name);
    }

    @Override
    public long getDocCount(String name) {
        return (countsMap.get(name) != null) ? countsMap.get(name) : 0;
    }
    
    @Override
    public double value(String name) {
        if (paramsMap.size() == 0)
        	return 0.0;

        return (paramsMap.get(name) != null) ? paramsMap.get(name) : 0.0;
    }
	
    @Override
    public InternalMultipleMetric doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    	logger.info("reduce called");
        InternalMultipleMetric reduced = null;
        
        if (aggregations.size() == 1) {
            reduced = (InternalMultipleMetric) aggregations.get(0);
            
        } else {
        
            for (InternalAggregation aggregation : aggregations) {
                if (reduced == null) {
                    reduced = (InternalMultipleMetric) aggregation;
                } else {
                    InternalMultipleMetric current = (InternalMultipleMetric) aggregation;
                    for (Map.Entry<String, Double> entry: current.paramsMap.entrySet()) 
                        reduced.paramsMap.put(entry.getKey(), reduced.paramsMap.get(entry.getKey()) + entry.getValue());
                    
                    for (Map.Entry<String, Long> entry: current.countsMap.entrySet()) 
                        reduced.countsMap.put(entry.getKey(), reduced.countsMap.get(entry.getKey()) + entry.getValue());
                        
                }
            }
            
        }
        
        if (reduced == null)
            reduced = (InternalMultipleMetric) aggregations.get(0);
        
        
        Map<String, Double> scriptedMap = new HashMap<String, Double>(); 
    	for (Map.Entry<String, MultipleMetricParam> entry: metricsMap.entrySet()) {
    		if (entry.getValue().isScript()) {
    			MultipleMetricParam metric = entry.getValue();

    	        if (reduced.paramsMap.size() == 0) {
    	        	scriptedMap.put(entry.getKey(), 0.0);
    	        	
    			} else {
	    			Map<String, Object> scriptParamsMap = metric.scriptParams();
	    	        if (scriptParamsMap == null)
	    	        	scriptParamsMap = new HashMap<String, Object>();
	    	        scriptParamsMap.putAll(reduced.paramsMap);
	    	        
	                Script script = new Script(metric.script().getScript(), metric.script().getType(), metric.script().getLang(), scriptParamsMap);
	                Double result = (Double)reduceContext.scriptService().executable(script, ScriptContext.Standard.AGGS, reduceContext).run();
	                
	                scriptedMap.put(entry.getKey(), result);
    	        }
    		}
    	}
    	
    	reduced.paramsMap.putAll(scriptedMap);
        
        return reduced;
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        name = in.readString();
        if (in.readBoolean()) {
            int n = in.readInt();
            paramsMap = new HashMap<String, Double>();
            for (int i = 0; i < n; i++) {
                String key = in.readString();
                Double value = in.readDouble();
                paramsMap.put(key, value);
            }
        }
        if (in.readBoolean()) {
            int n = in.readInt();
            metricsMap = new HashMap<String, MultipleMetricParam>();
            for (int i = 0; i < n; i++) {
                String key = in.readString();
                MultipleMetricParam value = MultipleMetricParam.readFrom(in);
                metricsMap.put(key, value);
            }
        }
        if (in.readBoolean()) {
            int n = in.readInt();
            countsMap = new HashMap<String, Long>();
            for (int i = 0; i < n; i++) {
                String key = in.readString();
                Long value = in.readLong();
                countsMap.put(key, value);
            }
        }
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (paramsMap != null) {
            out.writeBoolean(true);
            out.writeInt(paramsMap.size());
            for (Map.Entry<String, Double> entry: paramsMap.entrySet()) {
                out.writeString(entry.getKey());
                out.writeDouble(entry.getValue());
            }
        } else
            out.writeBoolean(false);
        
        if (metricsMap != null) {
            out.writeBoolean(true);
            out.writeInt(metricsMap.size());
            for (Map.Entry<String, MultipleMetricParam> entry: metricsMap.entrySet()) {
                out.writeString(entry.getKey());
                MultipleMetricParam.writeTo(entry.getValue(), out);
            }
        } else
            out.writeBoolean(false);
        
        if (countsMap != null) {
            out.writeBoolean(true);
            out.writeInt(countsMap.size());
            for (Map.Entry<String, Long> entry: countsMap.entrySet()) {
                out.writeString(entry.getKey());
                out.writeLong(entry.getValue());
            }
        } else
            out.writeBoolean(false);
        
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    	for (Map.Entry<String, MultipleMetricParam> entry : metricsMap.entrySet()) {
    		String metricName = entry.getKey();
        	builder.startObject(metricName);
        	
        	double value = value(metricName);
    		builder.field("value", (value != Double.POSITIVE_INFINITY && value != Double.NEGATIVE_INFINITY) ? value : null);
    		
    		if (countsMap != null && !entry.getValue().isScript())
        		builder.field("doc_count", getDocCount(metricName));
    		
    		builder.endObject();
        }
        
        return builder;
    }


    
    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMultipleMetric readResult(StreamInput in) throws IOException {
            InternalMultipleMetric result = new InternalMultipleMetric();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

}
