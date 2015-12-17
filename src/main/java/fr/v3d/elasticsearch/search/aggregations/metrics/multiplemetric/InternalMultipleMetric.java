package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

public class InternalMultipleMetric extends InternalNumericMetricsAggregation.MultiValue implements MultipleMetric {
    
    public final static Type TYPE = new Type("multiple-metric");

    public Map<String, MultipleMetricParam> metricsMap;
    public Map<String, Double> paramsMap;
    private ScriptService scriptService;

    InternalMultipleMetric() {} // for serialization

    InternalMultipleMetric(String name) {
        super(name);
        metricsMap = new HashMap<String, MultipleMetricParam>();
        paramsMap = new HashMap<String, Double>();
    }
    
    InternalMultipleMetric(String name, Map<String, MultipleMetricParam> metricsMap, Map<String, Double> paramsMap) {
        super(name);
        this.metricsMap = metricsMap;
        this.paramsMap = paramsMap;
    }
    
    public void setScriptService(ScriptService scriptService) {
        this.scriptService = scriptService;
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
    public double value(String name) {
        MultipleMetricParam metric = metricsMap.get(name);
        Double result = 0.0;
        result = (metric.isScript())
            ? (Double)scriptService.executable(metric.scriptLang(), metric.script(), metric.scriptType(), paramsMap).run()
            : paramsMap.get(name);

        return result;
    }

    @Override
    public InternalMultipleMetric reduce(ReduceContext reduceContext) {
        InternalMultipleMetric reduced = null;
        
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            reduced = (InternalMultipleMetric) aggregations.get(0);
        } else {
        
            for (InternalAggregation aggregation : aggregations) {
                if (reduced == null) {
                    reduced = (InternalMultipleMetric) aggregation;
                } else {
                    InternalMultipleMetric current = (InternalMultipleMetric) aggregation;
                    for (Map.Entry<String, Double> entry: current.paramsMap.entrySet()) {
                        String key = entry.getKey();
                        reduced.paramsMap.put(key, reduced.paramsMap.get(key) + entry.getValue());
                    }
                }
            }
            
        }
        
        if (reduced == null)
            reduced = (InternalMultipleMetric) aggregations.get(0);

        reduced.setScriptService(reduceContext.scriptService());
        
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        //builder.startObject(name);
        for (String metricName : metricsMap.keySet())
            builder.field(metricName, value(metricName));
        //builder.endObject();
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
