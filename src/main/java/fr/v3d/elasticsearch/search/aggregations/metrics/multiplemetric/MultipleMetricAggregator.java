package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import fr.v3d.elasticsearch.search.aggregations.support.MultipleValuesSourceAggregatorFactory;

/**
 *
 */
public class MultipleMetricAggregator extends NumericMetricsAggregator.MultiValue {

    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");
    
    private final Map<String, ValuesSource> valuesSourceMap;
    private final Map<String, Weight> weightMap = new HashMap<String, Weight>();

    private Map<String, MultipleMetricParam> metricParamsMap;
    private Map<String, DoubleArray> metricValuesMap = new HashMap<String, DoubleArray>();
    private Map<String, LongArray> metricCountsMap = new HashMap<String, LongArray>();
    
    private ScriptService scriptService;



    public MultipleMetricAggregator(String name, Map<String, ValuesSource> valuesSourceMap, AggregationContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
            Map<String, MultipleMetricParam> metricsMap) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSourceMap = valuesSourceMap;
        this.metricParamsMap = metricsMap;
        
        this.scriptService = context.searchContext().scriptService();
        
        for (Map.Entry<String, ValuesSource> entry: valuesSourceMap.entrySet()) {
            ValuesSource valuesSource = entry.getValue();
            String key = entry.getKey();
            if (valuesSource != null) {
                metricValuesMap.put(key, context.bigArrays().newDoubleArray(1, true));
                metricCountsMap.put(key, context.bigArrays().newLongArray(1, true));
            }
        }
        

        for (String metricName: valuesSourceMap.keySet()) {
        	weightMap.put(metricName, context.searchContext().searcher().createNormalizedWeight(metricParamsMap.get(metricName).filter(), false));
        }
    }
    

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
    	
        if (valuesSourceMap == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        
        final BigArrays bigArrays = context.bigArrays();

        final Map<String, SortedNumericDoubleValues> doubleValuesMap = new HashMap<String, SortedNumericDoubleValues>();
        final Map<String, SortedBinaryDocValues> docValuesMap = new HashMap<String, SortedBinaryDocValues>();
        final Map<String, Bits> bitsMap = new HashMap<String, Bits>();
        
        for (Entry<String, ValuesSource> entry: valuesSourceMap.entrySet()) {
        	String key = entry.getKey();
            Bits bits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), weightMap.get(key).scorer(ctx));
            bitsMap.put(key, bits);
            
            if (metricParamsMap.get(key).operator().equals(MultipleMetricParser.COUNT_OPERATOR)) {
            	SortedBinaryDocValues values = ( entry.getValue() != null) ? entry.getValue().bytesValues(ctx) : null;
            	docValuesMap.put(key, values);
            } else {
            	SortedNumericDoubleValues values = ( entry.getValue() != null) 
            			? ((ValuesSource.Numeric)entry.getValue()).doubleValues(ctx)
    					: null;
            	doubleValuesMap.put(key, values);
            }
        }
        
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {

                for (Entry<String, SortedNumericDoubleValues> entry: doubleValuesMap.entrySet() ) {
                    String key = entry.getKey();
                    SortedNumericDoubleValues values = entry.getValue();
                    if (values != null && bitsMap.get(key).get(doc)) {
                        values.setDocument(doc);
                        metricValuesMap.put(key, bigArrays.grow(metricValuesMap.get(key), bucket + 1));
                        double increment = 0;
                        for (int i = 0; i < values.count(); i++) 
                            increment += values.valueAt(i);
                        metricValuesMap.get(key).increment(bucket, increment);
                        
                        metricCountsMap.put(key, bigArrays.grow(metricCountsMap.get(key), bucket + 1));
                        metricCountsMap.get(key).increment(bucket, 1);
                    } 
                }
                
                for (Entry<String, SortedBinaryDocValues> entry: docValuesMap.entrySet() ) {
                    String key = entry.getKey();
                    SortedBinaryDocValues values = entry.getValue();
                    if (values != null && bitsMap.get(key).get(doc)) {
                        values.setDocument(doc);
                        metricValuesMap.put(key, bigArrays.grow(metricValuesMap.get(key), bucket + 1));
                        metricValuesMap.get(key).increment(bucket, values.count());

                        metricCountsMap.put(key, bigArrays.grow(metricCountsMap.get(key), bucket + 1));
                        metricCountsMap.get(key).increment(bucket, 1);
                    } 
                }
            }
        };
    }
    
    private Double getMetricValue(String name, long owningBucketOrdinal) {
        return metricValuesMap.containsKey(name) && metricValuesMap.get(name).size() > owningBucketOrdinal
                    ? metricValuesMap.get(name).get(owningBucketOrdinal)
                    : 0.0;
    }

    private HashMap<String, Double> getScriptParamsMap(long owningBucketOrdinal) {
        HashMap<String, Double> scriptParamsMap = new HashMap<String, Double>();
        for (Map.Entry<String, MultipleMetricParam> entry: metricParamsMap.entrySet())
            if (!entry.getValue().isScript())
            	scriptParamsMap.put(entry.getKey(), getMetricValue(entry.getKey(), owningBucketOrdinal));
        
        
        return scriptParamsMap;
    }

    private Map<String, Long> getCountsMap(long owningBucketOrdinal) {
        HashMap<String, Long> countsMap = new HashMap<String, Long>();
        for (Map.Entry<String, LongArray> entry: metricCountsMap.entrySet())
        	countsMap.put(entry.getKey(), entry.getValue().get(owningBucketOrdinal));
        
        return countsMap;
    }

    private Map<String, Long> getEmptyCountsMap() {
        HashMap<String, Long> countsMap = new HashMap<String, Long>();
        for (Map.Entry<String, LongArray> entry: metricCountsMap.entrySet())
        	countsMap.put(entry.getKey(), 0L);
        
        return countsMap;
    }
    
    @Override
    public boolean hasMetric(String name) {
        return metricParamsMap.containsKey(name);
    }
    
    @Override
    public double metric(String name, long owningBucketOrdinal) {
        Double result;
        MultipleMetricParam metric = metricParamsMap.get(name);
        if (metric.isScript()) {
            Map<String, Object> scriptParamsMap = metric.scriptParams();
            if (scriptParamsMap == null)
            	scriptParamsMap = new HashMap<String, Object>();
            scriptParamsMap.putAll(getScriptParamsMap(owningBucketOrdinal));
            
            Script script = new Script(metric.script().getScript(), metric.script().getType(), metric.script().getLang(), scriptParamsMap);
            result = (Double)scriptService.executable(script, ScriptContext.Standard.AGGS, context.searchContext(), new HashMap<String, String>()).run();
        } else {
            result = getMetricValue(name, owningBucketOrdinal);
        }
        
        return result;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        HashMap<String, Double> scriptParamsMap = getScriptParamsMap(owningBucketOrdinal);
        Map<String, Long> countsMap = getCountsMap(owningBucketOrdinal);
        
        return new InternalMultipleMetric(name, metricParamsMap, scriptParamsMap, countsMap, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
    	logger.info("empty aggregation");
        return new InternalMultipleMetric(name, metricParamsMap, getEmptyCountsMap(), pipelineAggregators(), metaData());
    }
    
    public static class Factory extends MultipleValuesSourceAggregatorFactory.LeafOnly<ValuesSource> {
        public Map<String, MultipleMetricParam> metricsMap;
        
        public Factory(String name, Map<String, ValuesSourceConfig<ValuesSource>> valueSourceConfigMap, 
                Map<String, MultipleMetricParam> metricsMap) {
            super(name, InternalMultipleMetric.TYPE.name(), valueSourceConfigMap);
            this.metricsMap = metricsMap;
        }

		@Override
		protected Aggregator doCreateInternal(Map<String, ValuesSource> valuesSourceMap,
				AggregationContext aggregationContext, Aggregator parent, boolean collectsFromSingleBucket,
				List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
			return new MultipleMetricAggregator(name, valuesSourceMap, aggregationContext, parent, pipelineAggregators, metaData, this.metricsMap);
		}
    }

    @Override
    public void doClose() {
        for (Map.Entry<String, DoubleArray> entry: metricValuesMap.entrySet())
            Releasables.close(entry.getValue());
        for (Map.Entry<String, LongArray> entry: metricCountsMap.entrySet())
            Releasables.close(entry.getValue());
        
        metricValuesMap = null;
        metricParamsMap = null;
    }

}
