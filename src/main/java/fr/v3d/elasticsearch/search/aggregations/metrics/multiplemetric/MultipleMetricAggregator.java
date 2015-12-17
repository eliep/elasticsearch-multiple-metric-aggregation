package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import fr.v3d.elasticsearch.search.aggregations.support.MultipleValuesSourceAggregatorFactory;

/**
 *
 */
public class MultipleMetricAggregator extends NumericMetricsAggregator.MultiValue {
    
    private final Map<String, ValuesSource.Numeric> valuesSourceMap;
    private Map<String, SortedNumericDoubleValues> valuesMap = new HashMap<String, SortedNumericDoubleValues>();

    private Map<String, MultipleMetricParam> metricsMap;
    private Map<String, DoubleArray> metricValuesMap = new HashMap<String, DoubleArray>();
    
    private ScriptService scriptService;

    private Map<String, Bits> bitsMap = new HashMap<String, Bits>();
    
    public MultipleMetricAggregator(String name, long estimatedBucketsCount, Map<String, ValuesSource.Numeric> valuesSourceMap, 
            AggregationContext aggregationContext, Aggregator parent,
            Map<String, MultipleMetricParam> metricsMap) {
        
        super(name, estimatedBucketsCount, aggregationContext, parent);
        this.valuesSourceMap = valuesSourceMap;
        this.metricsMap = metricsMap;
        
        this.scriptService = context.searchContext().scriptService();
        
        for (Map.Entry<String, ValuesSource.Numeric> entry: valuesSourceMap.entrySet()) {
            ValuesSource.Numeric valuesSource = entry.getValue();
            String key = entry.getKey();
            if (valuesSource != null) {
                final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
                metricValuesMap.put(key, bigArrays.newDoubleArray(initialSize, true));
            }
        }
    }
    
    @Override
    public boolean shouldCollect() {
        return this.valuesSourceMap != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        for (Entry<String, ValuesSource.Numeric> entry: valuesSourceMap.entrySet()) {
            try {
                Bits bits = DocIdSets.toSafeBits(
                        reader.reader(), 
                        metricsMap.get(entry.getKey()).filter().getDocIdSet(reader, reader.reader().getLiveDocs()));
                
                bitsMap.put(entry.getKey(), bits);
                valuesMap.put(entry.getKey(), entry.getValue() != null ? entry.getValue().doubleValues() : null);
            } catch (IOException ioe) {
                throw new AggregationExecutionException("Failed to aggregate filter aggregator [" + name + "]", ioe);
            }
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert this.valuesSourceMap != null : "should collect first";

        for (Entry<String, SortedNumericDoubleValues> entry: valuesMap.entrySet() ) {
            String key = entry.getKey();
            SortedNumericDoubleValues values = entry.getValue();
            if (values != null && bitsMap.get(key).get(doc)) {
                values.setDocument(doc);
                final int valueCount = values.count();

                metricValuesMap.put(key, bigArrays.grow(metricValuesMap.get(key), owningBucketOrdinal + 1));
                double increment = 0;
                if (metricsMap.get(key).operator().equals(MultipleMetricParser.COUNT_OPERATOR)) {
                    increment = valueCount;
                    
                } else if (metricsMap.get(key).operator().equals(MultipleMetricParser.SUM_OPERATOR)) {
                    for (int i = 0; i < valueCount; i++) 
                        increment += values.valueAt(i);
                    
                }
                metricValuesMap.get(key).increment(owningBucketOrdinal, increment);
            } 
        }
    }
    
    private Double getMetricValue(String name, long owningBucketOrdinal) {
        return metricValuesMap.containsKey(name)
                    ? metricValuesMap.get(name).get(owningBucketOrdinal)
                    : 0.0;
    }

    private HashMap<String, Double> getScriptParamsMap(long owningBucketOrdinal) {
        HashMap<String, Double> paramsMap = new HashMap<String, Double>();
        for (Map.Entry<String, MultipleMetricParam> entry: metricsMap.entrySet())
            if (!entry.getValue().isScript())
                paramsMap.put(entry.getKey(), getMetricValue(entry.getKey(), owningBucketOrdinal));
        
        return paramsMap;
    }
    
    @Override
    public boolean hasMetric(String name) {
        return metricsMap.containsKey(name);
    }
    
    @Override
    public double metric(String name, long owningBucketOrdinal) {
        Double result;
        MultipleMetricParam metric = metricsMap.get(name);
        if (metric.isScript()) {
            HashMap<String, Double> paramsMap = getScriptParamsMap(owningBucketOrdinal);
            result = (Double)scriptService.executable(metric.scriptLang(), metric.script(), metric.scriptType(), paramsMap).run();
        } else {
            result = getMetricValue(name, owningBucketOrdinal);
        }
        
        return result;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        HashMap<String, Double> paramsMap = getScriptParamsMap(owningBucketOrdinal);
        
        return new InternalMultipleMetric(name, metricsMap, paramsMap);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMultipleMetric(name);
    }
    
    public static class Factory extends MultipleValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {
        public Map<String, MultipleMetricParam> metricsMap;
        
        public Factory(String name, Map<String, ValuesSourceConfig<ValuesSource.Numeric>> valueSourceConfigMap, 
                Map<String, MultipleMetricParam> metricsMap) {
            super(name, InternalMultipleMetric.TYPE.name(), valueSourceConfigMap);
            this.metricsMap = metricsMap;
        }

        @Override
        public Aggregator create(Map<String, ValuesSource.Numeric> valuesSourceMap, 
                long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            
            return new MultipleMetricAggregator(name, expectedBucketsCount, valuesSourceMap, aggregationContext, parent, this.metricsMap);
        }
    }

    @Override
    public void doClose() {
        for (Map.Entry<String, DoubleArray> entry: metricValuesMap.entrySet())
            Releasables.close(entry.getValue());
        
        metricValuesMap = null;
        metricsMap = null;
        bitsMap = null;
    }
}
