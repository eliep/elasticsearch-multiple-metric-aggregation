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
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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
    
    private final Map<String, ValuesSource> valuesSourceMap;
    private Map<String, SortedNumericDoubleValues> doubleValuesMap = new HashMap<String, SortedNumericDoubleValues>();
    private Map<String, SortedBinaryDocValues> docValuesMap = new HashMap<String, SortedBinaryDocValues>();

    private Map<String, MultipleMetricParam> metricParamsMap;
    private Map<String, DoubleArray> metricValuesMap = new HashMap<String, DoubleArray>();
    private Map<String, LongArray> metricCountsMap = new HashMap<String, LongArray>();
    
    private ScriptService scriptService;

    private Map<String, Bits> bitsMap = new HashMap<String, Bits>();
    
    public MultipleMetricAggregator(String name, long estimatedBucketsCount, Map<String, ValuesSource> valuesSourceMap, 
            AggregationContext aggregationContext, Aggregator parent,
            Map<String, MultipleMetricParam> metricsMap) {
        
        super(name, estimatedBucketsCount, aggregationContext, parent);
        this.valuesSourceMap = valuesSourceMap;
        this.metricParamsMap = metricsMap;
        
        this.scriptService = context.searchContext().scriptService();
        
        for (Map.Entry<String, ValuesSource> entry: valuesSourceMap.entrySet()) {
            ValuesSource valuesSource = entry.getValue();
            String key = entry.getKey();
            if (valuesSource != null) {
                final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
                metricValuesMap.put(key, bigArrays.newDoubleArray(initialSize, true));
                metricCountsMap.put(key, bigArrays.newLongArray(initialSize, true));
            }
        }
    }
    
    @Override
    public boolean shouldCollect() {
        return this.valuesSourceMap != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        for (Entry<String, ValuesSource> entry: valuesSourceMap.entrySet()) {
            String key = entry.getKey();
            try {
                Bits bits = DocIdSets.toSafeBits(
                        reader.reader(), 
                        metricParamsMap.get(key).filter().getDocIdSet(reader, reader.reader().getLiveDocs()));
                
                bitsMap.put(key, bits);
                if (metricParamsMap.get(key).operator().equals(MultipleMetricParser.COUNT_OPERATOR)) {
                    SortedBinaryDocValues values = ( entry.getValue() != null) ? entry.getValue().bytesValues() : null;
                    docValuesMap.put(key, values);
                } else {
                    SortedNumericDoubleValues values = ( entry.getValue() != null) 
                            ? ((ValuesSource.Numeric)entry.getValue()).doubleValues()
                            : null;
                    doubleValuesMap.put(key, values);
                }
            } catch (IOException ioe) {
                throw new AggregationExecutionException("Failed to aggregate filter aggregator [" + name + "]", ioe);
            }
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert this.valuesSourceMap != null : "should collect first";

        for (Entry<String, SortedNumericDoubleValues> entry: doubleValuesMap.entrySet() ) {
            String key = entry.getKey();
            SortedNumericDoubleValues values = entry.getValue();
            if (values != null && bitsMap.get(key).get(doc)) {
                values.setDocument(doc);
                metricValuesMap.put(key, bigArrays.grow(metricValuesMap.get(key), owningBucketOrdinal + 1));
                double increment = 0;
                for (int i = 0; i < values.count(); i++) 
                    increment += values.valueAt(i);
                metricValuesMap.get(key).increment(owningBucketOrdinal, increment);
                
                metricCountsMap.put(key, bigArrays.grow(metricCountsMap.get(key), owningBucketOrdinal + 1));
                metricCountsMap.get(key).increment(owningBucketOrdinal, 1);
            } 
        }
        
        for (Entry<String, SortedBinaryDocValues> entry: docValuesMap.entrySet() ) {
            String key = entry.getKey();
            SortedBinaryDocValues values = entry.getValue();
            if (values != null && bitsMap.get(key).get(doc)) {
                values.setDocument(doc);
                metricValuesMap.put(key, bigArrays.grow(metricValuesMap.get(key), owningBucketOrdinal + 1));
                metricValuesMap.get(key).increment(owningBucketOrdinal, values.count());

                metricCountsMap.put(key, bigArrays.grow(metricCountsMap.get(key), owningBucketOrdinal + 1));
                metricCountsMap.get(key).increment(owningBucketOrdinal, 1);
            } 
        }
    }
    
    private Double getMetricValue(String name, long owningBucketOrdinal) {
        return metricValuesMap.containsKey(name)
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
            result = (Double)scriptService.executable(metric.scriptLang(), metric.script(), metric.scriptType(), scriptParamsMap).run();
        } else {
            result = getMetricValue(name, owningBucketOrdinal);
        }
        
        return result;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        HashMap<String, Double> scriptParamsMap = getScriptParamsMap(owningBucketOrdinal);
        Map<String, Long> countsMap = getCountsMap(owningBucketOrdinal);
        
        return new InternalMultipleMetric(name, metricParamsMap, scriptParamsMap, countsMap);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMultipleMetric(name, metricParamsMap, getEmptyCountsMap());
    }
    
    public static class Factory extends MultipleValuesSourceAggregatorFactory.LeafOnly<ValuesSource> {
        public Map<String, MultipleMetricParam> metricsMap;
        
        public Factory(String name, Map<String, ValuesSourceConfig<ValuesSource>> valueSourceConfigMap, 
                Map<String, MultipleMetricParam> metricsMap) {
            super(name, InternalMultipleMetric.TYPE.name(), valueSourceConfigMap);
            this.metricsMap = metricsMap;
        }

        @Override
        public Aggregator create(Map<String, ValuesSource> valuesSourceMap, 
                long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            
            return new MultipleMetricAggregator(name, expectedBucketsCount, valuesSourceMap, aggregationContext, parent, this.metricsMap);
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
        bitsMap = null;
        doubleValuesMap = null;
        docValuesMap = null;
    }
}
