package fr.v3d.elasticsearch.search.aggregations.support;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

public abstract class MultipleValuesSourceAggregatorFactory<VS extends ValuesSource> extends AggregatorFactory {

    public static abstract class LeafOnly<VS extends ValuesSource> extends MultipleValuesSourceAggregatorFactory<VS> {

        protected LeafOnly(String name, String type, Map<String, ValuesSourceConfig<VS>> valuesSourceConfigMap) {
            super(name, type, valuesSourceConfigMap);
        }

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }

    protected Map<String, ValuesSourceConfig<VS>> configMap;

    protected MultipleValuesSourceAggregatorFactory(String name, String type, Map<String, ValuesSourceConfig<VS>> configMap) {
        super(name, type);
        this.configMap = configMap;
    }

    @Override
    public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        Map<String, VS> vsMap = new HashMap<String, VS>();
        for (Entry<String, ValuesSourceConfig<VS>> entry: this.configMap.entrySet()) {
            ValuesSourceConfig<VS> config = entry.getValue();
            VS vs = !config.unmapped() ? context.valuesSource(config, context.searchContext()) : null;
            vsMap.put(entry.getKey(), vs);
        }
        return doCreateInternal(vsMap, context, parent, collectsFromSingleBucket, pipelineAggregators, metaData);
    }

    protected abstract Aggregator doCreateInternal(Map<String, VS> valuesSourceMap, AggregationContext aggregationContext, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException;

    @Override
    public void doValidate() {
    	for (ValuesSourceConfig<VS> config: configMap.values()) {
    		if (config == null || !config.valid()) {
    			throw new AggregationExecutionException("could not find the appropriate value context to perform aggregation [" + name + "]");
    		}
    	}
    }

}