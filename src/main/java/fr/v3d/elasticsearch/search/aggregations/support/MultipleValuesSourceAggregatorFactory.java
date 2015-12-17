package fr.v3d.elasticsearch.search.aggregations.support;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
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
    public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
        Map<String, VS> vsMap = new HashMap<String, VS>();
        for (Entry<String, ValuesSourceConfig<VS>> entry: this.configMap.entrySet()) {
        	ValuesSourceConfig<VS> config = entry.getValue();
        	VS vs = !config.unmapped() ? context.valuesSource(config, parent == null ? 0 : 1 + parent.depth()) : null;
    		vsMap.put(entry.getKey(), vs);
        }
        return create(vsMap, expectedBucketsCount, context, parent);
    }

    @Override
    public void doValidate() {
        /*if (config == null || !config.valid()) {
            resolveValuesSourceConfigFromAncestors(name, parent, config.valueSourceType());
        }*/
    }


    protected abstract Aggregator create(Map<String, VS> valuesSourceMap, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent);

//    private void resolveValuesSourceConfigFromAncestors(String aggName, AggregatorFactory parent, Class<VS> requiredValuesSourceType) {
//        ValuesSourceConfig config;
//        while (parent != null) {
//            if (parent instanceof ValuesSourceAggregatorFactory) {
//                config = ((ValuesSourceAggregatorFactory) parent).config;
//                if (config != null && config.valid()) {
//                    if (requiredValuesSourceType == null || requiredValuesSourceType.isAssignableFrom(config.valueSourceType)) {
//                        ValueFormat format = config.format;
//                        this.config = config;
//                        // if the user explicitly defined a format pattern, we'll do our best to keep it even when we inherit the
//                        // value source form one of the ancestor aggregations
//                        if (this.config.formatPattern != null && format != null && format instanceof ValueFormat.Patternable) {
//                            this.config.format = ((ValueFormat.Patternable) format).create(this.config.formatPattern);
//                        }
//                        return;
//                    }
//                }
//            }
//            parent = parent.parent();
//        }
//        throw new AggregationExecutionException("could not find the appropriate value context to perform aggregation [" + aggName + "]");
//    }
}