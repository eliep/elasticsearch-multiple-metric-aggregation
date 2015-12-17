package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class MultipleMetricParser implements Aggregator.Parser {

    public static final String PARAMS_TOKEN = "params";
    
    public static final String SUM_OPERATOR = "sum";
    public static final String COUNT_OPERATOR = "count";
    
    @Override
    public String type() {
        return InternalMultipleMetric.TYPE.name();
    }

    public static boolean isValidOperator(String operator) {
    	return (operator.equals(SUM_OPERATOR) || operator.equals(COUNT_OPERATOR));
    }
    
    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
       
        XContentParser.Token token;
        String currentFieldName = null;
        Map<String, MultipleMetricParam> metricsMap = new HashMap<String, MultipleMetricParam>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
            	MultipleMetricParam metric = MultipleMetricParam.parse(aggregationName, parser, context, currentFieldName);
            	metricsMap.put(currentFieldName, metric);
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "]");
            }
        }


        Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configMap = new HashMap<String, ValuesSourceConfig<ValuesSource.Numeric>>();
        for (Entry<String, MultipleMetricParam> entry: metricsMap.entrySet()) {
        	if (!entry.getValue().isScript()) {
	        	String field = entry.getValue().field();
	        	ValuesSourceConfig<ValuesSource.Numeric> config = new ValuesSourceConfig<ValuesSource.Numeric>(ValuesSource.Numeric.class);
	        	FieldMapper<?> mapper = context.smartNameFieldMapper(field);
	        	if (mapper == null) {
	        		config.unmapped(true);
	        	} else {
	        		config.fieldContext(new FieldContext(field, context.fieldData().getForField(mapper), mapper));
	        	}
	        	configMap.put(entry.getKey(), config);
        	}
        }
        
    	return new MultipleMetricAggregator.Factory(aggregationName, configMap, metricsMap);
    }
}
