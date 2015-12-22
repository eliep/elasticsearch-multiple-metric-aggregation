package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class MultipleMetricParser implements Aggregator.Parser {
    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");

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
        Map<String, ValuesSourceConfig<ValuesSource>> configMap  = new HashMap<String, ValuesSourceConfig<ValuesSource>>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
            	logger.info("ready to parse: ", currentFieldName);
                MultipleMetricParam metric = MultipleMetricParam.parse(aggregationName, parser, context, currentFieldName);
                metricsMap.put(currentFieldName, metric);
            	logger.info("saved metric: ", currentFieldName);
            	if (!metric.isScript())
            		configMap.put(currentFieldName, metric.vsParser().config());
                logger.info("saved metric config: ", currentFieldName);
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "]");
            }
        }


        
        return new MultipleMetricAggregator.Factory(aggregationName, configMap, metricsMap);
    }
}
