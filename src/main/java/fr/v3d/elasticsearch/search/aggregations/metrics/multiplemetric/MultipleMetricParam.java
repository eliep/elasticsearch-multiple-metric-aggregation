package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.search.MatchAllDocsFilter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregator;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

public class MultipleMetricParam {

    public static final String SUM_TOKEN = "sum";
    public static final String COUNT_TOKEN = "count";
    public static final String FILTER_TOKEN = "filter";
    public static final String SCRIPT_TOKEN = "script";
    public static final String PARAMS_TOKEN = "params";
    
    private String operator;
    private Filter filter;
    private String script;
    private ScriptType scriptType;
    private String scriptLang;
    private Map<String, Object> scriptParams;
    private ValuesSourceParser vsParser = null;
    
    public MultipleMetricParam() {}  // for serialization
    
    public MultipleMetricParam(ValuesSourceParser vsParser, String operator, Filter filter, 
            String script, ScriptType scriptType, String scriptLang, Map<String, Object> scriptParams) {
        this.vsParser = vsParser;
        this.operator = operator;
        this.filter = filter;
        this.script = script;
        this.scriptType = scriptType;
        this.scriptLang = scriptLang;
        this.scriptParams = scriptParams;
    }
    
    public MultipleMetricParam(ValuesSourceParser vsParser, String operator, ParsedFilter parsedFilter, 
            String script, ScriptType scriptType, String scriptLang, Map<String, Object> scriptParams) {
        this(vsParser, operator, parsedFilter == null ? new MatchAllDocsFilter() : parsedFilter.filter(),
                script, scriptType, scriptLang, scriptParams);
    }
    
    public ValuesSourceParser vsParser() {
    	return vsParser;
    }
    
    public String operator() {
        return operator;
    }
    
    public Filter filter() {
        return filter;
    }
    
    public String script() {
        return script;
    }
    
    public ScriptType scriptType() {
        return scriptType;
    }
    
    public String scriptLang() {
        return scriptLang;
    }
    
    public Map<String, Object> scriptParams() {
        return scriptParams;
    }
    
    public boolean isScript() {
        return (this.script != null);
    }
    
    public static MultipleMetricParam parse(String aggregationName, XContentParser parser, SearchContext context, String metricName) throws IOException {

        XContentParser.Token token;
        String currentFieldName = null;
        
        String operator = null;
        ValuesSourceParser vsParser = null;
        ParsedFilter parsedFilter = null;
        
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        Map<String, Object> scriptParams = null;


        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (SCRIPT_TOKEN.equals(currentFieldName)) {
                    if (!scriptParameterParser.token(currentFieldName, token, parser)) {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                    }
                }
                
            } else if (token == XContentParser.Token.START_OBJECT) {
            	if (SUM_TOKEN.equals(currentFieldName)) {
                    operator = currentFieldName;
                    vsParser = parseSum(aggregationName, parser, context); //ValuesSourceParser<ValuesSource.Numeric> 
                } else if (COUNT_TOKEN.equals(currentFieldName)) {
                    operator = currentFieldName;
                    vsParser = parseCount(aggregationName, parser, context);
                } else if (FILTER_TOKEN.equals(currentFieldName)) {
                    parsedFilter = context.queryParserService().parseInnerFilter(parser);
                } else if (SCRIPT_TOKEN.equals(currentFieldName)) {
                    if (!scriptParameterParser.token(currentFieldName, token, parser)) {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                    }
                } else if (PARAMS_TOKEN.equals(currentFieldName)) {
                	scriptParams = parser.map();
                }
            }
        }

        ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
        String script = null;
        ScriptType scriptType = null;
        String scriptLang = null;
        if (scriptValue != null) {
            script = scriptValue.script();
            scriptType = scriptValue.scriptType();
            scriptLang = scriptParameterParser.lang();
        }

        if (script == null && vsParser == null)
            throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] must either define a field or a script.");
        
        if (script == null && vsParser != null && operator == null)
            throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] must define an aggregator.");
        
        if (operator != null && !MultipleMetricParser.isValidOperator(operator))
            throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] define a non valid aggregator: [" + operator + "].");
        
        return new MultipleMetricParam(vsParser, operator, parsedFilter, script, scriptType, scriptLang, scriptParams);
    }
    
    public static ValuesSourceParser<ValuesSource.Numeric> parseSum(String aggregationName, XContentParser parser, SearchContext context) 
    		throws IOException {

        ValuesSourceParser<ValuesSource.Numeric> vsParser = ValuesSourceParser.numeric(aggregationName, InternalSum.TYPE, context)
              .build();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
        	if (token == XContentParser.Token.FIELD_NAME) {
    			currentFieldName = parser.currentName();
    		} else if (!vsParser.token(currentFieldName, token, parser)) {
				throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
    		}
        }
        
        return vsParser;
    }
    
    public static ValuesSourceParser parseCount(String aggregationName, XContentParser parser, SearchContext context) 
    		throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, InternalValueCount.TYPE, context)
              .build();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
        	if (token == XContentParser.Token.FIELD_NAME) {
    			currentFieldName = parser.currentName();
    		} else if (!vsParser.token(currentFieldName, token, parser)) {
				throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
    		}
        }
        
        return vsParser;
    }

    public static MultipleMetricParam readFrom(StreamInput in) throws IOException {
        MultipleMetricParam metric = new MultipleMetricParam();
        boolean hasScript = in.readBoolean();
        if (hasScript)
            metric.script = in.readString();
        
        boolean hasScriptLang = in.readBoolean();
        if (hasScriptLang)
            metric.scriptLang = in.readString();
        
        boolean hasScriptType = in.readBoolean();
        if (hasScriptType)
            metric.scriptType = ScriptType.readFrom(in);
        
        boolean hasScriptParams = in.readBoolean();
        if (hasScriptParams) {
        	int size = in.readInt();
        	metric.scriptParams = new HashMap<String, Object>(size);
        	for (int i=0; i<size; i++) {
        		String key = in.readString();
        		Object value = in.readGenericValue();
        		metric.scriptParams.put(key, value);
        	}
        }
        
        return metric;
    }
    
    public static void writeTo(MultipleMetricParam metric, StreamOutput out) throws IOException {
        if (metric.script != null) {
            out.writeBoolean(true);
            out.writeString(metric.script);
        } else
            out.writeBoolean(false);
        
        if (metric.scriptLang != null) {
            out.writeBoolean(true);
            out.writeString(metric.scriptLang);
        } else
            out.writeBoolean(false);
        
        if (metric.scriptType != null) {
            out.writeBoolean(true);
            ScriptType.writeTo(metric.scriptType, out);
        } else
            out.writeBoolean(false);
        
        if (metric.scriptParams != null) {
            out.writeBoolean(true);
            out.writeInt(metric.scriptParams.size());
            for (Map.Entry<String, Object> entry: metric.scriptParams.entrySet()) {
            	out.writeString(entry.getKey());
            	out.writeGenericValue(entry.getValue());
            }
        } else
            out.writeBoolean(false);
    }
}
