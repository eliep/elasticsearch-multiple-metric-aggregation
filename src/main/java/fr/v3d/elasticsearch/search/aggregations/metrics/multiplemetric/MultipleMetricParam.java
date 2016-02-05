package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
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
    public static final ParseField SUM_FIELD = new ParseField("sum");
    public static final ParseField COUNT_FIELD = new ParseField("count");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField PARAMS_FIELD = new ParseField("params");
    
    private String operator;
    private Query filter;
    private Script script;
    private Map<String, Object> scriptParams;
    private ValuesSourceParser vsParser = null;
    
    public MultipleMetricParam() {}  // for serialization
    
    public MultipleMetricParam(ValuesSourceParser vsParser, String operator, Query filter, 
    		Script script, Map<String, Object> scriptParams) {
        this.vsParser = vsParser;
        this.operator = operator;
        this.filter = filter;
        this.script = script;
        this.scriptParams = scriptParams;
    }
    
    public MultipleMetricParam(ValuesSourceParser vsParser, String operator, ParsedQuery parsedFilter, 
            Script script, Map<String, Object> scriptParams) {
        this(vsParser, operator, parsedFilter == null ? new MatchAllDocsQuery() : parsedFilter.query(),
                script, scriptParams);
    }
    
    public ValuesSourceParser vsParser() {
    	return vsParser;
    }
    
    public String operator() {
        return operator;
    }
    
    public Query filter() {
        return filter;
    }
    
    public Script script() {
        return script;
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
        ParsedQuery parsedFilter = null;
        
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        Map<String, Object> scriptParams = null;
        Script script = null;


        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.parseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                    if (!scriptParameterParser.token(currentFieldName, token, parser, context.parseFieldMatcher())) {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].", parser.getTokenLocation());
                    
                    } else {
                        ScriptParameterValue scriptValue = scriptParameterParser.getScriptParameterValue(SCRIPT_TOKEN);
                        if (scriptValue != null) {
                            script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), null);
                        }
                    }
                }
                
            } else if (token == XContentParser.Token.START_OBJECT) {
            	if (context.parseFieldMatcher().match(currentFieldName, SUM_FIELD)) {
                    operator = currentFieldName;
                    vsParser = parseSum(aggregationName, parser, context); //ValuesSourceParser<ValuesSource.Numeric> 
                    
                } else if (context.parseFieldMatcher().match(currentFieldName, COUNT_FIELD)) {
                    operator = currentFieldName;
                    vsParser = parseCount(aggregationName, parser, context);
                    
                } else if (context.parseFieldMatcher().match(currentFieldName, FILTER_FIELD)) {
                    parsedFilter = context.queryParserService().parseInnerFilter(parser);
                    
                } else if (context.parseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                    script = Script.parse(parser, context.parseFieldMatcher());
                    
                } else if (context.parseFieldMatcher().match(currentFieldName, PARAMS_FIELD)) {
                	scriptParams = parser.map();
                }
            }
        }
        
        if (script == null && vsParser == null)
            throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] must either define a field or a script.", parser.getTokenLocation());
        
        if (script == null && vsParser != null && operator == null)
            throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] must define an aggregator.", parser.getTokenLocation());
        
        if (operator != null && !MultipleMetricParser.isValidOperator(operator))
            throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] define a non valid aggregator: [" + operator + "].", parser.getTokenLocation());
        
        return new MultipleMetricParam(vsParser, operator, parsedFilter, script, scriptParams);
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
				throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].", parser.getTokenLocation());
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
				throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].", parser.getTokenLocation());
    		}
        }
        
        return vsParser;
    }

    public static MultipleMetricParam readFrom(StreamInput in) throws IOException {
        MultipleMetricParam metric = new MultipleMetricParam();
        boolean hasScript = in.readBoolean();
        if (hasScript)
            metric.script = Script.readScript(in);
        
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
            metric.script.writeTo(out);
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
