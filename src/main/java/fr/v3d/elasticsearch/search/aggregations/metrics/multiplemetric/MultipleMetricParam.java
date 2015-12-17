package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.MatchAllDocsFilter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

public class MultipleMetricParam {

    public static final String FIELD_TOKEN = "field";
    public static final String OPERATOR_TOKEN = "operator";
    public static final String FILTER_TOKEN = "filter";
    public static final String SCRIPT_TOKEN = "script";
    
	private String field;
	private String operator;
	private Filter filter;
	private String script;
	private ScriptType scriptType;
	private String scriptLang;
	
	public MultipleMetricParam() {}  // for serialization
	
	public MultipleMetricParam(String field, String operator, Filter filter, 
			String script, ScriptType scriptType, String scriptLang) {
		this.field = field;
		this.operator = operator;
		this.filter = filter;
		this.script = script;
		this.scriptType = scriptType;
		this.scriptLang = scriptLang;
	}
	
	public MultipleMetricParam(String field, String operator, ParsedFilter parsedFilter, 
			String script, ScriptType scriptType, String scriptLang) {
		this(field, operator, parsedFilter == null ? new MatchAllDocsFilter() : parsedFilter.filter(),
				script, scriptType, scriptLang);
	}
	
	public String field() {
		return field;
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
	
	public boolean isScript() {
		return (this.script != null);
	}
	
	public static MultipleMetricParam parse(String aggregationName, XContentParser parser, SearchContext context, String metricName) throws IOException {

        XContentParser.Token token;
        String currentFieldName = null;
        String field = null;
        String operator = null;
        ParsedFilter parsedFilter = null;
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                
            } else if (token == XContentParser.Token.VALUE_STRING) {
            	if (FIELD_TOKEN.equals(currentFieldName)) {
            		field = parser.text();
            	} else if (OPERATOR_TOKEN.equals(currentFieldName)) {
            		operator = parser.text();
            	} else if (SCRIPT_TOKEN.equals(currentFieldName)) {
            		if (!scriptParameterParser.token(currentFieldName, token, parser)) {
            			throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            		}
            	}
            	
            } else if (token == XContentParser.Token.START_OBJECT) {
            	if (FILTER_TOKEN.equals(currentFieldName)) {
            		parsedFilter = context.queryParserService().parseInnerFilter(parser);
            	} else if (SCRIPT_TOKEN.equals(currentFieldName)) {
            		if (!scriptParameterParser.token(currentFieldName, token, parser)) {
            			throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            		}
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

        if (script == null && field == null)
        	throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] must either define a field or a script.");
        
        if (script == null && field != null && operator == null)
        	throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] must either define an operator.");
        
        if (script == null && field != null && operator == null)
        	throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] must either define an operator.");
        
        if (operator != null && !MultipleMetricParser.isValidOperator(operator))
        	throw new SearchParseException(context, "Metric [" + metricName + "] in [" + aggregationName + "] define a non valid operator: [" + operator + "].");
        
        return new MultipleMetricParam(field, operator, parsedFilter, script, scriptType, scriptLang);
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
	}
}
