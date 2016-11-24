package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;

public class ScriptBuilder implements ToXContent {

	private String name;
	
    private Script script;
    private Map<String, Object> params;

    public ScriptBuilder(String name) {
    	this.name = name;
    }

    public ScriptBuilder script(Script script) {
        this.script = script;
        return this;
    }

    public ScriptBuilder params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return this;
    }

    public ScriptBuilder param(String name, Object value) {
        if (this.params == null) {
            this.params = new HashMap<String, Object>();
        }
        this.params.put(name, value);
        return this;
    }
    
	public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder
    		.startObject(name);
        
	    if (script != null) {
	        builder.field(MultipleMetricParam.SCRIPT_FIELD.getPreferredName(), script);
	    }
	    
        if (this.params != null && !this.params.isEmpty()) {
            builder
            	.field(MultipleMetricParam.PARAMS_FIELD.getPreferredName())
            	.map(this.params);
        }
	    
	    return builder.endObject();
	}

}
