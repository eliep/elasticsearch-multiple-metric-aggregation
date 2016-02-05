package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ScriptBuilder implements ToXContent {

    private String name;
    
    private String script;
    private String lang;
    private Map<String, Object> params;

    public ScriptBuilder(String name) {
        this.name = name;
    }

    public ScriptBuilder script(String script) {
        this.script = script;
        return this;
    }

    public ScriptBuilder lang(String lang) {
        this.lang = lang;
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
            this.params = Maps.newHashMap();
        }
        this.params.put(name, value);
        return this;
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder
            .startObject(name);
        
        if (script != null) {
            builder.field("script", script);
        }
    
        if (lang != null) {
            builder.field("lang", lang);
        }
    
        if (this.params != null && !this.params.isEmpty()) {
            builder.field("params").map(this.params);
        }
        
        return builder.endObject();
    }

}
