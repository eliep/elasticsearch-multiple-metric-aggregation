package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;

public class FieldBuilder implements ToXContent {

    private String name;
    private String type;
    
    private String field;
    private String script;
    private String lang;
    private Map<String, Object> params;
    public FilterBuilder filter;

    public FieldBuilder(String name, String type) {
        this.name = name;
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public FieldBuilder field(String field) {
        this.field = field;
        return this;
    }

    public FieldBuilder script(String script) {
        this.script = script;
        return this;
    }

    public FieldBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    public FieldBuilder params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return this;
    }

    public FieldBuilder param(String name, Object value) {
        if (this.params == null) {
            this.params = Maps.newHashMap();
        }
        this.params.put(name, value);
        return this;
    }
    
    public FieldBuilder filter(FilterBuilder filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder
            .startObject(name)
            .startObject(type);

        if (field != null) {
            builder.field("field", field);
        }

        if (script != null) {
            builder.field("script", script);
        }

        if (lang != null) {
            builder.field("lang", lang);
        }

        if (this.params != null && !this.params.isEmpty()) {
            builder.field("params").map(this.params);
        }
        builder.endObject();
        
        if (filter != null) {
            builder.field("filter", filter);
        }
        
        return builder.endObject();
    }
}
