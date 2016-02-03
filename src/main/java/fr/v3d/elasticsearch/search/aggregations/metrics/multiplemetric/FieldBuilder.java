package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;

public class FieldBuilder implements ToXContent {

	private String name;
	private String type;
	
    private String field;
    private Script script;
    public QueryBuilder filter;

    public FieldBuilder(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public FieldBuilder field(String field) {
        this.field = field;
        return this;
    }

    public FieldBuilder script(Script script) {
        this.script = script;
        return this;
    }
    
    public FieldBuilder filter(QueryBuilder filter) {
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
        
        builder.endObject();
        
        if (filter != null) {
            builder.field("filter", filter);
        }
        
        return builder.endObject();
    }
}
