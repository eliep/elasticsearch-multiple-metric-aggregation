package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

public class MultipleMetricBuilder extends AggregationBuilder<MultipleMetricBuilder> {
    

    private List<ToXContent> metrics = new ArrayList<ToXContent>(10);

    public MultipleMetricBuilder(String name) {
        super(name, InternalMultipleMetric.TYPE.name());
    }
    
    public MultipleMetricBuilder field(FieldBuilder fieldBuilder) {
        this.metrics.add(fieldBuilder);
        return this;
    }
    
    public MultipleMetricBuilder script(ScriptBuilder scriptBuilder) {
        this.metrics.add(scriptBuilder);
        return this;
    }
    
    
    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (ToXContent toXContent: this.metrics) 
            toXContent.toXContent(builder, params);

        return builder.endObject();
    }

}
