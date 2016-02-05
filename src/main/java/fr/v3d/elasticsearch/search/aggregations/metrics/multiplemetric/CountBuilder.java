package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

public class CountBuilder extends FieldBuilder {

    public CountBuilder(String name) {
        super(name, MultipleMetricParser.COUNT_OPERATOR);
    }
}
