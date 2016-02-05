package fr.v3d.elasticsearch.search.aggregations.metrics.multiplemetric;

public class SumBuilder extends FieldBuilder {

    public SumBuilder(String name) {
        super(name, MultipleMetricParser.SUM_OPERATOR);
    }

}
