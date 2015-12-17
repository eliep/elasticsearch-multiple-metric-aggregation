# Elasticsearch Multiple Metric Aggregation

This plugin add a multi-value metrics aggregation which can define and reuse several metrics. Each metric value is build either from an aggregator (sum or count) and a numeric field in the document, or generated from a script (using the previous metric defined). Because it's a multi-value metrics aggregation; each returned value can be used to sort a parent terms aggregation.

## Installation
sudo bin/plugin --url https://github.com/eliep/elasticsearch-multiple-metric-aggregation/files/65041/elasticsearch-multiple-metric-aggregation-1.4.zip install elasticsearch-multiple-metric-aggregation

## Examples

### Basics
Given these documents:
```
{ "id": "1", "x": 13, "y": 1, "tag": "a" }
{ "id": "1", "x": 15, "y": 4, "tag": "b" }
{ "id": "1", "x": 12, "y": 2, "tag": "a" }
{ "id": "2", "x": 14, "y": 1, "tag": "a" }
{ "id": "2", "x": 15, "y": 3, "tag": "a" }
{ "id": "2", "x": 11, "y": 9, "tag": "b" }
```

You can compute the ratio sum(x) / sum(y) with the following aggregation: 
```
{
  "aggs" : {
    "ranking" : { 
      "multiple-metric" : { 
        "ratio" : { "script": "sum_x / sum_y" },
        "sum_x" : { "field": "x", "operator": "sum" }
        "sum_y" : { "field": "y", "operator": "sum" }
	  } 
    }
  }
}
``` 

### Ordering a terms aggregation
Because it's a multi-value metrics aggregation, it can be used to order a terms aggregation:
```
{
  "aggs": {
    "group_by": {
      "terms": {
        "field": "id",
        "order": { "ranking.ratio": "asc" }
      },
      "aggs" : {
        "ranking" : { 
          "multiple-metric" : { 
            "ratio" : { "script": "sum_x / sum_y" },
            "sum_x" : { "field": "x", "operator": "sum" }
            "sum_y" : { "field": "y", "operator": "sum" }
          } 
	    }
      }
    }
  }
}
```

### Filtering
It's also possible to add a filter on a metric value computed from a document field:

```
{
  "aggs" : {
    "ranking" : { 
      "multiple-metric" : { 
        "ratio" : { "script": "sum_x / sum_y" },
        "sum_x" : { "field": "x", "operator": "sum", "filter": { "term" : { "tag" : "a" } } }
        "sum_y" : { "field": "y", "operator": "sum" }
	  } 
    }
  }
}
```
## Limitation

 * count operator can only be applied to numeric fields
 * script metric can only be defined inline
 * field metric parameter cannot be defined as a script (not possible to compute the sum(x * y) for example)

## Todo

 * Script parameter can only be inline (should use the [script format](https://www.elastic.co/guide/en/elasticsearch/reference/1.4/modules-scripting.html))
 * Allow script parameters
 * Field formating
 * Add min/max operator
 * Add '*' as a special field for count operator