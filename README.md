# Elasticsearch Multiple Metric Aggregation

This plugin add a multi-value metrics aggregation which can define and reuse several metrics. Each metric value is build either from an aggregator (sum or count) and a numeric field in the document, or generated from a script (using the previous metric defined). Because it's a multi-value metrics aggregation; each returned value can be used to sort a parent terms aggregation.

## Installation

### Versions

| elasticsearch version | latest version |
| --------------------- | -------------  |
| 1.4.5+                | 1.4.7          |
| 2.0+                  | 2.0.0          |
| 2.2+                  | 2.2.0          |
| 2.3.2                 | 2.3.1          |
| 2.3.5                 | 2.3.5.1        |
| 2.4.0                 | 2.4.0.0        |
| 2.4.1                 | 2.4.1.0        |
| 2.4.2                 | 2.4.2.0        |
| 2.4.3                 | 2.4.3.0        |
| 2.4.4                 | 2.4.4.0        |
| 2.4.5                 | 2.4.5.0        |

### Install as plugin 

Up to 2.0.0: 
```
bin/plugin --url https://github.com/eliep/elasticsearch-multiple-metric-aggregation/releases/download/2.0.0/elasticsearch-multiple-metric-aggregation-2.0.0.zip install elasticsearch-multiple-metric-aggregation
```

After:
```
bin/plugin install https://github.com/eliep/elasticsearch-multiple-metric-aggregation/releases/download/2.3.1/elasticsearch-multiple-metric-aggregation-2.3.1.zip
```

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
        "sum_x" : { "sum" : { "field": "x" } }
        "sum_y" : { "sum" : { "field": "y" } }
	  } 
    }
  }
}
``` 

The above will returns the following:

```
"aggregations": {
  "metrics": {
    "sum_x": {
      "value": 80.0,
      "doc_count": 6
    },
    "sum_y": {
      "value": 20.0,
      "doc_count": 6
    },
    "ratio": {
      "value": 4.0
    }
  }
}
``` 

You can also use it to compute a weighted average: 
```
{
  "aggs" : {
    "ranking" : { 
      "multiple-metric" : { 
        "ratio" : { "script": "sum_x / sum_y" },
        "sum_x" : { "sum" : { "script": "x * y" } }
        "sum_y" : { "sum" : { "field": "y" } }
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
            "sum_x" : { "sum" : { "field": "x" } }
            "sum_y" : { "sum" : { "field": "y" } }
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
        "sum_x" : { "sum" : { "field": "x" }, "filter": { "term" : { "tag" : "a" } } },
        "sum_y" : { "sum" : { "field": "y" } }
	  } 
    }
  }
}
```


### Script parameters
Script parameters are also allowed:

```
{
  "aggs" : {
    "ranking" : { 
      "multiple-metric" : { 
        "ratio" : { "script": "sum_x * factor / sum_y", "params" : { "factor": 2 } },
        "sum_x" : { "sum" : { "field": "x" }, "filter": { "term" : { "tag" : "a" } } },
        "sum_y" : { "sum" : { "field": "y" } }
      } 
    }
  }
}
```

## Todo

 * Script metric can only be defined inline
 * Add min/max operator