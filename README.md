## elasticsearch-river-dianping
-----------
The dianping river indexes [dianping](http://www.dianping.com/) open api data. 

install the plugin, run:
```
bin/plugin -install jiaoqsh/elasticsearch-river-dianping
```

###Prerequisites
You need to get an  token in order to use dianping river. Please follow [dianping documentation](http://developer.dianping.com/index).

###Create river
Creating the dianping river can be done using:
```
curl -XPUT localhost:9200/_river/dianping_river/_meta -d '
{
    "type" : "dianping",
    "dianping" : {
        "app" : {
            "appKey" : "xxx",
            "secret" : "xxx"
        },
        "appType" : "deal"
    },
    "index" : {
        "index" : "my_dianping_river",
        "type" : "deal",
        "bulk_size" : 100,
        "flush_interval" : "10s"
    }
}
'
```
###Search test
query index mapping:
```
curl -XGET  http://localhost:9200/my_dianping_river/_mapping?pretty=true
```
search city deal:
```
curl -XGET  http://localhost:9200/my_dianping_river/_search?q=city:*
```



