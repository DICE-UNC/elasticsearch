an elasticsearch connector for iRODS indexing framework
=============
### requirements

* install elasticsearch 1.0
* create index: 

        curl -XPUT 'http://<host>:<port>/<index>' 

  default is 
        localhost 9200 databook

* create schema:

    curl -XPUT 'http://localhost:9200/databook/entity/_mapping' -d '{"properties":{"uri":{"type":"string", "index":"not_analyzed"}, "type":{"type":"string", "index":"not_analyzed"}}}'
   
* the indexing bundle  

### installation
mvn install

### search interface
open src/index.html in a browser window, if non-default host/port/index is used this file has to be modified to match them


