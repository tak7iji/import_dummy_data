from datetime import datetime
from datetime import timedelta
from pytz import timezone
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
import ast
argvs = sys.argv

jst = timezone('Japan')
utc = timezone('UTC')
start_jst = datetime.strptime(argvs[1], '%Y-%m-%d').replace(tzinfo=jst)
secs=60*60*24*30
#secs=3

str=[
'{"@timestamp":"%s","metricset":{"module":"system","rtt":138,"name":"cpu"},"system":{"cpu":{"nice":{"pct":0},"irq":{"pct":0},"system":{"pct":0},"iowait":{"pct":0},"softirq":{"pct":0},"total":{"pct":0},"cores":4,"idle":{"pct":0},"steal":{"pct":0},"user":{"pct":0}}},"beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"}}',
'{"@timestamp":"%s","system":{"load":{"1":3.3169,"5":4.1338,"15":4.9922,"norm":{"1":0.8292,"5":1.0334,"15":1.248},"cores":4}},"beat":{"version":"6.1.3","name":"testdata","hostname":"testdata"},"metricset":{"name":"load","module":"system","rtt":102}}',
'{"@timestamp":"%s","metricset":{"name":"memory","module":"system","rtt":131},"system":{"memory":{"used":{"bytes":4277305344,"pct":0.9959},"free":17661952,"actual":{"used":{"pct":0.7236,"bytes":3107885056},"free":1187082240},"swap":{"total":1073741824,"used":{"bytes":525860864,"pct":0.4897},"free":547880960},"total":4294967296}},"beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"}}',
'{"@timestamp":"%s","metricset":{"name":"network","module":"system","rtt":421818},"system":{"network":{"name":"lo0","in":{"errors":0,"dropped":0,"bytes":3177049913,"packets":2961928},"out":{"packets":2961928,"bytes":3177049913,"errors":0,"dropped":0}}},"beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"}}',
'{"@timestamp":"%s","metricset":{"module":"system","rtt":233636,"name":"process"},"system":{"process":{"pgid":4450,"username":"mash","cmdline":"./metricbeat","pid":4450,"ppid":3925,"memory":{"size":571921035264,"rss":{"bytes":22167552,"pct":0.0052},"share":0},"name":"metricbeat","state":"running","cpu":{"total":{"pct":0,"norm":{"pct":0},"value":0},"start_time":"2018-02-13T12:14:39.872Z"}}},"beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"}}',
'{"@timestamp":"%s","metricset":{"name":"process_summary","module":"system","rtt":12362,"namespace":"system.process.summary"},"system":{"process":{"summary":{"idle":0,"stopped":0,"zombie":0,"unknown":137,"total":298,"sleeping":0,"running":161}}},"beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"}}',
'{"@timestamp":"%s","metricset":{"name":"diskio","module":"system","rtt":5161},"system":{"diskio":{"write":{"count":2730742,"time":1413097,"bytes":50394614272},"io":{"time":4248084},"name":"disk0","read":{"count":2707176,"time":2834987,"bytes":50290068992}}},"beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"}}',
'{"@timestamp":"%s","metricset":{"module":"system","rtt":45,"name":"uptime"},"system":{"uptime":{"duration":{"ms":354730414}}},"beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"}}',
'{"@timestamp":"%s","beat":{"name":"testdata","hostname":"testdata","version":"6.1.3"},"metricset":{"name":"filesystem","module":"system","rtt":168},"system":{"filesystem":{"free_files":9223372036854174,"used":{"pct":0.6715,"bytes":45076471808},"available":18173829120,"type":"apfs","total":67123523584,"free":22047051776,"files":9223372036854776,"device_name":"/dev/disk1s1","mount_point":"/"}}}',
'{"@timestamp":"%s","metricset":{"module":"system","rtt":193,"name":"fsstat"},"system":{"fsstat":{"total_files":2929252,"total_size":{"free":88686333952,"used":99559567360,"total":188245901312},"count":6}},"beat":{"hostname":"testdata","version":"6.1.3","name":"testdata"}}'
]

es=Elasticsearch(host='127.0.0.1', port=9200)
actions=[]

for i in range(0,secs,10):
  dt_jst = start_jst + timedelta(seconds=i)
  dt_utc = dt_jst.astimezone(utc).isoformat()
  bulk = {'_index': 'metricbeat-6.1.3-'+dt_jst.strftime('%Y.%m.%d'), '_type': 'doc'}
  actions=[
    {
      '_index': 'metricbeat-6.1.3-'+dt_jst.strftime('%Y.%m.%d'),
      '_type': 'doc',
      '_source': ast.literal_eval(str[j] % dt_utc)
    }
    for j in range(10)
  ]

  helpers.bulk(es, actions)
  actions=[]
