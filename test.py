
from pyspark import SparkContext, SparkConf
from pyspark.sql import  SQLContext
from pyspark.sql.types import *



# instanciando o Spark e SQLContext
conf=SparkConf().setAppName('semantix')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def splitFile(l):

    try:

        host = l[:l.index(' -')]
        timestamp = l[l.index('[')+1:l.index(']')]
        data = l[l.index('[')+1:l.index('[')+12]
        url = l[l.index(' "')+2:l.index('" ')]
        codigo = l[l.index('" ')+2:][:l[l.index('" ')+2:].index(' ')]
        bytes = l[l.index('" ')+2:][l[l.index('" ')+2:].index(' '):]

        return [
            'OK', host, timestamp, data, url, codigo, bytes
        ]
    except Exception as e:
        print (l);
    return ['NOK', l]



rdd = sc.textFile('/user/x197955/access_log_Aug95')
logs = rdd.map(lambda l : splitFile(l))
logs.cache()
#logs.filter(lambda l : l[0] == "NOK").saveAsTextFile('/user/cloudera/dataset/out_nok')
#logs.filter(lambda l : l[0] == "OK").saveAsTextFile('/user/cloudera/dataset/out_ok')

tbDf = sqlContext.createDataFrame(logs.filter(lambda l : l[0] == "OK"),['status', 'host', 'timestamp', 'data', 'url', 'codigo', 'bytes'])
tbDf.registerTempTable( "logs" )

###### PERGUNTAS ########

# p1
sqlContext.sql("select count(1), 'hosts unicos' from (select host from logs group by host) as hosts").show()

# p2
sqlContext.sql("select count(1), 'erros 404' from logs  where codigo = '404'").show()

# p3
sqlContext.sql("select timestamp from (select host, count(1)  from logs  where codigo = '404' group by host) as log404 order by 2 desc limit 5").show()

# p4
sqlContext.sql("select data, count(1) from logs where codigo = '404' group by data").show()

# p5
sqlContext.sql("select sum(bytes) from logs").show()



