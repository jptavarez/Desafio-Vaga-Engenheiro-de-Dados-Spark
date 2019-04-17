
Referências para o código: 

https://stackoverflow.com/questions/39235704/split-spark-dataframe-string-column-into-multiple-columns

https://stackoverflow.com/questions/46410887/pyspark-string-matching-to-create-new-column

https://stackoverflow.com/questions/40467936/how-do-i-get-the-last-item-from-a-list-using-pyspark

https://stackoverflow.com/questions/29600673/how-to-delete-columns-in-pyspark-dataframe

https://stackoverflow.com/questions/38080748/convert-pyspark-string-to-date-format

https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dateformat

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê? 

Porque o Spark processa os dados em memória, diferentemente do MapReduce que grava os dados em disco. O processo de ler e gravar os 
dados em disco é muito mais custoso em termos de processsamento. Contudo, o Spark também passa a gravar os dados em disco quando não 
há mais espaço na memoria.


Qual o objetivo do comando cache em Spark? 
O comando cache 

ref: https://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd



val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split("
"))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...") 
