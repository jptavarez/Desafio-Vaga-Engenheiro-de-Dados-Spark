
Referências para o código: 

https://stackoverflow.com/questions/39235704/split-spark-dataframe-string-column-into-multiple-columns

https://stackoverflow.com/questions/46410887/pyspark-string-matching-to-create-new-column

https://stackoverflow.com/questions/40467936/how-do-i-get-the-last-item-from-a-list-using-pyspark

https://stackoverflow.com/questions/29600673/how-to-delete-columns-in-pyspark-dataframe

https://stackoverflow.com/questions/38080748/convert-pyspark-string-to-date-format

https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dateformat

**O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?** 

Porque o Spark processa os dados em memória, diferentemente do MapReduce que grava os dados em disco. O processo de ler e gravar os 
dados em disco é muito mais custoso em termos de processsamento. Contudo, o Spark também passa a gravar os dados em disco quando não 
há mais espaço na memoria.


**Qual o objetivo do comando cache em Spark?** 

O comando cache é util quando em algum momento o RDD seguirá mais de um caminho de utilização, ou seja, ele não terá um caminho único e linear. Então, se por exemplo o método count for chamado duas vezes, na primeira chamada o count será executado e o Spark criará um checkpoint, e na segunda chamada o Spark utilizará o checkpoint criado e economizará processamento. 
ref: https://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd

**Qual é a função do SparkContext?**

O SparkContext é o cliente do ambiente de execução do Spark. Ele permite a sua aplicação Spark acessar o Cluster Spark com a ajuda do Resource Manager (YARN/Mesos). Só é possível criar RDDs, acessar serviços do Spark ou rodar jobs após a criação do SparkContext.

Refs: 

https://data-flair.training/forums/topic/what-is-sparkcontext-in-apache-spark/

https://data-flair.training/blogs/learn-apache-spark-sparkcontext/

**Explique com suas palavras o que é Resilient Distributed Datasets (RDD).** 

RDDs são a principal abstração que o Spark oferece para trabalhar com dados distribuidos ou em paralelo. O desenvolvedor não precisa se preocurar com a arquitetura e lógica do processamento distribuido, pois os RDDs abstraem tudo isso. Até mesmo DataFrames e DataSets utilizam RDDs.

ref: https://spark.apache.org/docs/latest/rdd-programming-guide.html

**GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**

Por conta de sua lógica de execução, o GroupByKey transfere muito mais dados pela rede.

ref: https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split("
"))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...") 
