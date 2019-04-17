# import findspark
# findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, size, to_timestamp, to_date, col, sum

spark = SparkSession.builder.appName('Desafio Engenheiro de Dados Spark').getOrCreate()

file = spark.read.text('files')

split_col = split(file['value'], ' - - ')
split_by_space = split(file['value'], ' ')

df = file.withColumn('host', split_col.getItem(0)) \
    .withColumn('timestamp', regexp_extract(split_col.getItem(1), '\[(.*?)\]', 1))

df = df.withColumn('timestamp', to_timestamp(df.timestamp, 'dd/MMM/yyyy:HH:mm:ss')) \
    .withColumn('request', regexp_extract(split_col.getItem(1), '\"(.*?)\"', 1)) \
    .withColumn('http_code', split_by_space.getItem(size(split_by_space) - 2)) \
    .withColumn('bytes', split_by_space.getItem(size(split_by_space) - 1)) \
    .drop('value')

hosts_number = df.groupBy("host").count().count()
print('1. Número de hosts unicos: ', hosts_number)

not_found_errors_df = df.filter(df['http_code'] == '404')

not_found_errors = not_found_errors_df.count()
print('2. Total de erros 404: ', not_found_errors)

print('3. Os 5 URLs que mais causaram erro 404: ')
not_found_errors_df.groupBy('host').count().orderBy('count', ascending=False).show(5)

print('4. Quantidade de erros 404 por dia: (mostrando apenas os primeiros 10 dias)')
not_found_errors_df.groupBy(to_date(col('timestamp')).alias('date')).count().orderBy('date').show(10)

bytes_total = df.agg(sum('bytes')).collect()[0][0]
print('5. O total de bytes retornados: ', bytes_total)