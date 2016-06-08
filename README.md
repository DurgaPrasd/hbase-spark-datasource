# hbase-spark-datasource
HBase datasource for SparkSQL and DataFrames.

This project is based on **Spark 1.6** and **HBase 0.98.17-hadoop2**.

## quick start
To use this package, you need to get the compiled jar file into every node of your cluster, and place them into extra classpath of your spark driver and executors.
And you should add the `hbase-spark-datasource.jar` together with extra jars under `HBASE_HOME/lib`.

### Usage in Spark-shell

    ./spark-shell --jars hbase-spark-datasource.jar:HBASE_HOME/lib/*.jar

you should write the basic catalog of your table and pass it to the connector.

    import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
    
    val catalog = s"""{"table":{"namespace":"default", "name":"test2"},"rowkey":"key","columns":{"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
                     |"A_FIELD":{"cf":"cf2", "col":"a", "type":"string"}
                     |}
                     |}""".stripMargin
                     
    val df = sqlContext.load("org.apache.hadoop.hbase.spark",Map(HBaseTableCatalog.tableCatalog->catalog,"hbase.config.resources"->"YOUR_HBASE_HOME/conf/hbase-site.xml"))

`table` means the table you take operations on, you can assign namespace and name here.

`rowkey` means the row key. The value is always `"rowkey":"key"`.

`columns` includes every columns with column family and type of the table. 

You **must state rowkey as a column called KEY_FIELD** here. And other rows one by one.

Now, you can use `sqlContext.load()` to load data and build related dataframe.

The first parameter should be `"org.apache.hadoop.hbase.spark"`.

You can put some customize enviroment variables in the second parameter. But as least you should assign your catalog to `HBaseTableCatalog.tableCatalog` and your HBase configuration file to `"hbase.config.resources"`.
