package gr.edu.flink.table;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableApiExample {

  public static void main(String[] args) {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // Register table
    final String tableDDL = """
          CREATE TEMPORARY TABLE Purchases (
            `date` STRING,
            `month` STRING,
            `category` STRING,
            `product` STRING,
            `profit` INT
          ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///home/gmandi/Documents/my-documents/my-projects/Educational/Flink-101/src/main/resources/datasets/purchases',
            'format' = 'csv'
          )
        """;
    tableEnv.executeSql(tableDDL);


    Table result = tableEnv.from("Purchases")
        .filter($("category").isEqual("Category5"))
        .groupBy($("month"))
        .select($("month"), $("profit").sum().as("sum"))
        .orderBy($("sum"));

    // Define output sink
    final String sinkDDL = """
          CREATE TEMPORARY TABLE ResultSink (
            `month` STRING,
            `sum` BIGINT
          ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///home/gmandi/Documents/my-documents/my-projects/Educational/Flink-101/src/main/resources/outputs/purchases-query',
            'format' = 'csv'
          )
        """;
    tableEnv.executeSql(sinkDDL);

    // Write to sink
    result.executeInsert("ResultSink");
  }
}
