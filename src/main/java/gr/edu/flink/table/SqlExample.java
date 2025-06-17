package gr.edu.flink.table;

import java.time.Duration;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlExample {

  public static void main(String[] args) throws Exception {

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


    var outputPath = "src/main/resources/outputs/purchases-query-2";
    FileSink<String> sink = FileSink
        .forRowFormat(
            new Path(outputPath),
            new SimpleStringEncoder<String>("UTF-8")
        )
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15))
                .withInactivityInterval(Duration.ofMinutes(5))
                .build()
        )
        .build();

    String sql = """
        SELECT
            `month`,
            SUM(profit) AS sum1
        FROM Purchases
        WHERE category = 'Category5'
        GROUP BY `month`
        ORDER BY sum1
        """;
    var result = tableEnv.sqlQuery(sql);
    // Convert result Table to DataStream
    tableEnv.toDataStream(result)
        .map(Object::toString)
        .sinkTo(sink);

    env.execute("SQL API Example with Flink 1.19");

  }
}
