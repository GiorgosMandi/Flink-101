package gr.edu.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationExample {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var dataFilePath = "src/main/resources/avg";
    FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataFilePath))
        .build();

    var mapped = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "avg-source")
        .map(new LineParser());

    mapped.keyBy(t -> t.f0).sum(3).print("Sum");
    mapped.keyBy(t -> t.f0).min(3).print("Minimum");
    // Note: beware the other values of the tuple, are not the same as of the min/max, etc.
    // Note: to maintain also the other fields, use minBy, maxBy, etc.
    mapped.keyBy(t -> t.f0).minBy(3).print("Min By");
    mapped.keyBy(t -> t.f0).maxBy(3).print("Max By");

    env.execute("Aggregation over Profit per month");
  }

  public static class LineParser
      implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
    @Override
    public Tuple5<String, String, String, Integer, Integer> map(String value) {
      var words = value.split(",");
      return new Tuple5<>(
          words[1],
          words[2],
          words[3],
          Integer.parseInt(words[4]),
          1 // denominator sum here
      );
    }
  }
}
