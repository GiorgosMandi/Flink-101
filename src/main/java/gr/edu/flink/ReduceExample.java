package gr.edu.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceExample {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var dataFilePath = "src/main/resources/avg";
    FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataFilePath))
        .build();

    env
        .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "avg-source")
        .map(new LineParser())
        .keyBy(t -> t.f0)
        .reduce(new MyReducer())
        .map(new ResultMapper())
        .print();

    env.execute("Avg Profit per month");
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

  public static class MyReducer
      implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {

    @Override
    public Tuple5<String, String, String, Integer, Integer> reduce(
        Tuple5<String, String, String, Integer, Integer> current,
        Tuple5<String, String, String, Integer, Integer> preResult) throws Exception {
      return new Tuple5<>(
          current.f0,
          current.f1,
          current.f2,
          current.f3 + preResult.f3,
          current.f4 + preResult.f4
      );
    }
  }

  public static class ResultMapper
      implements
      MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> value) {
      return new Tuple2<>(
          value.f0,
          (value.f3 * 0.1) / value.f4
      );
    }
  }
}
