package gr.edu.flink;

import gr.edu.flink.model.Purchase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationExample {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var dataFilePath = "src/main/resources/purchases";
    FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataFilePath))
        .build();

    var mapped = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "purchases-source")
        .map(new PurchaseParser())
        .map(p -> Tuple2.of(p, 1));

    mapped.keyBy(t -> t.f0.getMonth()).sum(3).print("Sum");
    mapped.keyBy(t -> t.f0.getMonth()).min(3).print("Minimum");
    // Note: beware the other values of the tuple, are not the same as of the min/max, etc.
    // Note: to maintain also the other fields, use minBy, maxBy, etc.
    mapped.keyBy(t -> t.f0.getMonth()).minBy(3).print("Min By");
    mapped.keyBy(t -> t.f0.getMonth()).maxBy(3).print("Max By");

    env.execute("Aggregation over Profit per month");
  }

  public static class PurchaseParser
      implements MapFunction<String, Purchase> {
    @Override
    public Purchase map(String value) {
      var words = value.split(",");
      return new Purchase(words[0], words[1], words[2], words[3], Integer.parseInt(words[4]));
    }
  }
}
