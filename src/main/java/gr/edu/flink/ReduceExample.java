package gr.edu.flink;

import gr.edu.flink.model.Purchase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceExample {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var dataFilePath = "src/main/resources/purchases";
    FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataFilePath))
        .build();

    env
        .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "purchases-source")
        .map(new PurchaseParser())
        .keyBy(Purchase::getMonth)
        .map(p -> Tuple3.of(p.getMonth(), p.getAmount(), 0))
        .keyBy(t -> t.f0)
        .reduce(new AmountReducer())
        .map(new ResultMapper())
        .print();

    env.execute("Avg Profit per month");
  }

  public static class PurchaseParser
      implements MapFunction<String, Purchase> {
    @Override
    public Purchase map(String value) {
      var words = value.split(",");
      return new Purchase(words[0], words[1], words[2], words[3], Integer.parseInt(words[4]));
    }
  }

  public static class AmountReducer implements ReduceFunction<Tuple3<String, Integer, Integer>> {

    @Override
    public Tuple3<String, Integer, Integer> reduce(
        Tuple3<String, Integer, Integer> current,
        Tuple3<String, Integer, Integer> preResult
    ) {
      return new Tuple3<>(
          current.f0,
          current.f1 + preResult.f1,
          preResult.f1 + 1
      );
    }
  }

  public static class ResultMapper
      implements
      MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> value) {
      return new Tuple2<>(
          value.f0,
          (value.f1 * 0.1) / value.f2
      );
    }
  }
}
