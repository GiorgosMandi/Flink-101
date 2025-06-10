package gr.edu.flink.basic;

import gr.edu.flink.model.Purchase;
import gr.edu.flink.util.MyFunctions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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

    var mapped = env
        .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "purchases-source")
        .map(new MyFunctions.PurchaseParser())
        .map(p -> Tuple2.of(p, 1))
        .returns(Types.TUPLE(TypeInformation.of(Purchase.class), Types.INT));

    mapped.keyBy(t -> t.f0.getMonth()).sum(1).print("Sum");
    mapped.keyBy(t -> t.f0.getMonth()).min(1).print("Minimum");
    // Note: beware the other values of the tuple, are not the same as of the min/max, etc.
    // Note: to maintain also the other fields, use minBy, maxBy, etc.
    mapped.keyBy(t -> t.f0.getMonth()).minBy(1).print("Min By");
    mapped.keyBy(t -> t.f0.getMonth()).maxBy(1).print("Max By");

    env.execute("Aggregation over Profit per month");
  }
}
