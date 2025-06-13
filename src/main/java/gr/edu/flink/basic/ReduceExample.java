package gr.edu.flink.basic;

import gr.edu.flink.model.Purchase;
import gr.edu.flink.util.MyFunctions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceExample {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var dataFilePath = "src/main/resources/datasets/purchases";
    FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataFilePath))
        .build();

    env
        .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "purchases-source")
        .map(new MyFunctions.PurchaseParser())
        .keyBy(Purchase::month)
        .map(p -> Tuple3.of(p.month(), p.amount(), 0))
        .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
        .keyBy(t -> t.f0)
        .reduce(new MyFunctions.AmountReducer())
        .map(new MyFunctions.ResultMapper())
        .print();

    env.execute("Avg Profit per month");
  }


}
