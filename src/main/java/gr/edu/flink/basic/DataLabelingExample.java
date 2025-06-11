package gr.edu.flink.basic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DataLabelingExample {

  public static void main(String[] args) throws Exception {

    // define labels/tags
    final OutputTag<Integer> evenTag = new OutputTag<>("even", Types.INT);
    final OutputTag<Integer> oddTag = new OutputTag<>("odd", Types.INT);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var dataFilePath = "src/main/resources/datasets/numbers";
    FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataFilePath))
        .build();

    var processed = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "purchases-source")
        .map(Integer::parseInt)
        .process(
            // tag values
            new ProcessFunction<Integer, Integer>() {
              @Override
              public void processElement(Integer value, Context ctx, Collector<Integer> out) {
                if (value % 2 == 0) {
                  ctx.output(evenTag, value);
                } else {
                  ctx.output(oddTag, value);
                }
              }
            });

    // get tagged values of stream
    DataStream<Integer> evenData = processed.getSideOutput(evenTag);
    DataStream<Integer> oddData = processed.getSideOutput(oddTag);

    evenData.writeAsText("src/main/resources/datasets/outputs/even");
    oddData.writeAsText("src/main/resources/datasets/outputs/odd");

    env.execute("Labeling Example");
  }
}
