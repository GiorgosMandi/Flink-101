package gr.edu.flink.basic;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DataLabelingExample {

  public static void main(String[] args) throws Exception {

    // define labels/tags
    final OutputTag<Integer> evenTag = new OutputTag<>("even", Types.INT);
    final OutputTag<Integer> oddTag = new OutputTag<>("odd", Types.INT);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // source
    var dataFilePath = "src/main/resources/datasets/numbers";
    FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataFilePath))
        .build();

    // even sink
    var evenOutputPath = "src/main/resources/outputs/even";
    FileSink<String> evenSink = FileSink
        .forRowFormat(
            new Path(evenOutputPath),
            new SimpleStringEncoder<String>("UTF-8")
        )
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15))
                .withInactivityInterval(Duration.ofMinutes(5))
                .build()
        )
        .build();

    // odd sink
    var oddOutputPath = "src/main/resources/outputs/odd";
    FileSink<String> oddSink = FileSink
        .forRowFormat(
            new Path(oddOutputPath),
            new SimpleStringEncoder<String>("UTF-8")
        )
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15))
                .withInactivityInterval(Duration.ofMinutes(5))
                .build()
        )
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
    processed.getSideOutput(evenTag).map(Object::toString).sinkTo(evenSink);
    processed.getSideOutput(oddTag).map(Object::toString).sinkTo(oddSink);

    env.execute("Labeling Example");
  }
}
