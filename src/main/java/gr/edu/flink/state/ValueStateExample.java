package gr.edu.flink.state;

import static gr.edu.flink.util.Constants.SOCKET_PORT;

import java.time.Duration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

public class ValueStateExample {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var outputPath = "src/main/resources/outputs/value-state";
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

    env.socketTextStream("localhost", SOCKET_PORT)
        .map(l -> {
          var parts = l.split(",");
          return new Tuple2<>(Long.parseLong(parts[0]), parts[1]);
        })
        .returns(Types.TUPLE(Types.LONG, Types.STRING))
        .keyBy(t -> t.f0)
        .flatMap(new StatefulMap())
        .map(Object::toString)
        .sinkTo(sink);

    // execute program
    env.execute("State");

  }


  static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {
    private transient ValueState<Long> sum;
    private transient ValueState<Long> count;

    public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
      Long currCount = 0L;
      Long currSum = 0L;

      if (count.value() != null) {
        currCount = count.value();
      }
      if (sum.value() != null) {
        currSum = sum.value();
      }

      currCount += 1;
      currSum = currSum + Long.parseLong(input.f1);

      count.update(currCount);
      sum.update(currSum);

      if (currCount >= 10) {
        /* emit sum of last 10 elements */
        out.collect(sum.value());
        /* clear value */
        count.clear();
        sum.clear();
      }
    }

    @Override
    public void open(Configuration conf) {
      ValueStateDescriptor<Long> descriptor =
          new ValueStateDescriptor<>("sum", TypeInformation.of(new TypeHint<>() {}));

      sum = getRuntimeContext().getState(descriptor);

      ValueStateDescriptor<Long> descriptor2 =
          new ValueStateDescriptor<>("count", TypeInformation.of(new TypeHint<>() {}));

      count = getRuntimeContext().getState(descriptor2);
    }
  }
}
