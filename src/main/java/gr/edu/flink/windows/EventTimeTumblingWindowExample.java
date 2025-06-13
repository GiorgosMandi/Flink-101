package gr.edu.flink.windows;

import static gr.edu.flink.util.Constants.SOCKET_PORT;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

public class EventTimeTumblingWindowExample {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.socketTextStream("localhost", SOCKET_PORT)
        .map(l -> {
          var parts = l.split(",");
          return new Tuple2<>(Long.parseLong(parts[0]), Integer.parseInt((parts[1])));
        })
        .returns(Types.TUPLE(Types.LONG, Types.INT))
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Tuple2<Long, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.f0)
        )
        .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(2)))
        .reduce((t1, t2) -> Tuple2.of(System.currentTimeMillis(), t1.f1 + t2.f1))
        .print();
    env.execute("Random numbers, streamed from socket");
  }
}