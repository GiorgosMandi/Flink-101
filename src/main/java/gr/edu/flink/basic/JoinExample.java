package gr.edu.flink.basic;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

public class JoinExample {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(parameterTool);
    // load `person` file and parse it into Tuple<Integer, String>
    var personFilePath = "src/main/resources/person";
    FileSource<String> personSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(personFilePath))
        .build();
    DataStream<Tuple2<Integer, String>> personStream = env
        .fromSource(personSource, WatermarkStrategy.noWatermarks(),"person-source")
        .map(new LineParser());

    personStream.print();


    // load `location` file and parse it into Tuple<Integer, String>
    var locationFilePath = "src/main/resources/location";
    FileSource<String> locationSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(locationFilePath))
        .build();
    DataStream<Tuple2<Integer, String>> locationStream = env
        .fromSource(locationSource, WatermarkStrategy.noWatermarks(),"location-source")
        .map(new LineParser());

    locationStream.print();

    // Warning: This join produces no results!
    //  Even though the window is 100 seconds, the bounded input files are processed too quickly and independently.
    //  Due to how processing time windows work, the two streams' records might not overlap in time from Flink's point of view.
    //  For joining bounded data, use the Table API or enable batch execution mode.
    // Note: Using `JoinOperatorBase.JoinHint` we can instruct Flink which join algorithm to use
    // perform join
    DataStream<Tuple3<Integer, String, String>> joined = personStream
        .join(locationStream)
        .where(p -> p.f0)
        .equalTo(l -> l.f0)
        .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(100)))
        .apply(new PersonLocationJoinFunction());

    var outputPath = "src/main/resources/inner-joins/output";
    joined.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);
    joined.print();

    env.execute("Join Example");
  }

  public static class LineParser implements MapFunction<String, Tuple2<Integer, String>> {
    @Override
    public Tuple2<Integer, String> map(String value) {
      var words = value.split(",");
      return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
    }
  }

  public static class PersonLocationJoinFunction implements
      JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {

    @Override
    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,
                                                Tuple2<Integer, String> location
    ) throws Exception {
      return new Tuple3<>(person.f0, person.f1, location.f1);
    }
  }


}
