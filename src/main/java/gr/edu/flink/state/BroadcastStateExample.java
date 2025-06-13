package gr.edu.flink.state;

import static gr.edu.flink.util.Constants.SOCKET_PORT;

import gr.edu.flink.model.Employee;
import gr.edu.flink.util.MyFunctions;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {

  public static final MapStateDescriptor<String, Employee> excludeEmpDescriptor =
      new MapStateDescriptor<>(
          "resigned_employees",
          BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.of(Employee.class)
      );

  public static void main(String[] args) throws Exception {
    final var env = StreamExecutionEnvironment.getExecutionEnvironment();

    var resignedEmployees = env.socketTextStream("localhost", SOCKET_PORT);
    var excludeEmpBroadcast = resignedEmployees
        .map(new MyFunctions.EmployeeParser())
        .broadcast(excludeEmpDescriptor);

    var employeesFilePath = "src/main/resources/datasets/employees";
    FileSource<String> employeesSource = FileSource
        .forRecordStreamFormat(new TextLineInputFormat(), new Path(employeesFilePath))
        .build();

    var outputPath = "src/main/resources/outputs/employees-per-department";
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

    env
        .fromSource(employeesSource, WatermarkStrategy.noWatermarks(), "employees-source")
        .map(new MyFunctions.EmployeeParser())
        .keyBy(Employee::departmentId)
        .connect(excludeEmpBroadcast)  // will return a BroadcastConnectedStream
        .process(new ExcludeEmp())
        .map(t -> String.format("%s -> %s", t.f0, t.f1))
        .sinkTo(sink);

    env.execute("Broadcast - Employees per department");
  }

  public static class ExcludeEmp extends KeyedBroadcastProcessFunction<
      String,                     // key of the keyed stream (department)
      Employee,                   // elements of the main keyed stream
      Employee,                   // elements of the broadcast stream
      Tuple2<String, Integer>     // the output type
      > {

    // note: this state is per key
    private transient ValueState<Integer> countState;


    @Override
    public void open(OpenContext openContext) {
      ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("count", Types.INT);
      countState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Employee employee,
                               ReadOnlyContext readOnlyContext,
                               Collector<Tuple2<String, Integer>> out) throws Exception {
      final var employeeId = employee.id();
       if (readOnlyContext.getBroadcastState(excludeEmpDescriptor).contains(employeeId)){
         return;
       }
      Integer currCount = countState.value();
      if (currCount == null) currCount = 0;

      countState.update(currCount+1);
      out.collect(Tuple2.of(employee.departmentId(), currCount+1));
    }


    /*
    process the broadcasted stream - extract the key and set it to broadcast state
     */
    @Override
    public void processBroadcastElement(Employee employee,
                                        Context context,
                                        Collector<Tuple2<String, Integer>> out
    )
        throws Exception {
      var employId = employee.id();
      context.getBroadcastState(excludeEmpDescriptor).put(employId, employee);
    }
  }
}
