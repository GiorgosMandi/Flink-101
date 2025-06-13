package gr.edu.flink.util;

import gr.edu.flink.model.Employee;
import gr.edu.flink.model.Purchase;
import lombok.experimental.UtilityClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

@UtilityClass
public class MyFunctions {

  public static class PurchaseParser implements MapFunction<String, Purchase> {
    @Override
    public Purchase map(String value) {
      var words = value.split(",");
      return new Purchase(words[0], words[1], words[2], words[3], Integer.parseInt(words[4]));
    }
  }

  public static class EmployeeParser implements MapFunction<String, Employee> {
    @Override
    public Employee map(String value) {
      var words = value.split(",");
      return new Employee(words[0], words[1], words[2], words[3], words[4]);
    }
  }

  public static class ResultMapper implements
      MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> value) {
      return new Tuple2<>(
          value.f0,
          (value.f1 * 0.1) / value.f2
      );
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
}
