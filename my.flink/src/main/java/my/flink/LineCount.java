package my.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

public class LineCount {

  public static final class LineMap implements MapFunction<String, Integer> {
    public Integer map(String value) { return 1; }
  }

  public static final class LineReduce implements ReduceFunction<Integer> {
    public Integer reduce(Integer a, Integer b) { return a+b; }
  }

  public static void main(String[] args) throws Exception {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);

    if ( params.has("file") == false ) {
      System.out.println("Use --file to specify file input.");
      System.exit(0);
    } 

    DataSet<String> text = env.readTextFile(params.get("file"));
    text.print();
    DataSet<Integer> mapDataSet = text.map(new LineMap());
    System.out.println("Map DataSet");
    mapDataSet.print();
    DataSet<Integer> reduceDataSet = mapDataSet.reduce(new LineReduce());
    System.out.println("Reduce DataSet");
    reduceDataSet.print();
    List<Integer> size = reduceDataSet.collect();
    System.out.println("lines : " + size.get(0));

  }
}
