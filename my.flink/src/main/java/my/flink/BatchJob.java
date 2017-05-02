package my.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.operators.ReduceOperator;

public class BatchJob {

  public static class MyReduce implements ReduceFunction<Long> {
    public Long reduce( Long a, Long b ) {
      return a+b;
    }
  };
	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    System.out.println("print arguments");
    for ( String s : args ) {
      System.out.println(s);
    }
    String outputPath = "test.txt";
    if ( args.length > 0 )  {
      outputPath = args[0];
    }
    DataSet<Long> seq = env.generateSequence(1,1000);
    seq.print();
    ReduceOperator<Long> out = seq.reduce(new MyReduce());
    out.print();
    out.writeAsText(outputPath);

		// execute program
    System.out.println("Output Path : " + outputPath );
		env.execute("Accumulate 1 to 1000");
	}
}
