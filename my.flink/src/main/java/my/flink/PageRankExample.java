package my.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

public class PageRankExample {
  private static double damper_factor = 0.85;
	private static final double EPSILON = 0.0001;

  public static final class LineMap implements MapFunction<String, Integer> {

    @Override
    public Integer map(String value) {
      return 1;
    }
  }

  public static final class LineReduce implements ReduceFunction<Integer> {
    public Integer reduce(Integer a, Integer b) {
      return a+b;
    }
  }

  public static void main(String[] args) throws Exception {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);

    if ( params.has("pages") == false ) {
      System.out.println("Use --pages to specify file input.");
      System.exit(0);
    } 
    if ( params.has("links") == false ) {
      System.out.println("Use --links to specify file input.");
      System.exit(0);
    } 

    DataSet<String> text = env.readTextFile(params.get("pages"));
    List<Integer> size = text.map(new LineMap()).reduce(new LineReduce()).collect();
    System.out.println("number of pages : " + size.get(0));
    final int numPages = size.get(0);
    final int maxIterations = params.getInt("iterations", 10);
    damper_factor = params.getDouble("damper", 0.85);


    DataSet<Long> pagesInput = getPagesDataSet(env, params);
    DataSet<Tuple2<Long, Long>> linksInput = getLinksDataSet(env, params);
    DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.map(new RankAssigner((1.0d / numPages)));
    DataSet<Tuple2<Long, Long[]>> adjacencyListInput = linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());
    IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);

    DataSet<Tuple2<Long, Double>> newRanks = iteration
      .join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
      .groupBy(0).aggregate(SUM, 1)
      .map(new Dampener(damper_factor, numPages));

    DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith( newRanks, 
        newRanks.join(iteration).where(0).equalTo(0)
        .filter(new EpsilonFilter()));

    System.out.println("Printing result to stdout. Use --output to specify output path.");
    finalPageRanks.print();

  }

  public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
    Tuple2<Long, Double> outPageWithRank;

    public RankAssigner(double rank) {
      this.outPageWithRank = new Tuple2<Long, Double>(-1L, rank);
    }

    @Override
    public Tuple2<Long, Double> map(Long page) {
      outPageWithRank.f0 = page;
      return outPageWithRank;
    }
  }

  @ForwardedFields("0")
  public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {
    private final ArrayList<Long> neighbors = new ArrayList<Long>();
    @Override
    public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
      neighbors.clear();
      Long id = 0L;

      for (Tuple2<Long, Long> n : values) {
        id = n.f0;
        neighbors.add(n.f1);
      }
      out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
    }
  }

  public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {
    @Override
    public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out){
      Long[] neighbors = value.f1.f1;
      double rank = value.f0.f1;
      double rankToDistribute = rank / ((double) neighbors.length);

      for (Long neighbor: neighbors) {
        out.collect(new Tuple2<Long, Double>(neighbor, rankToDistribute));
      }
    }
  }

  @ForwardedFields("0")
  public static final class Dampener implements MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> {

    private final double dampening;
    private final double randomJump;

    public Dampener(double dampening, double numVertices) {
      this.dampening = dampening;
      this.randomJump = (1 - dampening) / numVertices;
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
      value.f1 = (value.f1 * dampening) + randomJump;
      return value;
    }
  }

  public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

    @Override
    public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
      return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
    }
  }

  private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool params) {
    if (params.has("pages")) {
      return env.readCsvFile(params.get("pages"))
        .fieldDelimiter(" ")
        .lineDelimiter("\n")
        .types(Long.class)
        .map(new MapFunction<Tuple1<Long>, Long>() {
          @Override
          public Long map(Tuple1<Long> v) {
            return v.f0;
          }
        });
    } else {
      System.out.println("Use --pages to specify file input.");
      System.exit(0); 
    }
    return null;
  }

  private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool params) {
    if (params.has("links")) {
      return env.readCsvFile(params.get("links"))
        .fieldDelimiter(" ")
        .lineDelimiter("\n")
        .types(Long.class, Long.class);
    } else {
      System.out.println("Use --links to specify file input.");
      System.exit(0); 
    }
    return null;
	}
}
