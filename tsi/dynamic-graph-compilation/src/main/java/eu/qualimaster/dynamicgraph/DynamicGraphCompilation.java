package eu.qualimaster.dynamicgraph;

import eu.qualimaster.families.imp.FDynamicGraphCompilation;
import eu.qualimaster.families.inf.IFDynamicGraphCompilation;
import eu.qualimaster.observables.IObservable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by ap0n on 8/2/2016.
 */
public class DynamicGraphCompilation implements IFDynamicGraphCompilation {

  Logger logger = LoggerFactory.getLogger(DynamicGraphCompilation.class);

  private double thresshold;
  private Set<String> edges;

  public DynamicGraphCompilation() {
    this.thresshold = 0.7;
    edges = new HashSet<>();
  }

  public void calculate(
      IIFDynamicGraphCompilationPairwiseFinancialInput input,
      IIFDynamicGraphCompilationEdgeStreamOutput result) {

    String firstId = input.getId0();
    String secondId = input.getId1();
    String[] datetime = input.getDate().split(",");
    String date = datetime[0];
    String time = datetime[1];
    double correlation = input.getValue();

    String edge = makeEdge(firstId, secondId);

    if (Math.abs(correlation) >= thresshold) {
      if (edges.add(edge)) {  // new Edge. Update the graph
        result.clear();
        result.setEdge(edge + "," + date + "," + time + "," + "1");
        logger.info("Emitting NON-null edge");
      } else {
        result.noOutput();
        logger.info("No output");
      }
    } else {
      if (edges.remove(edge)) {  // removed edge. Update the graph
        result.clear();
        result.setEdge(edge + "," + date + "," + time + "," + "0");
        logger.info("Emitting NON-null edge");
      } else {
        result.noOutput();
        logger.info("No output");
      }
    }
  }

  public void setParameterCorrelationThreshold(double value) {
    thresshold = value;
  }

  @Override
  public void switchState(State state) {}

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  private String makeEdge(String v0, String v1) {
    if (v0.compareTo(v1) <= 0) {
      return v0 + "," + v1;
    }
    return v1 + "," + v0;
  }
}
