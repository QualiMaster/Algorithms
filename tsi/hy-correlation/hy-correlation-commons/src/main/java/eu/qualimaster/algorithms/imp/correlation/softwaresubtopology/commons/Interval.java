package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons;

/**
 * Created by Apostolos Nydriotis on 10/20/14.
 */
public class Interval {

  long beginning;  // Interval's beginning timestamp
  long end;  // Interval's end timestamp
  double delta;  // The difference of the value at this interval
  int index;

  public Interval(StreamTuple beginningTuple, StreamTuple endTuple, int index) {
    beginning = beginningTuple.getTimestamp();
    end = endTuple.getTimestamp();
    delta = endTuple.getValue() - beginningTuple.getValue();
    this.index = index;
  }

  public long getBeginning() {
    return beginning;
  }

  public long getEnd() {
    return end;
  }

  public void setDelta(double delta) {
    this.delta = delta;
  }

  public double getDelta() {
    return delta;
  }

  public int getIndex() {
    return index;
  }
}
