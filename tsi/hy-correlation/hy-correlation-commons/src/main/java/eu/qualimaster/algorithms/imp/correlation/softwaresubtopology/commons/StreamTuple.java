package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons;

/**
 * Created by Apostolos Nydriotis on 10/7/14.
 */
public class StreamTuple {

  private double value;
  private long timestamp;

  public StreamTuple(double value, long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }

  public void setValue(double value) {
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
