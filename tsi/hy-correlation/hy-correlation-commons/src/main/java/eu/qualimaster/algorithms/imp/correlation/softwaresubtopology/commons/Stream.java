package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Apostolos Nydriotis on 10/9/14.
 */
public class Stream implements java.io.Serializable {

  String id;
  List<Interval> intervals;

  double deltaSquaredSum;  // Sum((S_i - S_{i-1})^2)
  StreamTuple lastTuple;
  int intervalIndex;

  public Stream(String id) {
    this.id = id;
    intervals = new ArrayList<Interval>();
    deltaSquaredSum = 0.0;
    lastTuple = null;
    intervalIndex = 0;
  }

  public String getId() {
    return id;
  }

  public List<Interval> getIntervals() {
    return intervals;
  }

  public int getIntervalsCount() {
    return intervals.size();
  }

  public Interval getInterval(int index) {
    return intervals.get(index);
  }

  public double getDeltaSquaredSum() {
    return deltaSquaredSum;
  }

  public boolean addTuple(StreamTuple tuple) {

    if (lastTuple != null) {

      if (lastTuple.getTimestamp() == tuple.getTimestamp()) {
        return false;
      }

      Interval newInterval = new Interval(lastTuple, tuple, intervalIndex++);
      intervals.add(newInterval);
      if (intervalIndex == Integer.MAX_VALUE) {
        intervalIndex = 0;
      }
      deltaSquaredSum += Math.pow(newInterval.getDelta(), 2.0);
    }

    lastTuple = tuple;
    return true;
  }

  public void removeDeltaContribution(double delta) {
    deltaSquaredSum -= Math.pow(delta, 2.0);
  }

  public void removeInterval(Iterator<Interval> toRemove) {
    toRemove.remove();
    if (intervals.size() == 0) {  // This is to avoid precision errors (when no interval is in the
      // window, the delta sq sum should be 0 but sometimes is something
      // really small (e.g. 1e-27
      deltaSquaredSum = 0.0;
    }
  }
}
