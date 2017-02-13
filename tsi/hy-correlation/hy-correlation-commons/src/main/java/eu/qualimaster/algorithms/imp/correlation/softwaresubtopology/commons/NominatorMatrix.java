package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons;

import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 10/21/14.
 */
public class NominatorMatrix implements java.io.Serializable {

  Map<Pair<String, String>, Double> nominators;  // The estimator's nominator for each pair
  // of streams

  public NominatorMatrix() {
    nominators = new HashMap<Pair<String, String>, Double>();
  }

  public void addNominator(String streamId0, String streamId1, double value) {
    String s0;
    String s1;

    if (streamId0.compareTo(streamId1) > 0) {
      s0 = streamId0;
      s1 = streamId1;
    } else {
      s0 = streamId1;
      s1 = streamId0;
    }

    nominators.put(new Pair<String, String>(s0, s1), value);
  }

  public double getNominator(String streamId0, String streamId1) {
    String s0;
    String s1;

    if (streamId0.compareTo(streamId1) > 0) {
      s0 = streamId0;
      s1 = streamId1;
    } else {
      s0 = streamId1;
      s1 = streamId0;
    }

    Pair<String, String> key = new Pair<String, String>(s0, s1);
    if (nominators.containsKey(key)) {
      return nominators.get(key);
    }

    return 0.0;
  }
}
