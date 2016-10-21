package eu.qualimaster.algorithms.imp.correlation.softwaresubtopology.commons;

import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 10/20/14.
 */
public class OverlapsMatrix {

  //     Map< <streamId, streamId>, Map< <intervalIndex, intervalIndex>, doOverlap>
  private Map<Pair<String, String>, Map<Pair<Integer, Integer>, Boolean>> overlaps;

  public OverlapsMatrix() {
    overlaps = new HashMap<Pair<String, String>, Map<Pair<Integer, Integer>, Boolean>>();
  }

  public void addOverlap(String streamId0, String streamId1, int intervalIndex0, int intervalIndex1,
                         boolean doOverlap) {
    String s0;
    String s1;
    int i0;
    int i1;

    if (streamId0.compareTo(streamId1) > 0) {
      s0 = streamId0;
      i0 = intervalIndex0;
      s1 = streamId1;
      i1 = intervalIndex1;
    } else {
      s0 = streamId1;
      i0 = intervalIndex1;
      s1 = streamId0;
      i1 = intervalIndex0;
    }
    addOrderedKeyOverlap(s0, s1, i0, i1, doOverlap);
  }

  // We order the stream ids before accessing the overlaps map (for adding, removing or querying)
  // because if stream intervals (a, b, aX, bZ) overlap then the (b, a, bZ, aX) overlap as well.
  // We must store this info only once.

  public void removeOverlap(String streamId0, String streamId1, int intervalIndex0,
                            int intervalIndex1) {
    String s0;
    String s1;
    int i0;
    int i1;

    if (streamId0.compareTo(streamId1) > 0) {
      s0 = streamId0;
      i0 = intervalIndex0;
      s1 = streamId1;
      i1 = intervalIndex1;
    } else {
      s0 = streamId1;
      i0 = intervalIndex1;
      s1 = streamId0;
      i1 = intervalIndex0;
    }

    removeOrderedKeyOverlap(s0, s1, i0, i1);
  }

  public boolean doOverlap(String streamId0, String streamId1, int intervalIndex0,
                           int intervalIndex1) {
    String s0;
    String s1;
    int i0;
    int i1;

    if (streamId0.compareTo(streamId1) > 0) {
      s0 = streamId0;
      i0 = intervalIndex0;
      s1 = streamId1;
      i1 = intervalIndex1;
    } else {
      s0 = streamId1;
      i0 = intervalIndex1;
      s1 = streamId0;
      i1 = intervalIndex0;
    }

    return doOrderedKeyOverlap(s0, s1, i0, i1);
  }

  private void addOrderedKeyOverlap(String streamId0, String streamId1, int intervalIndex0,
                                    int intervalIndex1, boolean doOverlap) {
    Pair<String, String> streamsKey = new Pair<String, String>(streamId0, streamId1);
    Map<Pair<Integer, Integer>, Boolean> v = overlaps.get(streamsKey);
    if (v == null) {
      v = new HashMap<Pair<Integer, Integer>, Boolean>();
      overlaps.put(streamsKey, v);
    }
    Pair<Integer, Integer> indexKey = new Pair<Integer, Integer>(intervalIndex0, intervalIndex1);
    v.put(indexKey, doOverlap);
  }

  private void removeOrderedKeyOverlap(String streamId0, String streamId1, int intervalIndex0,
                                       int intervalIndex1) {
    Pair<String, String> streamsKey = new Pair<String, String>(streamId0, streamId1);
    Map<Pair<Integer, Integer>, Boolean> v = overlaps.get(streamsKey);

    if (v != null) {
      Pair<Integer, Integer> indexKey = new Pair<Integer, Integer>(intervalIndex0, intervalIndex1);
      v.remove(indexKey);

      if (v.size() == 0) {
        overlaps.remove(streamsKey);
      }
    }
  }

  private boolean doOrderedKeyOverlap(String streamId0, String streamId1, int intervalIndex0,
                                      int intervalIndex1) {
    Pair<String, String> streamsKey = new Pair<String, String>(streamId0, streamId1);
    Map<Pair<Integer, Integer>, Boolean> v = overlaps.get(streamsKey);
    if (v != null) {
      Pair<Integer, Integer> indexKey = new Pair<Integer, Integer>(intervalIndex0, intervalIndex1);
      if (v.containsKey(indexKey)) {
        return v.get(indexKey);
      }
    }

    return false;
  }
}
