package eu.qualimaster.dynamicgraph.core;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Nikolaos Pavlakis on 12/23/15.
 */
public class Node {

  private int visits;
  private int id;
  private TIntIntHashMap neighbors;
  //  private Int2IntOpenHashMap neighbors;
  // First -> neighbor id, second -> positive random walk count so far for this neighbor
  private int positiveWalksSoFar; // Holds the sum of positive walks so far from this node (sum of
  // values of neigbors). This is an optimization in order not to constantly iterate over the
  // neighbors.

  private int pendingNegativeWalks; // Holds the number of pending walks. E.g. If we want to perform
  // 15 negative walks, but we have only performed 10 positive in the past, we only do 10 negative,
  // and set pendingNegativeWalks to 5;

  private TIntObjectHashMap<TreeMap<Integer, Integer>> fip_map;
//  private Int2ObjectOpenHashMap<TreeMap<Integer, Integer>> fip_map;
  // Holds fip random walks. First -> walkId, Second -> Hashmap that holds <currPos, nextNode>

  public Node(int id) {
    this.id = id;
    visits = 0;
    positiveWalksSoFar = 0;
    pendingNegativeWalks = 0;
    neighbors = new TIntIntHashMap(0);
//    neighbors = new Int2IntOpenHashMap();
//    neighbors.defaultReturnValue(0);
    fip_map = new TIntObjectHashMap<TreeMap<Integer, Integer>>(0);
//    fip_map = new Int2ObjectOpenHashMap<>();
//    fip_map.defaultReturnValue(new TreeMap<Integer, Integer>());
  }

  public int getVisits() {
    return visits;
  }

  public void setVisits(int visits) {
    this.visits = visits;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  //  public Int2IntOpenHashMap getNeighbors() {
//    return neighbors;
//  }
  public TIntIntHashMap getNeighbors() {
    return neighbors;
  }

//  public void setNeighbors(Int2IntOpenHashMap neighbors) {
//    this.neighbors = neighbors;
//  }

  public void setNeighbors(TIntIntHashMap neighbors) {
    this.neighbors = neighbors;
  }

  //  public Int2ObjectOpenHashMap<TreeMap<Integer, Integer>> getFip_map() {
//    return fip_map;
//  }
  public TIntObjectHashMap<TreeMap<Integer, Integer>> getFip_map() {
    return fip_map;
  }

//  public void setFip_map(
//      Int2ObjectOpenHashMap<TreeMap<Integer, Integer>> fip_map) {
//    this.fip_map = fip_map;
//  }

  public void setFip_map(
      TIntObjectHashMap<TreeMap<Integer, Integer>> fip_map) {
    this.fip_map = fip_map;
  }

  public int getPendingNegativeWalks() {
    return pendingNegativeWalks;
  }

  public void setPendingNegativeWalks(int pendingNegativeWalks) {
    this.pendingNegativeWalks = pendingNegativeWalks;
  }

  public int getPositiveWalksSoFar() {
    return positiveWalksSoFar;
  }

  public void setPositiveWalksSoFar(int positiveWalksSoFar) {
    this.positiveWalksSoFar = positiveWalksSoFar;
  }

  public void printNeighbors() {
    System.out.print("Id: " + id + ", Size : " + neighbors.size() + ", values : [ ");
//    for (Map.Entry<Integer, Integer> e : neighbors.entrySet()) {
//      System.out.print("(" + e.getKey() + "->" + e.getValue() + ") ");
//    }
    TIntIntIterator it = neighbors.iterator();
    while (it.hasNext()) {
      it.advance();
      System.out.print("(" + it.key() + "->" + it.value() + ") ");
    }
    System.out.println("]");
  }

  public int getSumOfNeighbors() {
    int sum = 0;
//    for (Map.Entry<Integer, Integer> e : neighbors.entrySet()) {
//      sum += e.getValue();
//    }
    TIntIntIterator it = neighbors.iterator();
    while (it.hasNext()) {
      it.advance();
      sum += it.value();
    }
    return sum;
  }

  private void printHashMap(TreeMap<Integer, Integer> map) {
    System.out.print(" [ ");
    for (Map.Entry<Integer, Integer> e : map.entrySet()) {
      System.out.print("(" + e.getKey() + "->" + e.getValue() + ") ");
    }
    System.out.print("] ");
  }

  public void printFipMap() {
    System.out.print("Id: " + id + ", fip walks : " + fip_map.size() + ", values : [ ");
//    for (Map.Entry<Integer, TreeMap<Integer, Integer>> e : fip_map.entrySet()) {
//      System.out.print("(" + e.getKey() + " ->");
//      printHashMap(e.getValue());
//      System.out.print(") ");
//    }
    TIntObjectIterator<TreeMap<Integer, Integer>> it = fip_map.iterator();
    while (it.hasNext()) {
      it.advance();
      System.out.print("(" + it.key() + " ->");
      printHashMap(it.value());
      System.out.print(") ");
    }
    System.out.println("]");
  }
}
