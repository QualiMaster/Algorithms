package eu.qualimaster.dynamicgraph.core;

/**
 * Created by Nikolaos Pavlakis on 1/16/16.
 */
public class RandomWalk {

  private int nodeId;
  private int numOfWalks; // If negative, it is a negative walk.
  private int edgeNotToTraverse; // Use this field to keep track of the edge that was added after
  // the negative random walk was initiated.

  public RandomWalk(int id, int count, int badEdge) {
    nodeId = id;
    numOfWalks = count;
    edgeNotToTraverse = badEdge;
  }

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public int getNumOfWalks() {
    return numOfWalks;
  }

  public void setNumOfWalks(int numOfWalks) {
    this.numOfWalks = numOfWalks;
  }

  public int getEdgeNotToTraverse() {
    return edgeNotToTraverse;
  }
}
