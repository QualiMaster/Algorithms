package eu.qualimaster.dynamicgraph.core;

/**
 * Created by Nikolaos Pavlakis on 1/16/16.
 */
public class FipWalk {

  private int nodeId;
  private int walkId; // If negative, it is a negative walk.
  private int currentPosition;

  public FipWalk(int id, int walkId, int pos) {
    nodeId = id;
    this.walkId = walkId;
    currentPosition = pos;
  }

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public int getWalkId() {
    return walkId;
  }

  public void setWalkId(int walkId) {
    this.walkId = walkId;
  }

  public int getCurrentPosition() {
    return currentPosition;
  }

  public void setCurrentPosition(int currentPosition) {
    this.currentPosition = currentPosition;
  }

  public void printWalk() {
    System.out.println(
        "Fip walk with walkId = " + walkId + " towards node " + nodeId + " at position "
        + currentPosition);
  }
}
