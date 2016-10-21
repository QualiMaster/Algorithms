package eu.qualimaster.dynamicgraph.core;

/**
 * Created by Nikolaos Pavlakis on 1/27/16.
 */
public final class Helper {
  public static int nextServerId(int nodeId, int totalServers) {
    return nodeId % totalServers;
  }
}
