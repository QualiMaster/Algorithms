package eu.qualimaster.dynamicgraph.core;

/**
 * Created by Nikolaos Pavlakis on 1/16/16.
 */
public final class Consts {

  public static final int
      R = 10; // Number of random walks to be initiated when a new node is discovered.
  public static final double EPS = 0.2; // Stopping probability of a random walk
  public static final boolean DIRECTED = false; // Whether the graph is directed or not

  public static final boolean ACK_ENABLED = true; // Whether to enable acking or not

  public static final boolean DEBUG = false; // For debugging purposes

  public static final boolean LOCAL_MODE = true; // For testing locally
}
