package eu.qualimaster.dynamicgraph.core;

import cern.jet.random.Uniform;
import cern.jet.random.engine.RandomEngine;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.log4j.Logger;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

//import com.javamex.classmexer.MemoryUtil;

/**
 * Created by Nikolaos Pavlakis on 12/23/15.
 */
public class Graph {

  private static final Logger logger = Logger.getLogger(Graph.class);

  private int serverId;

  //  private Int2ObjectOpenHashMap<Node> nodes; // Holds all the nodes of this server.
  private TIntObjectHashMap<Node> nodes; // Holds all the nodes of this server.
  private RandomEngine generator;
  private Uniform uniform;

  private ObjectArrayList<RandomWalk> walksForOtherServers; // Holds walks that need to be sent
  // to other servers while the random walks on this server are being performed. The key is the id
  // of the node and the value is the number of walks. A negative value means negative random walks
  // This way we do not need to create instances of custom objects.

  private ObjectArrayList<FipWalk> fipWalksForOtherServer; // Same as above for fip walks

  private HashMap<Integer, Integer> visitsChanged;
  // Holds nodes whose visits changed (and the new count) during this execution

  private int totalServers;

  public Graph(int serverId, int totalTasks) {
//    nodes = new Int2ObjectOpenHashMap<Node>();
    nodes = new TIntObjectHashMap<Node>();
    visitsChanged = new HashMap<Integer, Integer>();
    this.serverId = serverId;
    totalServers = totalTasks;
//    int randomSeed = 43298476;
//    generator = new cern.jet.random.engine.MersenneTwister64(randomSeed);
    generator = new cern.jet.random.engine.MersenneTwister64(new Date());
    uniform = new Uniform(generator);
    walksForOtherServers = new ObjectArrayList();
    fipWalksForOtherServer = new ObjectArrayList<FipWalk>();
  }

  public void processNewEdgeFirst(int U, int V) {
    if (!nodes.containsKey(U)) {
      Node nodeU = new Node(U);
      nodeU.getNeighbors().put(V, 0);
      if (Consts.DEBUG) {
        logger.info("Putting " + U + " as first.");
      }
      nodes.put(U, nodeU);
      randomWalk(U, Consts.R, -1);
      fipWalk(U, U, 1);
    } else {
      if (Consts.DEBUG) {
        logger.info("Updating " + U + "->" + V);
      }
      updateWalksAddition(U, V);
      updateFipWalksAddition(U, V);
    }
  }

  public void processNewEdgeSecond(int V) {
    if (!nodes.containsKey(V)) {
      if (Consts.DEBUG) {
        logger.info("Putting " + V + " as second.");
      }
      nodes.put(V, new Node(V));
      randomWalk(V, Consts.R, -1);
      fipWalk(V, V, 1);
    }
  }

  public void removeEdge(int U, int V) {
    updateWalksDeletion(U, V);
    updateFipWalksDeletion(U, V);
  }

  /**
   * Starts numOfWalks randowm walks from node with key = nodeId. Recursively calls itself until all
   * random walks for the nodes of this server are resolved and constructs a collection containing
   * all random walks to be sent to other servers.
   *
   * @param nodeId            The id of the node where the walks start
   * @param numOfWalks        The number of walks to perform. If negative, it means negative random
   *                          walk
   * @param edgeNotToTraverse Which edge not to traverse during the RW (for negative - it was added
   *                          after the walk started).
   */
  public void randomWalk(int nodeId, int numOfWalks, int edgeNotToTraverse) {
    Node nodeU = nodes.get(nodeId);
    if (nodeU == null) {
      logger.error("Node " + nodeId + " not found in server " + serverId + ".");
    }
    nodeU.setVisits(nodeU.getVisits() + numOfWalks); // numOfWalks is negative for neg walks
    visitsChanged.put(nodeId, nodeU.getVisits());

    if (Consts.DEBUG) {
      logger.info(nodeId + " visits changed to " + nodeU.getVisits() + " from rw " + numOfWalks);
    }
    // If the node has neighbors. Else, all random walks end/pause here.
    if (!nodeU.getNeighbors().isEmpty()) {
      int walksToBePropagated;
      walksToBePropagated = getPropagatedWalks(numOfWalks);
      if (walksToBePropagated == 0) {
        return;
      }
      if (numOfWalks < 0) { // Means they are negative walks
        walksToBePropagated *= -1; // Flip the sign to negative
      }
      walksToBePropagated = adjustPendingNegativeWalks(nodeU, walksToBePropagated);
      if (walksToBePropagated == 0) {
        return;
      }
      // Only one neihbor. Add all walks to it. If walksToBePropagated is positive the value of the
      // neihbor will increase, else decrease, but it is the same operation.
      if (nodeU.getNeighbors().size() == 1) {
        int val = 0;
        int key = 0;
//        for (int k : nodeU.getNeighbors().keySet()) {
//          key = k;
//          val = nodeU.getNeighbors().get(k);
//        }
        TIntIntIterator it = nodeU.getNeighbors().iterator();
        while (it.hasNext()) {
          it.advance();
          key = it.key();
          val = it.value();
        }
        nodeU.getNeighbors().put(key, val + walksToBePropagated);
        if (nodes.containsKey(key)) {
          randomWalk(key, walksToBePropagated, edgeNotToTraverse);
        } else {
          walksForOtherServers.add(new RandomWalk(key, walksToBePropagated, edgeNotToTraverse));
        }
      } else {
        if (walksToBePropagated < 0) { // Perform negative walks
          performNegativeWalks(nodeU, -walksToBePropagated, edgeNotToTraverse);
        } else if (walksToBePropagated > 0) { // Perform positive walks
          performPositiveWalks(nodeU, walksToBePropagated);
        }
      }
    }
  }

  private void performNegativeWalks(Node nodeU, int walksToBePropagated, int edgeNotToTraverse) {
    if (walksToBePropagated < 0) {
      logger.error("walksToBePropagated must be a positive number.");
    }
    // Create a temporary arraylist and add each neighbor to it as many times as the positive walks
    // that passed through this neighbor
    IntArrayList idList = new IntArrayList();
//    for (int key : nodeU.getNeighbors().keySet()) {
//      if (key != edgeNotToTraverse) {
//        for (int i = 0; i < nodeU.getNeighbors().get(key); i++) {
//          idList.add(key);
//        }
//      }
//    }
    TIntIntIterator it = nodeU.getNeighbors().iterator();
    while (it.hasNext()) {
      it.advance();
      if (it.key() != edgeNotToTraverse) {
        for (int i = 0; i < nodeU.getNeighbors().get(it.key()); i++) {
          idList.add(it.key());
        }
      }
    }

    // Now randomly draw from the list
    Int2IntOpenHashMap walksForThisServer = new Int2IntOpenHashMap();
    walksForThisServer.defaultReturnValue(0);
    Int2IntOpenHashMap walksForOtherServersMap = new Int2IntOpenHashMap();
    walksForThisServer.defaultReturnValue(0);
    int idx;
    int neighborId;
    for (int i = 0; i < walksToBePropagated; i++) {
      if (idList.size() == 0) {
        break;
      }
      idx = uniform.nextIntFromTo(0, idList.size() - 1);
      neighborId = idList.removeInt(idx);
//      nodeU.getNeighbors().addTo(neighborId, -1);
      nodeU.getNeighbors().adjustOrPutValue(neighborId, -1, -1);
      if (nodes.containsKey(neighborId)) {
        // Same server contains this neighbor. Aggregate in order to continue the walks
        walksForThisServer.addTo(neighborId, -1);
      } else {
        // Other server. Save for later.
        walksForOtherServersMap.addTo(neighborId, -1);
      }
    }
    // Put all walks for other servers in the list
    RandomWalk tempRW;
    for (int key : walksForOtherServersMap.keySet()) {
      tempRW = new RandomWalk(key, walksForOtherServersMap.get(key), edgeNotToTraverse);
      if (Consts.DEBUG) {
        logger.info(
            walksForOtherServersMap.get(key) + " walks from " + nodeU.getId() + " towards " + key);
      }
      walksForOtherServers.add(tempRW);
    }

    // Continue the walks for all neighbors contained in this server
    for (int key : walksForThisServer.keySet()) {
      if (Consts.DEBUG) {
        logger.info(
            walksForThisServer.get(key) + " walks from " + nodeU.getId() + " towards " + key);
      }
      randomWalk(key, walksForThisServer.get(key), edgeNotToTraverse);
    }
  }

  private void performPositiveWalks(Node nodeU, int walksToBePropagated) {
    if (walksToBePropagated < 0) {
      logger.error("walksToBePropagated must be a positive number.");
    }

    int[] neigh = nodeU.getNeighbors().keySet().toArray();
    int neihborIndex;
    Int2IntOpenHashMap walksForThisServer = new Int2IntOpenHashMap(0);
    walksForThisServer.defaultReturnValue(0);
    Int2IntOpenHashMap walksForOtherServersMap = new Int2IntOpenHashMap(0);
    walksForThisServer.defaultReturnValue(0);
    for (int i = 0; i < walksToBePropagated; i++) {
      neihborIndex = uniform.nextIntFromTo(0, neigh.length - 1);
//      nodeU.getNeighbors().addTo(neigh[neihborIndex], 1);
      nodeU.getNeighbors().adjustOrPutValue(neigh[neihborIndex], 1, 1);
      if (nodes.containsKey(neigh[neihborIndex])) {
        // Same server contains this neighbor. Aggregate in order to continue the walks
        walksForThisServer.addTo(neigh[neihborIndex], 1);
      } else {
        // Other server. Save for later.
        walksForOtherServersMap.addTo(neigh[neihborIndex], 1);
      }
    }
    // Put all walks for other server in the list
    RandomWalk tempRW;
    for (int key : walksForOtherServersMap.keySet()) {
      tempRW = new RandomWalk(key, walksForOtherServersMap.get(key), -1);
      if (Consts.DEBUG) {
        logger.info(
            walksForOtherServersMap.get(key) + " walks from " + nodeU.getId() + " towards " + key);
      }
      walksForOtherServers.add(tempRW);
    }

    // Continue the walks for all neighbors contained in this server
    for (int key : walksForThisServer.keySet()) {
      if (Consts.DEBUG) {
        logger.info(
            walksForThisServer.get(key) + " walks from " + nodeU.getId() + " towards " + key);
      }
      randomWalk(key, walksForThisServer.get(key), -1);
    }
  }

  /**
   * Adjusts the pending negative random walks in the event that we want to perform more negative
   * random walks than the sum of positive random walks that we have performed so far.
   *
   * @param nodeU               The node at hand
   * @param walksToBePropagated The walks to be propagated. Positive for regular walks and negative
   *                            for negative walks.
   * @return The actual number of walks to be executed based on the pending negative ones. Positive
   * for regular walks and negative for negative walks.
   */
  private int adjustPendingNegativeWalks(Node nodeU, int walksToBePropagated) {
    int pendingNegativeWalks = nodeU.getPendingNegativeWalks();
    if (walksToBePropagated < 0) { // Means they are negative walks
      int positiveWalksSoFar = nodeU.getSumOfNeighbors();
      // If the negative to be done are more than the positive so far, we only do as many as the
      // positive and add the rest to the pending negative.
      if (Math.abs(walksToBePropagated) > positiveWalksSoFar) {
        nodeU.setPendingNegativeWalks(
            pendingNegativeWalks + Math.abs(walksToBePropagated) - positiveWalksSoFar);
        return -positiveWalksSoFar; // Return a negative number to denote negative walks
      } else {
        return walksToBePropagated; // Return a negative number to denote negative walks
      }
    } else { // Positive walks
      if (pendingNegativeWalks != 0) {
        // If the positive to be done are more than the pending negative, we only do as many as the
        // positive to be done minus the pending negative.
        if (walksToBePropagated >= pendingNegativeWalks) {
          nodeU.setPendingNegativeWalks(0);
          return walksToBePropagated - pendingNegativeWalks;
        } else {
          nodeU.setPendingNegativeWalks(pendingNegativeWalks - walksToBePropagated);
          return 0;
        }
      } else { // Pending negative is 0. Perform all the positive as planned
        return walksToBePropagated;
      }
    }
  }

  // TODO Check if the number of walks to be propagated can be generated without a for.
  private int getPropagatedWalks(int numOfWalks) {
    int ctr = 0;
    for (int i = 0; i < Math.abs(numOfWalks); i++) {
      if (generator.raw() > Consts.EPS) {
        ctr++;
      }
    }
    return ctr;
  }

  /**
   * Perfoms fip random walks (normal and negative). Recursively calls itself if this server
   * contains the next node or prepares fipWalkForOtherServer in order to forward the message
   *
   * @param nodeId          The id of the node at hand
   * @param walkId          The id of the random walk. If negative, then it is a negative walk, else
   *                        normal
   * @param currentPosition The position of this node in the walk
   */
  public void fipWalk(int nodeId, int walkId, int currentPosition) {
    Node nodeU = nodes.get(nodeId);
    if (walkId < 0) { // Negative walk
      walkId *= -1; // Flip the sign
//      nodeU.setVisits(nodeU.getVisits() - 1);
      if (Consts.DEBUG) {
//        logger.info(nodeId + " visits changed to " + nodeU.getVisits() + " from fip -1");
        logger.info("I am " + nodeId + " and I got fip walk with walkId " + (-walkId) + " at pos "
                    + currentPosition);
      }
      TreeMap<Integer, Integer> mapForThisWalk;
      if (nodeU.getFip_map().containsKey(walkId)) {
        mapForThisWalk = nodeU.getFip_map().get(walkId);
        if (mapForThisWalk.containsKey(currentPosition)) {
          int nextNode = mapForThisWalk.get(currentPosition);
          mapForThisWalk.remove(currentPosition);
          if (mapForThisWalk.isEmpty()) {
            nodeU.getFip_map().remove(walkId);
          }
          if (nextNode != -1) {
            performFipWalkCall(nextNode, -walkId, currentPosition + 1);
          }
        }
      }
    } else { // Normal walk
//      nodeU.setVisits(nodeU.getVisits() + 1);
      if (Consts.DEBUG) {
//        logger.info(nodeId + " visits changed to " + nodeU.getVisits() + " from fip +1");
      }
      TreeMap<Integer, Integer> mapForThisWalk;
      if (!nodeU.getFip_map().containsKey(walkId)) { // New random walk
        mapForThisWalk = new TreeMap<Integer, Integer>();
        nodeU.getFip_map().put(walkId, mapForThisWalk);
      } else {
        mapForThisWalk = nodeU.getFip_map().get(walkId);
      }
      int nextNode = getNextNode(nodeU);
      mapForThisWalk.put(currentPosition, nextNode);
//      nodeU.getFip_map().put(walkId, mapForThisWalk);
      if (nextNode != -1) {
        performFipWalkCall(nextNode, walkId, currentPosition + 1);
      }
    }
  }

  private void performFipWalkCall(int key, int walkId, int pos) {
    if (nodes.containsKey(key)) {
      fipWalk(key, walkId, pos);
    } else {
      FipWalk newWalk = new FipWalk(key, walkId, pos);
      fipWalksForOtherServer.add(newWalk);
    }
  }

  /**
   * Gets the next node for fip walk
   *
   * @param nodeU current node
   * @return the id of the next node
   */
  private int getNextNode(Node nodeU) {
    int nextNode = -1;
    if (nodeU.getNeighbors().size() > 0 && generator.raw() > Consts.EPS) {
      int idx = uniform.nextIntFromTo(0, nodeU.getNeighbors().size() - 1);
      int counter = 0;
//      for (int key : nodeU.getNeighbors().keySet()) {
//        if (idx == counter) {
//          nextNode = key;
//          break;
//        }
//        counter++;
//      }
      TIntIntIterator it = nodeU.getNeighbors().iterator();
      while (it.hasNext()) {
        it.advance();
        if (idx == counter) {
          nextNode = it.key();
          break;
        }
        counter++;
      }
    }
    return nextNode;
  }

  /**
   * Called when there is a new edge between two nodes that already exist in the graph. Updates the
   * walks in order to preserve correct visit count
   *
   * @param U The left node of the edge
   * @param V The right node of the edge
   */
  private void updateWalksAddition(int U, int V) {
    Node nodeU = nodes.get(U);
    if (nodeU.getNeighbors().isEmpty()) {
      // Resume all the random walks that stopped at U.
      nodeU.getNeighbors().put(V, 0); // Link addition
      int walksToPropagate = 0;
      for (int i = 0; i < nodeU.getVisits(); i++) {
        if (generator.raw() > Consts.EPS) {
          walksToPropagate++;
        }
      }
      randomWalk(U, walksToPropagate, -1);
    } else {
      int outDegreeU = nodeU.getNeighbors().size();
      int walksToReSend = adjustPendingNegativeWalks(nodeU, getWalksToResend(nodeU, outDegreeU));
//      randomWalk(false, U, walksToReSend, V);
      performNegativeWalks(nodeU, walksToReSend, V); // walksToReSend is already positive
      nodeU.getNeighbors().put(V, walksToReSend); // Link addition.
      // walksToResend RW will pass through V
      if (nodes.containsKey(V)) {
        randomWalk(V, walksToReSend, -1);
      } else {
        walksForOtherServers.add(new RandomWalk(V, walksToReSend, -1));
      }
    }
  }

  /**
   * Called when there is a deletion of an edge that already exists in the graph. Updates the walks
   * in order to preserve correct visit count
   *
   * @param U The left node of the edge
   * @param V The right node of the edge
   */
  private void updateWalksDeletion(int U, int V) {
    Node nodeU = nodes.get(U);
    if (nodeU.getNeighbors().size() == 1) { // V is the only neighbor of U
      int walksToUndo = nodeU.getNeighbors().get(V); // How many positive walks passed over V
      nodeU.setVisits(nodeU.getVisits() - walksToUndo);
      if (nodes.containsKey(V)) {
        randomWalk(V, -walksToUndo, -1);
      } else {
        walksForOtherServers.add(new RandomWalk(V, -walksToUndo, -1));
      }
    } else {
      int outDegreeU = nodeU.getNeighbors().size() - 1; // See paper "Deletion of a link"
      int walksToReSend = adjustPendingNegativeWalks(nodeU, getWalksToResend(nodeU, outDegreeU));
      // Pass through V
      if (nodes.containsKey(V)) {
        randomWalk(V, -walksToReSend, -1);
      } else {
        walksForOtherServers.add(new RandomWalk(V, -walksToReSend, -1));
      }
      performPositiveWalks(nodeU, walksToReSend); // perform the same amount of positive steps
    }
  }

  private int getWalksToResend(Node nodeU, int outDegreeU) {

    int walksToResend = 0;
    int fipWalks = 0;
    int distinct;
    double prob;

    distinct = nodeU.getFip_map().size();
//    for (int key : nodeU.getFip_map().keySet()) {
//      fipWalks += nodeU.getFip_map().get(key).size();
//    }
    TIntObjectIterator<TreeMap<Integer, Integer>> it = nodeU.getFip_map().iterator();
    while (it.hasNext()) {
      it.advance();
      fipWalks += it.value().size();
    }
    double ret = 1 - ((double) distinct / (double) fipWalks);
    prob = ((1 - ret)) / (outDegreeU * (1 - ret) + 1);

    int positiveWalksSoFar = nodeU.getSumOfNeighbors();
    for (int i = 0; i < positiveWalksSoFar; i++) {
      if (generator.raw() < prob) {
        walksToResend++;
      }
    }
    return walksToResend;
  }

  /**
   * Called when there is a new edge between two nodes that already exist in the graph. Updates the
   * fip walks in order to accommodate the new edge. Link addition is performed in
   * updateWalksAddition so the link already exists when called
   *
   * @param U The left node of the edge
   * @param V The right node of the edge
   */
  private void updateFipWalksAddition(int U, int V) {
    Node nodeU = nodes.get(U);
    if (nodeU.getNeighbors().size() == 1) {
      TIntObjectIterator<TreeMap<Integer, Integer>> it = nodeU.getFip_map().iterator();
      while (it.hasNext()) {
        it.advance();
        // For all entries of this fip walk, see if the walk stops here
        int lastKey = it.value().lastEntry().getKey();
        int lastValue = it.value().lastEntry().getValue();
        if (lastValue == -1) {
          if (generator.raw() > Consts.EPS) { // Resume the walk
            it.value().put(lastKey, V);
            performFipWalkCall(V, it.key(), lastKey + 1);
          }
        }
      }
    } else {
      int outDegreeU = nodeU.getNeighbors().size();
      double prob = 1 / (double) outDegreeU; // Probability to re-route an existing walk
      TIntObjectIterator<TreeMap<Integer, Integer>> walkIterator = nodeU.getFip_map().iterator();
      while (walkIterator.hasNext()) {
        walkIterator.advance();
        // For all entries of this fip walk, see if the walk needs to be rerouted
        for (Map.Entry<Integer, Integer> singleWalk : walkIterator.value().entrySet()) {
          // The walk stopped due to probabilities, not due to non-existing neighbors, so no resume
          if (singleWalk.getValue() == -1) {
            continue;
          }
          if (generator.raw() <= prob) {
            // Negative walk from this pos towards previous nextNode
            if (Consts.DEBUG) {
              logger.info("Fip walk with walkId " + -walkIterator.key() + " from " + U + " towards "
                          + singleWalk.getValue() + " at pos " + (singleWalk.getKey() + 1));
            }
            performFipWalkCall(singleWalk.getValue(), -walkIterator.key(), singleWalk.getKey() + 1);

            // Continue from V
            walkIterator.value().put(singleWalk.getKey(), V);

            // Remove all entries greater than the current position to replace the rest of the walk
            walkIterator.value().tailMap(singleWalk.getKey() + 1).clear();

            // Normal walk from this pos towards V
            if (Consts.DEBUG) {
              logger.info(
                  "Fip walk with walkId " + walkIterator.key() + " from " + U + " towards " + V
                  + " at pos " + (singleWalk.getKey() + 1));
            }
            performFipWalkCall(V, walkIterator.key(), singleWalk.getKey() + 1);
            break; // Break since we replaced the rest
          }
        }
      }
    }
  }

  /**
   * Called when there is a deletion of an edge that already exists in the graph. Updates the fip
   * walks in order to accommodate the deletion. Actual link deletion is performed in
   * updateWalksDeletion.
   *
   * @param U The left node of the edge
   * @param V The right node of the edge
   */
  private void updateFipWalksDeletion(int U, int V) {
    Node nodeU = nodes.get(U);
    TIntObjectIterator<TreeMap<Integer, Integer>> walkIterator = nodeU.getFip_map().iterator();
    while (walkIterator.hasNext()) {
      walkIterator.advance();
      // For all entries of this fip walk, see if the walk goes towards V
      for (Map.Entry<Integer, Integer> singleWalk : walkIterator.value().entrySet()) {
        if (singleWalk.getValue() == V) {

          // Negative walk from this pos towards V
          performFipWalkCall(singleWalk.getValue(), -V, singleWalk.getKey() + 1);

          if (nodeU.getNeighbors().size() == 0) { // V was the only neighbor
            // Stop it here since U has no more neighbors
            singleWalk.setValue(-1);
          } else {
            // Choose another neighbor to forward it.
            int neighbor = getNextNode(nodeU);
            singleWalk.setValue(neighbor);
            if (neighbor != -1) {
              performFipWalkCall(singleWalk.getValue(), neighbor, singleWalk.getKey() + 1);
            }
          }

          // Remove all entries greater than the current position to replace the rest of the walk
          walkIterator.value().tailMap(singleWalk.getKey() + 1).clear();
          break; // Break since we replaced the rest
        }
      }
    }
  }

  private boolean meantForThisServer(int nodeId) {
    if (Helper.nextServerId(nodeId, totalServers) == serverId) {
      return true;
    }
    return false;
  }

  public boolean containsNode(int nodeId) {
    return nodes.containsKey(nodeId);
  }

  public ObjectArrayList<RandomWalk> getWalksForOtherServers() {
    return walksForOtherServers;
  }

  public void clearWalksForOtherServers() {
    walksForOtherServers.clear();
  }

  public void printWalksForOtherServers() {
    System.out.print("Size : " + walksForOtherServers.size() + ", values : [ ");
    for (int i = 0; i < walksForOtherServers.size(); i++) {
      RandomWalk tempRW = walksForOtherServers.get(i);
      System.out.print("(" + tempRW.getNodeId() + "->" + tempRW.getNumOfWalks() + ") ");
    }
    System.out.println("]");
  }

  public ObjectArrayList<FipWalk> getFipWalksForOtherServer() {
    return fipWalksForOtherServer;
  }

  public void clearFipWalksForOtherServers() {
    fipWalksForOtherServer.clear();
  }

  public void clearVisitsChanged() {
    visitsChanged.clear();
  }

  public HashMap<Integer, Integer> getVisitsChanged() {
    return visitsChanged;
  }
}
