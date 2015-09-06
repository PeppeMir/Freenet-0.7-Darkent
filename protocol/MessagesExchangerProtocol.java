package protocol;

import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import control.LocationKeysManager;
import control.LocationKeysManager.Coin;
import peersim.cdsim.CDState;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.transport.UniformRandomTransport;
import structure.FPeer;
import structure.HashMapEntry;
import structure.Message;
import structure.Message.Type;

/**
 *  Class that implements the Hybrid Cycle-Driven & Event-Driven protocol and so manages the FPeer's periodic requests and 
 *  messages exchanging with the others FPeers of the overlay network.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   March 9, 2015  
 **/

public class MessagesExchangerProtocol implements peersim.edsim.EDProtocol, peersim.cdsim.CDProtocol
{
	// unique suffix for statistics file used during the current simulation
	private static long statisticsFileExtension = System.currentTimeMillis();		

	// flag that indicates if the general statistics of the simulations are already written or not
	private static boolean simStatToWrite = true;

	// protocol identifier of the Hybrid-Protocol itself
	private int itselfPID;		

	// protocol identifier of the used Transport protocol
	private int transportPID;			

	// protocol identifier of the used Linkable protocol
	private int linkablePID;

	// maximum HTL value used to bound the Greedy Depth-First Routing
	private int maxHTL;		

	// maximum HTL value used to bound the SWAP requests
	private int maxHTLswap;

	// maximum number of neighbors toward expand a successfully PUT (key replication)
	private int replicationFactor;

	// interval, in minutes, based on which the protocol decides that an HashMap's entry is to mark as "useless"
	private long uselessFactor;

	// frequency, in units of time, based on which the protocol removes from HashMap the "useless" messages-entries
	private int cleanupFrequency;

	// frequency, in number of cycles, that FPeers tries to performs a SWAP operation
	private int swapFrequency;	

	// factor used to model the probability to send a GET request rather a PUT request, during the "nextCycle" execution
	private double biasFactor;			

	// flag that specify if the prints are allowed or not, during the simulation messages exchange
	private boolean printsAllowed;							

	// HashMap that stores triples (ID, locKey, [locKey1,...]) if the running FPeer have received a message with identifier = "ID" 
	// from the FPeer with location key "locKey" and have sent a message with identifier = "ID" to the FPeers having location keys
	// in "[locKey1,...]"
	private HashMap<Long,HashMapEntry> SRmessages;	


	/**
	 * Constructor method. Sets up internal fields from the PeerSim configuration file, using {@code prefix}.
	 * @param prefix  the prefix, in the configuration file, representing the class.
	 **/
	public MessagesExchangerProtocol(String prefix)
	{
		this.itselfPID = Configuration.getPid(prefix + ".itself_pid");
		this.transportPID = Configuration.getPid(prefix + ".transport_pid");
		this.linkablePID = Configuration.getPid(prefix + ".linkable_pid");
		this.maxHTL = Configuration.getInt(prefix + ".maxHTL");
		this.maxHTLswap = Configuration.getInt(prefix + ".maxHTLswap");
		this.replicationFactor = Configuration.getInt(prefix + ".replicationFactor");
		this.swapFrequency = Configuration.getInt(prefix + ".swapFrequency");
		this.uselessFactor = Configuration.getLong(prefix + ".uselessFactor");
		this.cleanupFrequency = Configuration.getInt(prefix + ".cleanupFrequency");
		this.biasFactor = Configuration.getDouble(prefix + ".coinBiasing");
		this.printsAllowed = Configuration.getBoolean(prefix + ".allowPrints");			
		this.SRmessages = new HashMap<Long, HashMapEntry>();
	}


	/**
	 * Creates an identical (clone) Hybrid protocol.
	 * @return cloned_prot the cloned Hybrid protocol.
	 **/
	@Override
	public Object clone()
	{
		MessagesExchangerProtocol cloned_prot = null;

		try
		{
			// invoke Object.clone()
			cloned_prot = (MessagesExchangerProtocol) super.clone();
		} 
		catch (final CloneNotSupportedException e)
		{
			System.out.println("Error during ED protocol cloning...\nError details: " + e.getMessage());
			return null;
		}

		// set up the fields of the cloned protocol (behave as constructor method)
		cloned_prot.itselfPID = this.itselfPID;
		cloned_prot.transportPID = this.transportPID;
		cloned_prot.linkablePID = this.linkablePID;
		cloned_prot.maxHTL = this.maxHTL;
		cloned_prot.maxHTLswap = this.maxHTLswap;
		cloned_prot.replicationFactor = this.replicationFactor;
		cloned_prot.swapFrequency = this.swapFrequency;
		cloned_prot.uselessFactor = this.uselessFactor;
		cloned_prot.cleanupFrequency = this.cleanupFrequency;
		cloned_prot.biasFactor = this.biasFactor;
		cloned_prot.printsAllowed = this.printsAllowed;
		cloned_prot.SRmessages = new HashMap<Long, HashMapEntry>();

		return cloned_prot;
	}


	/**
	 * Cleanup routine for deallocation of useless entries in the protocol's HashMap. 
	 * All the HashMap's entries that are not used for more than {@code inactivityToUseless} minutes are removed from the
	 * HashMap, saving memory.
	 **/
	private void cleanHashMap()
	{
		// gets the current time in milliseconds
		long currentTime = new Date().getTime();

		// allocates an HashSet to store the HashMap's keys relative to HashMap's entries to remove
		HashSet<Long> toRemoveIDs = new HashSet<Long>();

		// for each entry of the HashMap
		for (Entry<Long, HashMapEntry> entry : this.SRmessages.entrySet())
		{
			// if the current entry is not used for more than "inactivityToUseless" minutes, mark it as "useless"
			final long diffInMinutes = Math.abs(currentTime - entry.getValue().getLastModTimetamp()) / 60000;
			if (diffInMinutes > this.uselessFactor)
				toRemoveIDs.add(entry.getKey());
		}

		// remove all the useless HashMap's entries
		for (Long key : toRemoveIDs)
		{
			this.SRmessages.remove(key);
		}
	}


	/**
	 * Writes statistics of the passed message {@code mex} on the statistics file of the simulation.
	 * Statistics consists in the pairs (messageType, messageTHC).
	 * @param mex  the message on which writes statistics
	 **/
	private void writeStatisticsOnFile(Message mex) 
	{
		PrintWriter statFile = null;
		try 
		{
			// try to open the statistics file and to write statistics on it
			statFile = new PrintWriter(new BufferedWriter(new FileWriter("../statistics/sim_stat_" + statisticsFileExtension + ".stat", true)));

			// only the first time, write generics simulation statistics
			if (simStatToWrite)
			{
				statFile.println("---------------------------------------------------");
				statFile.println("Simulation DateTime: \t " + (new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")).format(new Date()));
				statFile.println("Overlay Size: \t " + Network.size());
				statFile.println("Overlay Log-Size: \t " +  (Math.log(Network.size()) / Math.log(2)));
				statFile.println("Simulation max-HTL: \t " + this.maxHTL);
				statFile.println("Simulation maxSwap-HTL: \t " + this.maxHTLswap);
				statFile.println("Simulation Key-Replication Factor: \t " + this.replicationFactor);
				statFile.println("---------------------------------------------------\n");
				simStatToWrite = false;
			}

			// write statistics for the current message
			statFile.println(mex.getMessageType() + "\t" + mex.getTHC());
		}
		catch (IOException e) 
		{
			System.out.println("Error during statistics file opening/writing:\n" + e.getMessage());
		}
		finally
		{
			// close the file
			if (statFile != null)
				statFile.close();
		}
	}


	/**
	 * Verifies if the passed location key {@code locKey1} is closest, in term of circular distance, than the other 
	 * passed location key {@code locKey2} w.r.t. the passed {@code contentLocKey}.
	 * @param locKey1		the first location key to compare
	 * @param locKey2		the second location key to compare
	 * @param contentLocKey	the content location key
	 * @return {@code true} if the passed {@code locKey1} is closest w.r.t. {@code contentLocKey} 
	 * 		   than {@code locKey2}. {@code false} otherwise.
	 **/
	private boolean isLessWrtContent(double locKey1, double locKey2, double contentLocKey)
	{
		final double dist1 = Math.abs(locKey1 - contentLocKey);
		final double dist2 = Math.abs(locKey2 - contentLocKey);

		return (Math.min(dist1, 1 - dist1) < Math.min(dist2, 1 - dist2));
	}


	/**
	 * Tries to find a candidate FPeer, scanning {@code neighborsList} from the beginning, that satisfies the following
	 * two tests: <br><br>
	 * 
	 * test 1) it is not already present in the {@code sentTo} field of the passed HashMap {@code entry}; <br>
	 * test 2) it is different from the passed FPeer {@code toAvoid}.
	 * 
	 * @param neighborsList	the list of neighbors to scan
	 * @param entry			the HashMap entry to check
	 * @param toAvoid		the FPeer to avoid
	 * @return a neighbors list's index in [0, size) if a candidate is found, >= size if it is not found.
	 **/
	private int findBestIndex(ArrayList<FPeer> neighborsList, HashMapEntry entry, FPeer toAvoid)
	{
		int index = 0;
		for (; index < neighborsList.size(); index++)
		{
			// get index-th neighbor as candidate
			final FPeer fpeer_cand = neighborsList.get(index);

			// check equality between the selected candidate and the FPeer to avoid
			final boolean areEquals = (fpeer_cand == toAvoid);

			// if the entry is null, only the test 2) is needed (no sent previously)
			if (entry == null) 
			{
				if (areEquals)
					continue;
				else
					break;
			}

			// otherwise, perform test 1) + test 2)
			if (!entry.alreadySentTo(fpeer_cand) && !areEquals)
				break;
		}

		return index;
	}


	/**
	 * Utility method performing the following operations: <br>
	 * 1) changes the type of the passed message {@code mex} exploiting the passed {@code mexType}; <br>
	 * 2) changes the last-hop FPeer of the passed message {@code mex} exploiting the passed FPeer {@code fpeer_sender}; <br>
	 * 3) sends the passed message {@code mex} from the passed FPeer {@code fpeer_sender} to the passed FPeer 
	 *    {@code fpeer_receiver}. <br>
	 * 
	 * @param mex				the message to modify and to send
	 * @param mexType			the type to set as message type
	 * @param fpeer_sender		the reference to the FPeer to use as last-hop field and from which send the message
	 * @param fpeer_receiver	the reference to the FPeer to which send the message
	 */
	private void changeAndSendMessage(Message mex, Type mexType, FPeer fpeer_sender, FPeer fpeer_receiver) 
	{
		// change the message type and the last hop FPeer to the running FPeer exploiting passed parameters
		mex.changeMessageType(mexType);
		mex.changeLastHopFPeer(fpeer_sender);

		// send the message from "fpeer_sender" to "fpeer_receiver"
		this.sendMessage(fpeer_sender, fpeer_receiver, mex);
	}


	/**
	 * Stores the content location key of the passed message {@code m} into the local storage of the running FPeer {@code fpeer},
	 * avoiding the store if the content location key is already present in the storage.
	 * Furthermore, stores the information triple (messageID, lastHopFPeer, null) into the running FPeer's HashMap, if the
	 * information does not already exists.
	 * 
	 * @param m			the message from which extract the informations
	 * @param fpeer		the FPeer in which store the message content location key
	 * @param mEntry	the HashMap entry relative to the message
	 **/
	private void storeContentAndInfo(Message m, FPeer fpeer, HashMapEntry mEntry)
	{
		// store the content location key in running FPeer's storage and ends the routing for the message
		fpeer.addContentLocationKey(m.getMessageLocationKey());

		if (mEntry == null)
		{
			final long messageID  = m.getMessageID();
			this.SRmessages.put(messageID, new HashMapEntry(m.getLastHopFPeer()));
			mEntry = this.SRmessages.get(messageID);
		}
	}

	
	/**
	 * Sends the message {@code msg} from the FPeer {@code sender} to Hybrid protocol of the FPeer {@code receiver}.
	 * Furthermore, if the message is a GET or a PUT request, the method increases its THC value.
	 * @param sender  	the node that send the message
	 * @param receiver 	the node to which send the message
	 * @param msg 		the message to send
	 **/
	private void sendMessage(Node sender, Node receiver, Message msg)
	{
		// get the transport protocol of the sender node
		UniformRandomTransport urt = (UniformRandomTransport) sender.getProtocol(this.transportPID);

		// increase message THC (True Hop Counter) for GET and PUT requests (statistics only)
		final Type mexType = msg.getMessageType();
		if (mexType == Type.GET || mexType == Type.PUT)
			msg.increaseTHC();

		// send the message msg to the protocol having eventdrivenPID of the FPeer receiver
		urt.send(sender, receiver, msg, this.itselfPID);

		if (printsAllowed)
			System.out.println("FPeer " + ((FPeer)sender).toString() + ": sent message " + msg.toString() + " ...");
	}
	
	
	/**
	 * Replicates the passed content location key {@code contentLocKey} sending at most {@code replicationFactor} PUT_REPLICATION
	 * messages from the running FPeer {@code fpeer} toward its top-neighbors, having all the same message identifier and
	 * the HTL value set to the {@code maxHTLvalue}.
	 * 
	 * @param fpeer			the running FPeer that will perform the replication process
	 * @param contentLocKey	the content location key to replicate in the overlay network
	 */
	private void replicatesTowardNeighbors(FPeer fpeer, double contentLocKey) 
	{
		// get top-"replicationFactor" (maximum, if exists) neighbors of the running FPeer
		LinkableProtocol lp = (LinkableProtocol) fpeer.getProtocol(linkablePID);
		ArrayList<FPeer> topToReplicate = lp.retrieveTopKNeighbors(contentLocKey, this.replicationFactor);

		// replicates the stored content location key on top-"replicationFactor" neighbors				
		Message replMex = null;
		HashMapEntry entry = null;
		for (int k = 0; k < topToReplicate.size(); k++)
		{
			if (k > 0)
			{
				// creates an exact copy of the first message
				replMex = (Message) replMex.clone();
			}
			else
			{
				// creates the first replication message
				replMex = new Message(Type.PUT_REPLICATION, contentLocKey, this.maxHTL);
				replMex.changeLastHopFPeer(fpeer);
				replMex.decreaseHTL();
				replMex.changePathClosestLocKey(fpeer.getLocationKey());

				// add HashMap entry for the replication message
				final long replMessageID = replMex.getMessageID();
				this.SRmessages.put(replMessageID, new HashMapEntry(fpeer));
				entry = this.SRmessages.get(replMessageID);
			}

			// get the k-th running FPeer's top-neighbor
			FPeer fpeer_k = topToReplicate.get(k);

			// send the replication message to it
			this.sendMessage(fpeer, fpeer_k, replMex);

			// add information "sent to" in the own HashMap
			entry.addSent(fpeer_k);
		}

	}

	
	/**
	 * Utility method that handles the forwarding of the passed message {@code m}, performing the following operations: <br>
	 * 
	 * 1) decreases the {@code m}'s HTL field value; <br>
	 * 2) changes the {@code m}'s last hop FPeer field with the passed {@code fpeer_sender}; <br>
	 * 3) forwards the message {@code m} from {@code fpeer_sender} to the FPeer {@code fpeer_receiver}; <br>
	 * 4) adds "received from {@code fpeer_recFrom}" and "sent to {@code fpeer_receiver}" informations into the 
	 * 	  running FPeer's HashMap.
	 * 
	 * @param m					the message to forward
	 * @param mEntry			the HashMap entry relative to the message to send (possibly {@code null})
	 * @param fpeer_sender		the FPeer from which forward the message
	 * @param fpeer_receveiver	the FPeer toward which forward the message
	 * @param fpeer_recFrom		the FPeer from which the running FPeer has received the message to forward
	 **/
	private void handleMessageForwarding(Message m, HashMapEntry mEntry, FPeer fpeer_sender, FPeer fpeer_receveiver, FPeer fpeer_recFrom)
	{
		// decrease the message HTL value
		m.decreaseHTL();

		// change the last hop FPeer to the running FPeer
		m.changeLastHopFPeer(fpeer_sender);

		// propagate the message to the FPeer "fpeer_receiver"
		this.sendMessage(fpeer_sender, fpeer_receveiver, m);

		// if is the first time that propagates the received message
		if (mEntry == null)
		{
			// get message identifier
			final long messageID = m.getMessageID();

			// add an entry relative to the messageID into the running FPeer's HashMap
			this.SRmessages.put(messageID, new HashMapEntry(fpeer_recFrom));
			mEntry = this.SRmessages.get(messageID);
		}

		// add information "message propagated to fpeer_receiver FPeer" into the running FPeer's HashMap
		mEntry.addSent(fpeer_receveiver);
	}


	/**
	 * Handles the {@code fpeer}'s receiving of the backward message {@code mex} (e.g. PUT_OK, PUT_COLLISION, GET_FOUND),
	 * performing the following operations: <br>
	 * 
	 * 1) it checks its own HashMap to find the FPeer from which have received the relative FORWARD message (e.g. PUT, GET); <br>
	 * 2) if that FPeer is ITSELF, this means that it is the owner of the FORWARD message, so do nothing and routing ends; 
	 *    otherwise, it propagates the relative BACKWARD message (e.g. PUT_OK, PUT_COLLISION, GET_FOUND)  message toward that neighbor.
	 * 
	 * @param fpeer  the FPeer that have received the backward message
	 * @param mex    the received backward message
	 **/
	private void handleBackwardMessage(FPeer fpeer, Message mex)
	{
		// query the own HashMap using the message ID to get the entry of the FORWARD message relative to the received BACKWARD message
		HashMapEntry fwd_entry = this.SRmessages.get(mex.getMessageID());
		final FPeer backw_fpeer = fwd_entry.getReceivedFrom();

		// if, in the HashMap entry, it found that the FORWARD message correspondent to the now receive BACKWARD message is sent  
		// by itself, then the running FPeer is the owner of the backward message, so routing ends with the received message
		if (backw_fpeer == fpeer)
		{
			if (printsAllowed)
				System.out.println("FPeer " + fpeer + ": routing for contentLocKey = " + mex.getMessageLocationKey() + " ends with " + mex.getMessageType() + " ...");

			// write statistics on a file
			writeStatisticsOnFile(mex);
		}
		else
		{
			// ... otherwise, the forward message is received by a running FPeer's neighbor

			// change the last hop location key to the location key of the running FPeer
			mex.changeLastHopFPeer(fpeer);

			// propagate backward the message toward this neighbor
			this.sendMessage(fpeer, backw_fpeer, mex);
		}
	}

	
	/**
	 * Handles the {@code fpeer}'s receiving of the GET/GET_NOTFOUND message {@code mex}, performing the following operations: <br><br>
	 * 
	 * First of all, it checks if its location key is the closest, w.r.t. the content location key, than all the location
	 * keys encountered up to now during the {@code mex}'s routing. If that key is the closest, then it changes the {@code mex}'s
	 * relative field with that key (see "CHECK 0"). <br><br>
	 * 
	 * After, if {@code fpeer} contains the content location key in its own storage, it sends backward a GET_FOUND message to the 
	 * neighbor from which it has received the GET/GET_NOTFOUND message {@code mex} (see "CHECK 1"); <br><br>
	 *   
	 * Otherwise, if {@fpeer} has already received the same (check on unique ID) GET message {@code mex}, it send backward
	 * a GET_NOTFOUND message to the neighbor from which it has received the GET message {@code mex} (see "CHECK 2"); <br><br>
	 * 
	 * Otherwise, {@code mex} is not a duplicated GET and {@code fpeer} does NOT contains the requested content location key.
	 * At this point, if {@code fpeer} is the closest in the routing path, it resets HTL to the maximum value (see "CHECK 3"). 
	 * After that, {@code fpeer} checks the HTL value: <br><br>
	 * 
	 * 	- if HTL = 0, then {@code fpeer} backwards a GET_NOTFOUND message to the neighbor from which it has received 
	 * 	  the GET message {@code mex} (obviously, if it is not the owner); (see "CHECK 4") <br>
	 * 
	 * 	- if HTL > 0, then {@code fpeer} searches for an its candidate neighbor having the following characteristics: <br><br>
	 *   
	 *		1) it is the {@code fpeer}'s neighbor closest w.r.t. the content location key; <br>
	 *		2) it is not already visited during the current Bounded-Depth-First Search;  <br>
	 *		3) it is different from the FPeer from which {@code fpeer} has received the GET message. <br><br>
	 *
	 * 	If such neighbor does NOT exists, {@code fpeer} backwards a GET_NOTFOUND message to the neighbor from which it has 
	 * 	received the GET message {@code mex}; <br><br>
	 *     	  
	 * 	If such neighbor exists, then {@code fpeer} forwards the received GET message {@code mex} to this candidate neighbor, 
	 * 	decreasing its HTL value.
	 * 
	 * @param fpeer  the FPeer that have received the message (e.g. GET, GET_NOTFOUND)
	 * @param mex	 the received message (e.g. GET, GET_NOTFOUND)
	 **/
	private void handleReceivedGET(FPeer fpeer, Message mex)
	{
		final double contentLocKey = mex.getMessageLocationKey();
		final double fpeer_locKey = fpeer.getLocationKey();

		// decide if the received message is of type GET or GET_NOTFOUND
		final boolean isGETmsg = (mex.getMessageType() == Type.GET);		

		// get the running FPeer's neighbor from which it has received the current GET/GET_NOTFOUND message
		final FPeer fpeer_receivedFrom = mex.getLastHopFPeer();

		// CHECK 0): if the running FPeer's location key is closest (w.r.t. content location key) than all
		// FPeer's location keys encountered during the routing of the current processed message, it changes  
		// the message's "closest w.r.t. content location key" information with that location key 
		final boolean isClosestInPath = ((fpeer_locKey == mex.getPathClosestLocKey()) || isLessWrtContent(fpeer_locKey, mex.getPathClosestLocKey(), contentLocKey));
		if (isClosestInPath)
			mex.changePathClosestLocKey(fpeer_locKey);

		// CHECK 1): if the running FPeer contains the requested location key, it sends backward a GET_FOUND message
		if (fpeer.containsContentLocationKey(contentLocKey))
		{	
			if (isGETmsg)
				changeAndSendMessage(mex, Type.GET_FOUND, fpeer, fpeer_receivedFrom);
			else
			{
				mex.changeMessageType(Type.GET_FOUND);
				handleBackwardMessage(fpeer, mex);
			}

			return;
		}

		// get the entry relative to the received message from the running FPeer's HashMap
		HashMapEntry messageIDEntry = this.SRmessages.get(mex.getMessageID());

		// CHECK 2): if is NOT the first time that the running FPeer receive this GET message, then a CYCLE is
		// detected, so it sends backward a GET_NOTFOUND message
		if (isGETmsg && messageIDEntry != null)
		{
			changeAndSendMessage(mex, Type.GET_NOTFOUND, fpeer, fpeer_receivedFrom);
			return;
		}

		// ... otherwise, the message is not a duplicated GET and running FPeer does NOT contains the requested location key

		// CHECK 3): if the running FPeer's content location key is the closest w.r.t. content location key encountered 
		// during the message routing, it resets message's HTL value to the maximum
		if (isClosestInPath)
			mex.resetHTLTo(this.maxHTL);


		// CHECK 4): if, also after the reset check, the HTL is still 0, backward the relative GET_NOTFOUND message to  
		// the neighbor from which it has received the relative GET message (obviously, only if it is not the owner, 
		// when processing a received GET_NOTFOUND)
		if (mex.getHTL() == 0)
		{ 
			if (isGETmsg)
				changeAndSendMessage(mex, Type.GET_NOTFOUND, fpeer, fpeer_receivedFrom);
			else
				handleBackwardMessage(fpeer, mex);

			return;
		}

		// ... otherwise, HTL > 0 ...

		// get the running FPeer's neighbors ranked by "closest w.r.t. the content location key"
		final LinkableProtocol fpeer_lp = (LinkableProtocol) fpeer.getProtocol(linkablePID);
		ArrayList<FPeer> topNeighbors = fpeer_lp.retrieveTopKNeighbors(contentLocKey, fpeer_lp.degree());

		// search for a running FPeer's candidate neighbor to forward the GET message, with the following characteristics:
		// 1) it is the closest w.r.t. the content location key, in the neighbors set (running FPeer not considered)
		// 2) it is different from all the already visited neighbors 
		// 3) it is different from the FPeer from which the running FPeer has received the associate GET message
		final int index = findBestIndex(topNeighbors, messageIDEntry, fpeer_receivedFrom);

		// if there are no valid neighbors available, backward the relative GET_NOTFOUND message to the neighbor from which it has 
		// received the relative GET message (obviously, only if it is not the owner, when processing a received GET_NOTFOUND)
		if (index >= topNeighbors.size())
		{
			if (isGETmsg)
				changeAndSendMessage(mex, Type.GET_NOTFOUND, fpeer, fpeer_receivedFrom);
			else
				handleBackwardMessage(fpeer, mex);
		}
		else
		{
			// ... otherwise, selects the candidate FPeer neighbor at found index, forwards the GET message to the    
			// candidate FPeer and adds informations relative to the message into the running FPeer's HashMap

			FPeer fpeer_cand = topNeighbors.get(index);

			if (!isGETmsg)
				mex.changeMessageType(Type.GET);

			handleMessageForwarding(mex, messageIDEntry, fpeer, fpeer_cand, fpeer_receivedFrom);
		}
	}

	
	/**
	 * Handles the {@code fpeer}'s receiving of the PUT message {@code mex}, performing the following operations: <br><br>
	 * 
	 * - if {@code fpeer} contains the content location key in its own storage, it send backward a PUT_COLLISION message to the 
	 *   neighbor from which it has received the PUT message {@code mex}; <br>
	 * 
	 * - otherwise, {@code fpeer} try to select a neighbor that is closest w.r.t. the content location key to PUT than its
	 *   location key: <br><br>
	 *   
	 *   	=> if this neighbor exists, {@code fpeer} forward the PUT request toward it; <br>
	 *   	=> if this neighbor does not exists, {@code fpeer} stores the content location key in its own storage and forwards
	 *   	   a PUT_REPLICATION message toward the {@code replicationFactor} closest w.r.t. the content location key neighbors, 
	 *   	   having maximum HTL value and its reference as last hop FPeer.
	 *   
	 * @param fpeer	the FPeer that have received the PUT message
	 * @param mex	the received PUT message
	 **/
	private void handleReceivedPUT(FPeer fpeer, Message mex) 
	{
		// get message's content location key
		final double contentLocKey = mex.getMessageLocationKey();

		// if the running FPeer has received a PUT request for a content that it already stores, then 
		// it sends a PUT_COLLISION message toward the FPeer from which it has received the PUT
		if (fpeer.containsContentLocationKey(contentLocKey))
		{
			changeAndSendMessage(mex, Type.PUT_COLLISION, fpeer, mex.getLastHopFPeer());
			return;
		}

		// ... otherwise, the running FPeer does not contains the content location key to PUT

		// get the FPeer from which the running FPeer have received the PUT message
		final FPeer fpeer_recFrom = mex.getLastHopFPeer();

		// get the candidate running FPeer's neighbor closest w.r.t. the content location key
		final LinkableProtocol fpeer_lp = (LinkableProtocol) fpeer.getProtocol(linkablePID);
		final FPeer fpeer_cand = fpeer_lp.retrieveTopKNeighbors(contentLocKey, 1).get(0);

		// if the candidate neighbor is closest to the content location key than the running FPeer
		if (isLessWrtContent(fpeer_cand.getLocationKey(), fpeer.getLocationKey(), contentLocKey))
		{	
			// get the entry relative to the received message (possibly NULL)
			HashMapEntry messageIDEntry = this.SRmessages.get(mex.getMessageID());

			// forward the PUT message to the candidate FPeer and add informations relative to the message into the  
			// running FPeer's HashMap
			handleMessageForwarding(mex, messageIDEntry, fpeer, fpeer_cand, fpeer_recFrom);
		}
		else
		{
			// ... otherwise, the running FPeer's location key is closest than the candidate FPeers's location key

			// store the content location key in the running FPeer's keys storage
			fpeer.addContentLocationKey(contentLocKey);

			// change message type, last hop FPeer and send the message toward the FPeer from which have received the PUT message
			changeAndSendMessage(mex, Type.PUT_OK, fpeer, fpeer_recFrom);

			// replicates the content location key toward the "replicationFactor" top-neighbors of the running FPeer
			replicatesTowardNeighbors(fpeer, contentLocKey);
		}
	}

	
	/**
	 * Handles the {@code fpeer}'s receiving of the PUT_REPLICATION/PUT_REPL_COLLISION message {@code mex}, performing the 
	 * following operations: <br><br>
	 * 
	 * First of all, it checks if its location key is the closest, w.r.t. the content location key, than all the location
	 * keys encountered up to now during the {@code mex}'s routing. If that key is the closest, then it changes the 
	 * {@code mex}'s relative field with that key (see "CHECK 0"). <br><br>
	 *   
	 * After, if {@code fpeer} has already received the same (check on unique ID) PUT_REPLICATION message {@code mex}, it 
	 * sends backward a PUT_REPL_COLLISION message to the neighbor from which it has received the PUT_REPLICATION message 
	 * {@code mex} (see "CHECK 1"); <br><br>
	 * 
	 * Otherwise, if {@code fpeer} is the closest in the routing path, it resets HTL to the maximum value (see "CHECK 2"). 
	 * After that, {@code fpeer} checks the HTL value: <br><br>
	 * 
	 * 	- if HTL = 0, then {@code fpeer} stores the content location key to replicate in its own storage (see "CHECK 3") <br>
	 * 
	 * 	- if HTL > 0, then {@code fpeer} searches for an its candidate neighbor having the following characteristics: <br><br>
	 *   
	 *		1) it is the {@code fpeer}'s neighbor closest w.r.t. the content location key; <br>
	 *		2) it is not already visited during the current Bounded-Depth-First Search;  <br>
	 *		3) it is different from the FPeer from which {@code fpeer} has received the GET message. <br><br>
	 *
	 * 	If such neighbor does NOT exists, {@code fpeer} stores the content location key to replicate in its own storage ; <br><br>
	 *     	  
	 * 	If such neighbor exists and it is closest w.r.t. the content location key than {@code fpeer}, then it forwards the 
	 *  PUT_REPLICATION message to this candidate neighbor, decreasing the message's HTL value.
	 *  Otherwise, it stores the content location key to replicate in its own storage.
	 * 
	 * @param fpeer	the FPeer that have received the message (e.g. PUT_REPLICATION, PUT_REPL_COLLISION)
	 * @param mex	the received message (e.g. PUT_REPLICATION, PUT_REPL_COLLISION)
	 **/
	private void handleReceivedPUT_REPLICATION(FPeer fpeer, Message mex) 
	{
		// get message's content location key and FPeer's location key
		final double contentLocKey = mex.getMessageLocationKey();
		final double fpeer_locKey = fpeer.getLocationKey();

		// decide if the received message is of type PUT_REPLICATION or PUT_REPL_COLLISION
		final boolean isPUT_REPL = (mex.getMessageType() == Type.PUT_REPLICATION);

		// CHECK 0): if the running FPeer's location key is closest (w.r.t. content location key) than all
		// FPeer's location keys encountered during the routing of the current processed message, it changes  
		// the message's "closest w.r.t. content location key" information with that location key 
		final boolean isClosestInPath = ((fpeer_locKey == mex.getPathClosestLocKey()) || isLessWrtContent(fpeer_locKey, mex.getPathClosestLocKey(), contentLocKey));
		if (isClosestInPath)
			mex.changePathClosestLocKey(fpeer_locKey);

		// get the HashMap's entry relative to the current processed message's ID
		HashMapEntry messageIDEntry = this.SRmessages.get(mex.getMessageID());

		// get the running FPeer's neighbor from which it has received the current processed message
		final FPeer fpeer_receivedFrom = mex.getLastHopFPeer();

		// CHECK 1): if already exists informations about the current processed message, this means that running FPeer  
		// have already received the same PUT_REPLICATION message, so it sends backward a PUT_REPL_COLLISION message
		if (isPUT_REPL && messageIDEntry != null)
		{
			changeAndSendMessage(mex, Type.PUT_REPL_COLLISION, fpeer, fpeer_receivedFrom);
			return;
		}

		// CHECK 2): if the running FPeer's content location key is the closest w.r.t. content location key  
		// encountered during the message routing, it resets message's HTL value to the maximum
		if (isClosestInPath)
			mex.resetHTLTo(this.maxHTL);


		// CHECK 3): if the message have HTL = 0, stores the content location key in running FPeer's storage and ends the routing for 
		// the message; furthermore, in the case of PUT_REPLICATION, stores also informations for cycle-avoidance in its own HashMap
		if (mex.getHTL() == 0)
		{
			if (isPUT_REPL)
				storeContentAndInfo(mex, fpeer, messageIDEntry);
			else
				fpeer.addContentLocationKey(contentLocKey);

			if (printsAllowed)
				System.out.println("FPeer " + fpeer + ": contentLocKey=" + contentLocKey + " replication stored ...");

			return;
		}


		// get the running FPeer's neighbors ranked by "closest w.r.t. the content location key"
		final LinkableProtocol fpeer_lp = (LinkableProtocol) fpeer.getProtocol(linkablePID);
		ArrayList<FPeer> topNeighbors = fpeer_lp.retrieveTopKNeighbors(contentLocKey, fpeer_lp.degree());

		// search for a running FPeer's candidate neighbor to forward the PUT_REPLICATION message, with the following characteristics:
		// 1) it is the closest w.r.t. the content location key, in the neighborhood (running FPeer not considered)
		// 2) it is different from all the already visited neighbors  
		// 3) it is different from the FPeer from which the running FPeer has received the current processed message
		final int index = findBestIndex(topNeighbors, messageIDEntry, fpeer_receivedFrom);

		// if there are no valid neighbors available, stores the content location key in running FPeer's storage and ends the routing 
		// for the message; furthermore, in the case of PUT_REPLICATION, stores also informations for cycle-avoidance in its own HashMap
		if (index >= topNeighbors.size())
		{		
			if (isPUT_REPL)
				storeContentAndInfo(mex, fpeer, messageIDEntry);
			else
				fpeer.addContentLocationKey(contentLocKey);

			if (printsAllowed)
				System.out.println("FPeer " + fpeer + ": contentLocKey=" + contentLocKey + " replication stored ...");
		}
		else
		{
			// ... otherwise, select the candidate FPeer neighbor at found index
			FPeer fpeer_cand = topNeighbors.get(index);

			// if the candidate neighbor is closest to the content location key than the running FPeer, forwards a PUT_REPLICATION 
			// message to the candidate FPeer and stores informations for cycle-avoidance in its own HashMap
			if (isLessWrtContent(fpeer_cand.getLocationKey(), fpeer_locKey, contentLocKey))
			{
				if (isPUT_REPL)
					handleMessageForwarding(mex, messageIDEntry, fpeer, fpeer_cand, fpeer_receivedFrom);
				else
				{
					mex.decreaseHTL();
					changeAndSendMessage(mex, Type.PUT_REPLICATION, fpeer, fpeer_cand);
					messageIDEntry.addSent(fpeer_cand);
				}
			}
			else
			{
				// ... otherwise, the running FPeer is closest to the content location key than the candidate neighbor, so
				// stores the content location key in running FPeer's storage and ends the routing for the message; 
				// furthermore, in the case of PUT_REPLICATION, stores also informations for cycle-avoidance in its own HashMap

				if (isPUT_REPL)
					storeContentAndInfo(mex, fpeer, messageIDEntry);
				else
					fpeer.addContentLocationKey(contentLocKey);

				if (printsAllowed)
					System.out.println("FPeer " + fpeer + ": contentLocKey=" + contentLocKey + " replication stored ...");
			}
		}
	}

	
	/**
	 * Tries to select a neighbor of the passed FPeer {@code fpeer_proposer} to involves into a SWAP process with itself.
	 * The choice is made taking care that a neighbor is not already involved in a SWAP process with another FPeer and that
	 * the selecting process ends also if no FPeers are available, after some attempts.
	 * @param fpeer_proposer the running FPeer that tries to select an its neighbor
	 * @param lp			 the Linkable protocol of the running FPeer
	 * @return {@code null} if the running FPeer is involved itself in a SWAP process or there are no neighbors available for
	 * 		   the SWAP. {@code fpeer_candidate}, the reference to the selected FPeer, if it is availbale for the SWAP.
	 */
	private FPeer selectNeighborForSwap(FPeer fpeer_proposer, LinkableProtocol lp)
	{
		Random rand = new Random(System.nanoTime());
		boolean selectedSuccessfully = false;
		final int neighborhoodSize = lp.degree();
		FPeer fpeer_candidate = null;

		for (int i = 0; i < neighborhoodSize && !selectedSuccessfully; i++)
		{
			// select as "candidate" a pseudo-random FPeer in the neighborhood
			final int randomIndex = rand.nextInt(neighborhoodSize);
			fpeer_candidate = (FPeer) lp.getNeighbor(randomIndex);

			// if the selected candidate FPeer is already involved in a SWAP process with another FPeer, go next;
			// otherwise, select it as SWAP candidate
			if (fpeer_candidate.isInvolvedInSwap())
				continue;
			else
				selectedSuccessfully = true;
		}

		if (selectedSuccessfully)
			return fpeer_candidate;
		else
			return null;
	}
	
	
	/**
	 * Handles the {@code fpeer}'s receiving of the SWAP message {@code mex} performing the following operations: <br><br>
	 * 
	 * First of all, it checks the HTL value of the message: if HTL > 0, then it select a random neighbor (different from 
	 * this from which it has received the actual request) toward which forward the SWAP request. 
	 * Otherwise, if HTL = 0, it performs the following actions: <br><br>
	 * 
	 * 1) computes the product of the distances between the SWAP request-owner FPeer's location key and the location keys of each one 
	 *    of its neighbors (see "prodAA"); <br>
	 * 
	 * 2) computes the product of the distances between {@code fpeer}'s location key and the location keys of each one of the
	 *    SWAP request-owner FPeer's neighbors (see "prodBA"); <br>
	 * 
	 * 3) computes the product of the distances between the {@code fpeer}'s location key and the location keys of each one of its
	 * 	  neighbors (see "prodBB"); <br>
	 * 
	 * 4) computes the product of the distances between the SWAP request-owner FPeer's location key and the location keys of each one
	 * 	  of {@code fpeer}'s neighbors (see "prodAB"); <br><br>
	 *     
	 * After, it computes the product between the distances before and after (the eventual) swap of the two involved FPeers (see "D1" and "D2"). 
	 * 
	 * The next backward operation depends on the values of "D1" and "D2": <br><br>
	 * 
	 * 		- if D1 >  D2, then {@code fpeer} send a SWAP_OK message to the FPeer owner of the SWAP request (swap request approved); <br>
	 * 		- if D2 <= D1, then {@code fpeer} send a SWAP_OK message with probability D1 / D2 (see "D_AB"), while a 
	 * 		  SWAP_REFUSED otherwise (swap request refused).
	 * 
	 * @param fpeer  the FPeer that have received the SWAP message
	 * @param mex	 the received SWAP message
	 **/
	private void handleReceivedSWAP(FPeer fpeer, Message mex)
	{
		// stores locally some utility fields to make fast further accesses
		final FPeer fpeer_proposer = mex.getLastHopFPeer();
		final double fpeer_locKey = fpeer.getLocationKey();
		final double fpeer_proposer_locKey = fpeer_proposer.getLocationKey();
		final LinkableProtocol fpeer_lp = (LinkableProtocol) fpeer.getProtocol(linkablePID);
		
		// if the HTL of the message is still greater than 0
		if (mex.getHTL() > 0)
		{
			// select a neighbor peer different from which that have sent the SWAP request
			// and forward the request to him, decreasing the HTL value
			FPeer fpeer_cand = selectNeighborForSwap(fpeer, fpeer_lp);
			if (fpeer_cand != null && fpeer_cand != fpeer_proposer)
			{
				mex.decreaseHTL();
				fpeer.setSwapStatus(false);
				fpeer_cand.setSwapStatus(true);
				this.sendMessage(fpeer, fpeer_cand, mex);
				return;
			}
		}

		// ... otherwise, HTL = 0 ...
		
		// get iterators to scan neighbors lists of the running and SWAP request-owner FPeers
		Iterator<FPeer> fpeer_it_A = ((LinkableProtocol) fpeer_proposer.getProtocol(linkablePID)).getNeighborsIterator();
		Iterator<FPeer> fpeer_it_B = fpeer_lp.getNeighborsIterator();

		// allocates variables for temporary distances computing
		double currentLocKey, prodAA, prodBB, prodAB, prodBA, D1, D2, D_AB;
		prodAA = prodBB = prodAB = prodBA = 1.0;

		// scanning the neighbor list of the FPeer owner of the SWAP request
		while (fpeer_it_A.hasNext())
		{
			currentLocKey = fpeer_it_A.next().getLocationKey();

			if (currentLocKey != fpeer_locKey)
			{
				prodAA *= fpeer_proposer.getDistanceFromLocationKey(currentLocKey);
				prodBA *= fpeer.getDistanceFromLocationKey(currentLocKey);
			}
		}

		// scanning the neighbor list of the running FPeer
		while (fpeer_it_B.hasNext())
		{
			currentLocKey = fpeer_it_B.next().getLocationKey();

			if (currentLocKey != fpeer_proposer_locKey)
			{
				prodBB *= fpeer.getDistanceFromLocationKey(currentLocKey);
				prodAB *= fpeer_proposer.getDistanceFromLocationKey(currentLocKey);
			}				
		}

		// compute the ratio between the distances before and after the (eventual) swap
		D1 = (prodAA * prodBB);
		D2 = (prodBA * prodAB);
		D_AB = D1 / D2;

		// accept or refuse the SWAP request based on values of D1, D2 and their ratio
		if (D1 > D2 || (new Random(System.nanoTime())).nextDouble() < D_AB)
			changeAndSendMessage(mex, Type.SWAP_OK, fpeer, fpeer_proposer);
		else
			changeAndSendMessage(mex, Type.SWAP_REFUSED, fpeer, fpeer_proposer);
	}

	
	/**
	 * Handles the {@code fpeer}'s receiving of the answer {@code mex} relative to the SWAP message (e.g. SWAP_OK, SWAP_REFUSED)  
	 * sent in the past, performing the following operations: <br><br>
	 * 
	 * - if the received message is of type SWAP_REFUSED, do nothing; <br>
	 * 
	 * - otherwise, if the received message is of type SWAP_OK, {@code fpeer} swaps its location key and its stored content location 
	 * 	 keys with the location key and the stored content location keys of the FPeer from which it has received the answer. <br><br>
	 * 
	 * In both the cases, it sets the status of the two SWAP-involved FPeers in "not involved into a SWAP process".
	 * 
	 * @param fpeer  the FPeer that have received the answer message
	 * @param mex	 the received answer message (e.g. SWAP_OK, SWAP_REFUSED)
	 **/
	private void handleReceivedSWAPanswer(FPeer fpeer, Message mex)
	{
		// get the FPeer from which have received the answer to the SWAP request
		FPeer fpeer_toSwap = mex.getLastHopFPeer();

		// if the SWAP request sent in the past is approved, performs really the swap
		if (mex.getMessageType() == Type.SWAP_OK)
			fpeer.swapWith(fpeer_toSwap, this.linkablePID);

		// set running FPeer and swapped-neighbor's status in "not involved into a SWAP process"
		fpeer.setSwapStatus(false);
		fpeer_toSwap.setSwapStatus(false);

		if (printsAllowed)
			System.out.println("FPeer " + fpeer + ": swapping with FPeer " + fpeer_toSwap + " ends with " + mex.getMessageType() + " ...");
	}
	

	/**
	 * Handles the receiving of a message {@code message} from an FPeer {@code peer}, using the protocol having {@code pid}.
	 * @param peer 	  the FPeer that have received the message
	 * @param pid     the identifier of the protocol that have sent the message
	 * @param message the received message of type as specified in enum {@code Message.Type}
	 **/
	@Override
	public void processEvent(Node peer, int pid, Object message) 
	{
		// adjust objects to the right types and get running FPeer Linkable protocol to access its neighbors
		FPeer fpeer = (FPeer) peer;		
		Message mex = (Message) message;

		if (printsAllowed)
			System.out.println("FPeer " + fpeer.toString() + ": received message " + mex.toString() + " ...");

		// test the type of the message received by the running FPeer
		switch (mex.getMessageType())
		{

		case GET:
		case GET_NOTFOUND:
		{
			handleReceivedGET(fpeer, mex);
			break;
		}

		case PUT:
		{
			handleReceivedPUT(fpeer, mex);
			break;
		}

		case PUT_REPLICATION:
		case PUT_REPL_COLLISION:
		{
			handleReceivedPUT_REPLICATION(fpeer, mex);
			break;
		}

		case GET_FOUND:
		case PUT_OK:
		case PUT_COLLISION:
		{
			handleBackwardMessage(fpeer, mex);
			break;
		}

		case SWAP:
		{
			handleReceivedSWAP(fpeer, mex);
			break;
		}

		case SWAP_OK:
		case SWAP_REFUSED:
		{
			handleReceivedSWAPanswer(fpeer, mex);
			break;
		}
		}
	}


	/**
	 *  Implements the GET, PUT and SWAP requests operations, based on the passed parameter {@code mexType}: <br><br>
	 *  
	 *  - for {@code mexType} = PUT, {@code fpeer_sender} injects the passed content location key {@code contentLocKey} into the overlay network, 
	 *    sending a PUT message request. The message is sent to the neighbor that have location key closest w.r.t. the content location key 
	 *    {@code contentLocKey} to put. <br>
	 *  
	 *  - for {@code mexType} = GET, {@code fpeer_sender} search the passed content location key {@code contentLocKey} into the overlay network, 
	 *    sending a GET message request. The message is sent to the neighbor that have location key closest w.r.t. the content location key 
	 *    {@code contentLocKey} to get. <br>
	 *    
	 *	- for {@code mexType} = SWAP, {@code fpeer_sender} select randomly an its neighbor and send to it the SWAP message request. <br><br>
	 *     
	 *  In the GET and PUT cases, {@code fpeer_sender} stores in its own HashMap an entry with the following informations: <br><br>
	 *  
	 *  	1) "request received from itself", storing the {@code fpeer_sender}'s reference in the field "receivedFrom"; <br>
	 *  	2) "request sent to selectedNeighbor", storing selected neighbors's reference in the HashSet-field "sentTo"; <br><br>
	 *     
	 *  Only in the case of the PUT, obviously, the request is not sent if {@code fpeer_sender} already contains the content location
	 *  key {@code contentLocKey} or if it is the closest, w.r.t. the content location key in the neighborhood.
	 *  
	 * @param mexType		the type of the request to perform (e.g. GET, PUT or SWAP)    
	 * @param fpeer_sender  the FPeer that performs the request into the overlay
	 * @param contentLocKey the content location key to GET from or PUT into the overlay network (unused for SWAP request)
	 **/
	public void performRequest(Type mexType, FPeer fpeer_sender, double contentLocKey)
	{		
		// get the Linkable protocol of the sender FPeer to access to its neighbors
		final LinkableProtocol lp = (LinkableProtocol) fpeer_sender.getProtocol(linkablePID);

		// select the running FPeer's candidate neighbor to which forward the request
		FPeer fpeer_candidate = null;
		if (mexType == Type.SWAP)
		{
			// if the running FPeer is already involved in a SWAP process, abort the request
			if (fpeer_sender.isInvolvedInSwap())
				return;
					
			// select a candidate to send the SWAP requesr
			fpeer_candidate = selectNeighborForSwap(fpeer_sender, lp);

			// if there are no available candidates, abort the request
			if (fpeer_candidate == null)
				return;
		}
		else
		{
			// ... otherwise, for GET and PUT requests ...

			// if the content location key to is already stored in the running FPeer storage, does not forward away the request
			if (fpeer_sender.containsContentLocationKey(contentLocKey))
			{
				if (printsAllowed)
					System.out.println("FPeer " + fpeer_sender + ": content location key = " + contentLocKey + " already stored in my storage, " + mexType + " request not forwarded ...");

				return;
			}

			// select as "candidate" the neighbor FPeer having location key closest to the content location key to GET/PUT
			fpeer_candidate = lp.retrieveTopKNeighbors(contentLocKey, 1).get(0);

			// for PUT request, if the running FPeer's location key is closest w.r.t. the content location key than the candidate's location key		
			if (mexType == Type.PUT && isLessWrtContent(fpeer_sender.getLocationKey(), fpeer_candidate.getLocationKey(), contentLocKey))
			{
				// stores the content location key in its own storage, without send the PUT request
				fpeer_sender.addContentLocationKey(contentLocKey);

				// replicates the content location key toward its "replicationFactor" top-neighbors
				replicatesTowardNeighbors(fpeer_sender, contentLocKey);

				if (printsAllowed)
					System.out.println("FPeer " + fpeer_sender + ": content location key = " + contentLocKey + " stored in myself storage, , PUT request not forwarded ...");

				return;
			}
		}

		// create the GET/PUT/SWAP message changing the last-hop FPeer to the running FPeer
		Message requestMessage = null;
		if (mexType == Type.SWAP)
			requestMessage = new Message(mexType, contentLocKey, this.maxHTLswap);
		else
			requestMessage = new Message(mexType, contentLocKey, this.maxHTL);

		requestMessage.changeLastHopFPeer(fpeer_sender);
		requestMessage.changePathClosestLocKey(fpeer_sender.getLocationKey());

		// decrease the HTL value of the received message after the last hop
		requestMessage.decreaseHTL();

		// send the request message to the "candidate" neighbor
		this.sendMessage(fpeer_sender, fpeer_candidate, requestMessage);

		if (mexType == Type.SWAP)
		{
			// set the swapping status of the two FPeers as "involved in a SWAP process"
			fpeer_sender.setSwapStatus(true);
			fpeer_candidate.setSwapStatus(true);
		}
		else
		{
			// store informations "request received by myself" and "request sent to candidate fpeer_candidate"
			final long reqMessageID = requestMessage.getMessageID();
			this.SRmessages.put(reqMessageID, new HashMapEntry(fpeer_sender));
			this.SRmessages.get(reqMessageID).addSent(fpeer_candidate);
		}

		if (printsAllowed)
			System.out.println("FPeer " + fpeer_sender + ": request " + requestMessage + " forwarded ...");
	}


	/**
	 * Implements the running FPeer's performing of some requests, based on probability and periods criteria.
	 * @param peer  the overlay network's FPeer associated to the protocol that performs the cycle
	 * @param pid   the protocol identifier of the running protocol
	 **/
	@Override
	public void nextCycle(Node peer, int pid) 
	{
		// get the simulation time
		final long currentTime = CDState.getTime();

		// cast general Node to FPeer
		final FPeer fpeer = (FPeer) peer;

		// if the cleanup period decades, performs periodically HashMap's cleanup
		if ((currentTime % this.cleanupFrequency) == 0)
			cleanHashMap();

		// if the SWAP period decades, try to swap with a random selected neighbor
		if ((currentTime % this.swapFrequency) == 0)
			performRequest(Type.SWAP, fpeer, -1.0);

		// toss a biased coin: if the result is HEAD
		if (LocationKeysManager.tossCoin(this.biasFactor) == Coin.HEAD)
		{
			try
			{
				// perform a GET request
				performRequest(Type.GET, fpeer, LocationKeysManager.getAvailableContentLocationKey());
			}
			catch (UnsupportedOperationException exc) { }
		}
		else
		{
			// perform a PUT request
			performRequest(Type.PUT, fpeer, LocationKeysManager.generateUniform(true));
		}
	}

}
