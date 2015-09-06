package protocol;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import peersim.core.Node;
import structure.FPeer;

/**
 * Implements the Linkable protocol that manages the FPeer's neighborhood view.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   March 9, 2015  
 **/

public class LinkableProtocol implements peersim.core.Linkable, peersim.core.Protocol
{
	// Red-Black Tree that stores references to all the neighbors FPeers, sorted by FPeer's increasing location key
	// It ensures all basic operations time complexity in O(logN)
	private TreeSet<FPeer> neighborsRBTree;		


	/**
	 * Class constructor. Initializes the class fields.
	 * @param prefix the prefix, in the configuration file, of the protocol. 
	 **/
	public LinkableProtocol(String prefix)
	{
		this.neighborsRBTree = new TreeSet<FPeer>();
	}


	/**
	 * Creates an identical (clone) Linkable protocol.
	 * @return the cloned Linkable protocol.
	 **/
	@Override
	public Object clone()
	{
		LinkableProtocol lp = null;

		try
		{
			// invoke the "original" clone method (Object.clone() invoked)
			lp = (LinkableProtocol) super.clone();
		} 
		catch (final CloneNotSupportedException e)
		{
			System.out.println("Error during Linkable protocol cloning..\n" + e.getMessage());
			return null;
		}

		// performs "constructor" actions on the clone
		lp.neighborsRBTree = new TreeSet<FPeer>();

		return lp;
	}


	/**
	 * Checks if the passed FPeer {@code peer} is a neighbor of the running FPeer.
	 * @param  peer the FPeer to compare with the running FPeer
	 * @return {@code true} if {@code peer} is a neighbor of the running FPeer. {@code false} otherwise.
	 **/
	@Override
	public boolean contains(Node peer) 
	{
		return this.neighborsRBTree.contains((FPeer) peer);
	}


	/**
	 * Tries to add the passed FPeer {@code peer} to the neighborhood of the running FPeer.
	 * @param peer   the FPeer to add to the neighborhood of the running FPeer
	 * @return {@code false} if {@code peer} is already a neighbor of the running FPeer or if the insertion fails.
	 * 		   {@code true} otherwise.
	 **/
	@Override
	public boolean addNeighbor(Node peer) 
	{
		return this.neighborsRBTree.add((FPeer) peer);
	}


	/**
	 * Removes the passed FPeer {@code fpeer} from the neighborhood of the running FPeer.
	 * @param fpeer the FPeer to remove from the neighborhood of the running FPeer
	 * @return {@code true} if {@code fpeer} is in the running FPeer's neighborhood and it is removed successfully;
	 * 		   {@code false} otherwise.
	 **/
	public boolean removeNeighbor(FPeer fpeer)
	{
		return this.neighborsRBTree.remove(fpeer);
	}


	/**
	 * Returns the number of running FPeer's neighbors
	 * @return the number of neighbors of the running FPeer.
	 **/
	@Override
	public int degree() 
	{
		return this.neighborsRBTree.size();
	}


	/**
	 * Gets the {@code index}-th neighbor of the running FPeer.
	 * @param  index the index of by which retrieves the {@code index}-th neighbor of the running FPeer
	 * @return the {@code index}-th neighbor of the running FPeer.
	 * @throws IndexOutOfBoundsException if {@code index >= neighbors.size()} 
	 **/
	@Override
	public Node getNeighbor(int index) 
	{
		if (index < 0 || index >= this.degree())
			throw new IndexOutOfBoundsException();

		Iterator<FPeer> it = this.neighborsRBTree.iterator();

		FPeer fpeer = null;
		for (int i = 0; it.hasNext() && i <= index; i++)
			fpeer = it.next();

		return fpeer;
	}


	/**
	 * Returns the FPeer, neighbor of the running FPeer, having location key equal to the passed location key {@code locKey}.
	 * @param locKey  the location key based on which retrieve the neighbor FPeer
	 * @return the FPeer that have {@code locKey} as location key
	 **/
	public FPeer getNeighborByLocationKey(double locKey)
	{
		final FPeer aux_peer = new FPeer(null, null, locKey);
		final NavigableSet<FPeer> tailSet = this.neighborsRBTree.tailSet(aux_peer, true);

		if (tailSet.size() > 0)
			return tailSet.first();
		else
			return this.neighborsRBTree.headSet(aux_peer, true).last();
	}


	/**
	 * Gets out the iterator to iterate on the entire list of neighbors of the running FPeer. 
	 * @return the iterator to iterate on the entire list of neighbors of the running FPeer. 
	 **/
	public Iterator<FPeer> getNeighborsIterator()
	{
		return this.neighborsRBTree.iterator();
	}


	/**
	 * Computes the {@code k} neighbors FPeers of the running FPeer that are closest w.r.t. the passed location key {@code locationKey}.
	 * "Closest" means similar on location key values, so less is the circul distance between the value of two location keys, 
	 * more similar are the two location keys.
	 * @param locationKey  the location key on which perform the similarity check
	 * @param k			   the maximum number of most similar FPeers that the method should returns
	 * @return topK		   the list of the {@code k} most similar FPeer w.r.t the passed {@code locationKey}, in decreasing order
	 * 					   of similarity. 
	 **/
	public ArrayList<FPeer> retrieveTopKNeighbors(double locationKey, int k)
	{
		// BR-Tree "clone" call needed because of side-effect operations 
		// N.B. only the tree structure are cloned, so FPeers inside the RBtree are NOT cloned
		@SuppressWarnings("unchecked")
		TreeSet<FPeer> clonedRBT = (TreeSet<FPeer>) this.neighborsRBTree.clone();	

		// use an auxiliary FPeer for comparisons in the tree
		FPeer aux_fpeer = new FPeer(null, null, locationKey);

		// get the set of all FPeers with location key strictly less than the passed location key
		NavigableSet<FPeer> less_peers =  clonedRBT.headSet(aux_fpeer, false);

		// get the set of all FPeers with location key strictly greater than the passed location key
		NavigableSet<FPeer> greater_peers =  clonedRBT.tailSet(aux_fpeer, false);

		// allocate the array list that will contains the top-k location key-closest FPeers for the passed location key
		ArrayList<FPeer> topK = new ArrayList<FPeer>();

		// fill the top-k array list with FPeers of "less_peers" and "greater_peers" sets
		for (int added = 0; less_peers.size() > 0 && greater_peers.size() > 0 && added < k; added++)
		{
			// compute the distance between the passed location key and the location keys 
			// of the two closest candidates FPeer of the sets "greater_peers" and "less_peers"
			final double dist_less = less_peers.last().getDistanceFromLocationKey(locationKey);
			final double dist_greater = greater_peers.first().getDistanceFromLocationKey(locationKey);

			// add to the top-k array list the candidate FPeer with smallest distance
			if (dist_less < dist_greater)
				topK.add(less_peers.pollLast());
			else
				topK.add(greater_peers.pollFirst());								
		}

		// if less than k items are inserted in top-k array list
		if (topK.size() < k)
		{
			// compute the remaining number of FPeers to add to top-k array list
			final int residuals = k - topK.size();

			// check if the loop is exited because of "less_peers" or "greater_peers" emptiness
			NavigableSet<FPeer> residual_peers = (less_peers.size() == 0) ? greater_peers : less_peers;

			// insert from "residual_peers" the "residuals" FPeers closest to the passed location key
			for (int j = 0; j < residuals && residual_peers.size() > 0; j++)
			{
				final double dist_first = residual_peers.first().getDistanceFromLocationKey(locationKey);
				final double dist_last = residual_peers.last().getDistanceFromLocationKey(locationKey);
				if (dist_first < dist_last)
					topK.add(residual_peers.pollFirst());
				else
					topK.add(residual_peers.pollLast());			
			}
		}

		return topK;
	}


	/**
	 * Performs the update (adding or removing) of the neighborhood tree-representation of each one of the neighbors of the 
	 * running FPeer with the passed FPeer {@code fpeer_update}, avoiding the passed FPeer {@code fpeer_toAvoid}.
	 * @param fpeer_update  the FPeer with which update the trees
	 * @param fpeer_toAvoid the FPeer to avoid during the trees update
	 * @param linkablePID  the protocol identifier of the Linkable protocol used during the simulation
	 * @param remove	   flag that specify if the update perform removes or adds
	 * @return {@code true} if all the add/remove operations was completed successfully. {@code false} otherwise.
	 **/
	public boolean updateNeighborhood(FPeer fpeer_update, FPeer fpeer_toAvoid, int linkablePID, boolean remove)
	{
		boolean finalResult = true;

		// iterate on the neighborhood of the running protocol
		Iterator<FPeer> it = this.neighborsRBTree.iterator();

		while (it.hasNext())
		{
			// get the current-iteration neighbor
			FPeer neighbor = it.next();

			// if it is not the FPeer to avoid
			if (neighbor != fpeer_toAvoid)
			{
				// get its Linkable protocol
				LinkableProtocol cand_lp = (LinkableProtocol) neighbor.getProtocol(linkablePID);

				// update its tree representation (add/remove based on flag)
				if (remove)
					finalResult = finalResult && cand_lp.removeNeighbor(fpeer_update);
				else
					finalResult = finalResult && cand_lp.addNeighbor(fpeer_update);
			}
		}

		return finalResult;
	}


	/**
	 * ... Not implemented ...
	 **/
	@Override
	public void onKill() 
	{
		// TODO Auto-generated method stub
	}


	/**
	 * ... Not implemented ...
	 **/
	@Override
	public void pack() 
	{
		// TODO Auto-generated method stub
	}

}
