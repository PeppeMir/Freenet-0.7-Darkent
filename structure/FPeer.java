package structure;

import java.util.HashSet;

import protocol.LinkableProtocol;

/**
 *  Class that implements the prototype of Freenet 0.7 (and PeerSim node) used during the simulation.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   March 9, 2015  
 **/

public class FPeer extends peersim.core.GeneralNode implements Comparable<FPeer>
{
	// unique and immutable string, assigned to the FPeer when retrieved from the given DataSet
	private String identifier;	 			
	
	// unique, pseudo-random assigned, double-precision number in [0.0, 1.0)
	private double locationKey;  
	
	// HashSet for the storing of all contents locations keys paired with the contents that the FPeer stores
	private HashSet<Double> storedContentLocationKeys;	
	
	// indicates if FPeer is involved in a swapping process with the another FPeer
	private boolean isInvolvedInSwap;
	
	/**
	 * Basic constructor method. It is only checked by the simulator, but is unused in the rest of
	 * the protocol.
	 * @param prefix string prefix of the configuration file for the class
	 **/
	public FPeer(String prefix)
	{
		super(prefix);
	}


	/**
	 * Specialized constructor method. It is used by the overlay initializer for the allocation of
	 * the FPeers of the overlay network.
	 * @param prefix  string prefix of the configuration file for the class
	 * @param _ID     the identifier of the FPeer to allocate
	 * @param _locKey the location key of the FPeer to allocate
	 **/
	public FPeer(String prefix, String _ID, double _locKey) 
	{
		super(prefix);
		this.identifier = _ID;
		this.locationKey = _locKey;
		this.storedContentLocationKeys = new HashSet<Double>();
		this.isInvolvedInSwap = false;
	}

	
	/**
	 * Computes the circular distance between the running FPeer {@code this} location key and the passed
	 * location key.
	 * @param locKey the location key to compare with the running FPeer's location key
	 * @return {@code min (abs(this.locKey - locKey), 1 - abs(this.locKey - locKey)) } 
	 **/
	public double getDistanceFromLocationKey(double locKey)
	{
		final double dist = Math.abs(this.locationKey - locKey);
		return Math.min(dist, 1 - dist);
	}
	
	
	/**
	 * Compares the passed FPeer {@code p} with the FPeer {@code this}, via address-comparison.
	 * In other words, two FPeers are equals if they are the same object.
	 * @param peer  the FPeer to compare with the FPeer {@code this}
	 * @return {@code true} if the two FPeers are equals. {@code false} otherwise. 
	 **/
	@Override
	public boolean equals(Object peer)
	{
		FPeer p = (FPeer) peer;
		return this == p;
	}


	/**
	 * Compares the passed FPeer {@code p} with the FPeer {@code this}, via location keys comparison.
	 * @param p the FPeer to be compared with {@code this}
	 * @return {@code 0} if {@code p} and {@code this} have the same location key; 
	 * 		   {@code -1} if the location key of {@code this} is less than the location key of {@code p};
	 * 		   {@code 1} otherwise.
	 **/
	@Override
	public int compareTo(FPeer p) 
	{
		final double this_locKey = this.getLocationKey();
		final double p_locKey = p.getLocationKey();

		if (this_locKey == p_locKey)
			return 0;
		else
			if (this_locKey < p_locKey)
				return -1;
			else
				return 1;	
	}


	/**
	 * Gets out the unique identifier of the FPeer.
	 * @return the unique identifier of the FPeer.
	 **/
	public String getIdentifier()
	{
		return this.identifier;
	}


	/**
	 * Gets out the location key of the FPeer.
	 * @return the location key of the FPeer.
	 **/
	public double getLocationKey()
	{
		return this.locationKey;
	}


	/**
	 * Checks if the passed content location key {@code locKey} is stored by the FPeer {@code this}.
	 * @param locKey the content location key on which perform the check
	 * @return {@code true} if the passed content location key is stored by the FPeer;
	 * 		   {@code false} otherwise.
	 **/
	public boolean containsContentLocationKey(double locKey)
	{
		return this.storedContentLocationKeys.contains(locKey);
	}


	/**
	 * Tries to add the passed location key {@code locKey} to the local storage of contents location keys of the FPeer {@code this}.
	 * @param locKey the key to add to the set of contents locations keys
	 * @return {@code false} if the local set already contains the passed location key (in this case, the set leave unchanged);
	 * 		   {@code true} otherwise.
	 **/
	public boolean addContentLocationKey(double locKey)
	{
		return this.storedContentLocationKeys.add(locKey);
	}

	/**
	 * Gets out the swapping status of the FPeer {@code this}.
	 * @return {@code true} if it is currently involved in a SWAP process with another FPeer. {@code false} otherwise.
	 **/
	public boolean isInvolvedInSwap()
	{
		return this.isInvolvedInSwap;
	}
	
	
	/**
	 * Sets the swapping status of the FPeer {@code this}.
	 * @param involved	the new swapping status of {@code this}
	 */
	public void setSwapStatus(boolean involved)
	{
		this.isInvolvedInSwap = involved;
	}
	
	
	/**
	 * Performs the swap of the location key and of the stored content location keys of the running FPeer with the relative fields
	 * of the passed FPeer {@code fpeer}.
	 * Furthermore, updates the neighbor's trees neighborhood representation with the new locations-changed FPeers. 
	 * This last operation is needed to maintains the trees sorted by FPeer's location keys and ensure O(logN) operations on its.
	 * @param fpeer  the FPeer with which perform the swap
	 * @param linkablePID the protocol identifier of the Linkable protocol used during the simulation
	 **/
	public void swapWith(FPeer fpeer, int linkablePID)
	{
		// get Linkable protocols of the FPeers to swap
		LinkableProtocol this_lp = (LinkableProtocol) this.getProtocol(linkablePID);
		LinkableProtocol fpeer_lp = (LinkableProtocol) fpeer.getProtocol(linkablePID);	

		// remove-update for this's neighbors tree representation, at exception of the neighbor "fpeer"
		this_lp.updateNeighborhood(this, fpeer, linkablePID, true);

		// remove-update for fpeer's neighbors tree representation, at exception of the neighbor "this"
		fpeer_lp.updateNeighborhood(fpeer, this, linkablePID, true);

		// ad-hoc remove-update for "fpeer" in the neighborhood of "this", and vice-versa
		this_lp.removeNeighbor(fpeer);
		fpeer_lp.removeNeighbor(this);

		// storing temporary for swapping
		double oldLocKey = this.locationKey;
		HashSet<Double> oldSet = this.storedContentLocationKeys;		

		// performs location keys and stored content location keys swapping for "this"
		this.locationKey = fpeer.locationKey;
		this.storedContentLocationKeys = fpeer.storedContentLocationKeys;

		// performs location keys and stored content location keys swapping for "fpeer"
		fpeer.locationKey = oldLocKey;
		fpeer.storedContentLocationKeys = oldSet;

		// add-update for this's neighbors tree representation, at exception of the neighbor "fpeer"
		this_lp.updateNeighborhood(this, fpeer, linkablePID, false);

		// add-update for fpeer's neighbors tree representation, at exception of the neighbor "this"
		fpeer_lp.updateNeighborhood(fpeer, this, linkablePID, false);

		// ad-hoc add-update for "fpeer" in the neighborhood of "this", and vice-versa
		this_lp.addNeighbor(fpeer);
		fpeer_lp.addNeighbor(this);

	}

	
	/**
	 * Serialize the class fields in the string (identifier, locationKey, localStorageContentsKeys).
	 * @return the serialized FPeer as string
	 **/
	@Override
	public String toString()
	{
		return "(ID=" + this.getIdentifier() + ",locKey=" + this.getLocationKey() + ",contentsKeys=" + this.storedContentLocationKeys + ")";
	}
}
