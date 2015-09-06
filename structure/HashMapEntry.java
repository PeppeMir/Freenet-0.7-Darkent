package structure;

import java.util.Date;
import java.util.HashSet;

/**
 *  Class that implements the HashMap entry, associated to message identifiers used by the Hybrid protocol, in a compact way.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   March 9, 2015  
 **/

public class HashMapEntry 
{
	// reference to FPeer from which was received a message
	private FPeer receivedFrom;		
	
	//references to FPeers to which was sent a message
	private HashSet<FPeer> sentTo;	
	
	// the entry's last use time stamp 
	private long lastUseTimetamp;

    
	/**
	 * Constructor method. Initializes the {@code receivedFrom} object field to the passed {@code _receivedFrom} FPeer,
	 * leaving to {@code null} the {@code sentTo} field. 
	 * Furthermore, initialize the {@code lastUseTimetamp} object field with the current time.
	 * @param _receivedFrom  the FPeer reference used to initialize the field {@code receivedFrom}.
	 **/
	public HashMapEntry(FPeer _receivedFrom) 
	{
		this.receivedFrom = _receivedFrom;
		this.sentTo = null;
		
		// update last use time stamp
		setTimestampToNow();
	}
	
	/**
	 * Sets the {@code lastUseTimetamp} object field to the current time.
	 **/
	private void setTimestampToNow()
	{
		// change the last use time stamp to the current date-time
		this.lastUseTimetamp = new Date().getTime();
	}
	
	/**
	 * Gets the FPeer's reference contained in the {@code receivedFrom} object field, after setting the entry's last use
	 * time stamp to the current time.
	 * @return the FPeer's reference contained in the {@code receivedFrom} field.
	 **/
	public FPeer getReceivedFrom()
	{
		// update last use time stamp
		setTimestampToNow();
		
		return this.receivedFrom;
	}
	
	
	/**
	 * First of all, if the HashSet field is not allocated, allocates it.
	 * Then adds the passed FPeer's reference {@code fpeer} to this HashSet, if it is not already contained (no duplicates admitted).
	 * Finally, sets the entry's last use time stamp to the current time.
	 * @param fpeer  the reference to the FPeer to add into the HashSet
	 **/
	public void addSent(FPeer fpeer)
	{
		if (this.sentTo == null)
			this.sentTo = new HashSet<FPeer>();
		
		this.sentTo.add(fpeer);
		
		// update last use time stamp
		this.setTimestampToNow();
	}
	
	
	/**
	 * Checks if the passed FPeer's reference {@code fpeer} is contained in the object's {@code sentTo} field, after setting the entry's last
	 * use time stamp to the current time.
	 * @param  fpeer  the FPeer's reference on which perform the check
	 * @return {@code false} if the HashSet is not allocated or {@code fpeer} is not in the HashSet.
	 * 		   {@code true}	 otherwise.
	 **/
	public boolean alreadySentTo(FPeer fpeer)
	{
		// update last use time stamp
		setTimestampToNow();
		
		if (this.sentTo == null || !this.sentTo.contains(fpeer))
			return false;
		else
			return true;
	}
	
	
	/**
	 * Returns the time of the last modification on the object (field {@code lastModTimetamp}).
	 * @return the time of the last modification on the object.
	 **/
	public long getLastModTimetamp()
	{
		return this.lastUseTimetamp;
	}

	
	/**
	 * Serializes the class fields in the string (receivedFromID, [locKey1,...], timestamp).
	 * @return  the serialized message as string.
	 **/
	@Override
	public String toString() 
	{
		return "(recFromID=" + this.receivedFrom.getIdentifier() + ",SentTo=" + this.sentTo + ",timestamp=" + this.lastUseTimetamp + ")";
	}
}
