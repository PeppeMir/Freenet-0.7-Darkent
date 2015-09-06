package structure;

/**
 *  Class that implements the prototype of the message exchanged by the FPeers during the simulation.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   March 9, 2015  
 **/

public class Message implements Cloneable
{
	// enum for message typing
	public static enum Type {GET, GET_FOUND, GET_NOTFOUND, PUT, PUT_OK, 
		PUT_COLLISION, PUT_REPLICATION, PUT_REPL_COLLISION, SWAP, SWAP_OK, SWAP_REFUSED}
	
	// static counter to assigns an unique identifier to new messages
	private static long nextMessageID = 0;
	
	// the unique identifier of the message
	private long messageID;
	
	// the type of the message
	private Type messageType;
	
	// the location key of the message
	private double messageLocationKey;
	
	// the reference to the FPeer that have sent the last time the message
	private FPeer lastHopFPeer;
	
	// the Hops-To-Live counter of the message
	private int HTL;
	
	// the location key of the closest (w.r.t. messageLocationKey) FPeer encountered during the routing of the message
	private double pathClosestLocKey;
	
	// the True-Hops-Counter of the message (statistics only)
	private int THC;
	
	
	/**
	 * Constructor method for a generic message. Initializes the object fields with the relative passed values. <br>
	 * N.B. the field {@code prevHopLocKey} is set to {@code creatorLocKey}.
	 * @param _messageType		the type to set as message type
	 * @param _messageLocKey	the location key to set as message location key
	 * @param _HTL				the initial HTL value to use as initial message HTL
	 **/
	public Message(Type _messageType, double _messageLocKey, int _HTL)
	{
		this.messageID = Message.nextMessageID++;
		this.messageType = _messageType;
		this.messageLocationKey = _messageLocKey;
		this.lastHopFPeer = null;
		this.HTL = _HTL;	
		this.pathClosestLocKey = -1.0;
		this.THC = 0;
	}
	
	
	/**
	 * Gets out the unique identifier of the message.
	 * @return the unique identifier of the message.
	 **/
	public long getMessageID()
	{
		return this.messageID;
	}
	
	
	/**
	 * Gets out the current type of the message.
	 * @return the current type of the message.
	 **/
	public Type getMessageType()
	{
		return this.messageType;
	}
	
	
	/**
	 * Changes the message current type with the passed type {@code t}.
	 * @param t	the type to set as message current type
	 **/
	public void changeMessageType(Type t)
	{
		this.messageType = t;
	}
	
	
	/**
	 * Gets out the current location key of the message.
	 * @return the current location key of the message.
	 **/
	public double getMessageLocationKey()
	{
		return this.messageLocationKey;
	}
	
	/**
	 * Gets out the reference to the FPeer thath have sent the message the last time.
	 * @return  the reference to the FPeer thath have sent the message the last time
	 **/
	public FPeer getLastHopFPeer()
	{
		return this.lastHopFPeer;
	}
	
	
	/**
	 * Changes the reference to the FPeer that have sent the message the last time with the passed reference {@code fpeer}.
	 * @param fpeer the FPeer reference to set as last hop Fpeer value
	 **/
	public void changeLastHopFPeer(FPeer fpeer)
	{
		this.lastHopFPeer = fpeer;
	}
	
	/**
	 * Gets out the current HTL value of the message.
	 * @return the current HTL value of the message.
	 **/
	public int getHTL()
	{
		return this.HTL; 
	}
	
	
	/**
	 * Decreases by 1 the current HTL value of the message.
	 **/
	public void decreaseHTL()
	{
		this.HTL--;
	}
	
	
	/**
	 * Resets the HTL value of the message with the passed value {@code value}.
	 * @param value	the value to set as message HTL value
	 **/
	public void resetHTLTo(int value)
	{
		this.HTL = value;
	}
	
	
	/**
	 * Gets out the FPeer location key closest w.r.t. the content location key encountered during the
	 * message routing.
	 * @return	the FPeer location key closest w.r.t. the content location key encountered during the
	 * 			message routing.
	 */
	public double getPathClosestLocKey()
	{
		return this.pathClosestLocKey;
	}
	
	
	/**
	 * Changes the FPeer location key closest w.r.t. the content location key encountered during the
	 * message routing using the passed location key {@code locKey}.
	 * @param locKey	the new, closest w.r.t. the content location key, location key
	 */
	public void changePathClosestLocKey(double locKey)
	{
		this.pathClosestLocKey = locKey;
	}
	
	
	/**
	 * Gets out the current THC value of the message.
	 * @return	the current THC value of the message
	 **/
	public int getTHC()
	{
		return this.THC;
	}
	
	
	/**
	 * Increases by 1 the current THC value of the message.
	 **/
	public void increaseTHC()
	{
		this.THC++;
	}
	
	
	/**
	 * Creates an exact clone of the message.
	 * @return the clone of the message.
	 **/
	@Override
	public Object clone() 
	{
		Message cloned_mex = null;
		
		try
		{
			cloned_mex = (Message) super.clone();
			cloned_mex.messageID = this.messageID;
			cloned_mex.messageLocationKey = this.messageLocationKey;
			cloned_mex.messageType = this.messageType;
			cloned_mex.lastHopFPeer = this.lastHopFPeer;
			cloned_mex.HTL = this.HTL;
			cloned_mex.pathClosestLocKey = this.pathClosestLocKey;
			cloned_mex.THC = this.THC;
		}
		catch (CloneNotSupportedException exc)
		{
			System.out.println("Error during message cloning ...\n" + exc.getMessage());
		}
		
		return cloned_mex;
	}
	
	
	/**
	 * Serialize the class fields in the string (ID, type, mexLocKey, HTL, lastHopFPeerID, pathClosestLocKey).
	 * @return  the serialized message as string.
	 **/
	@Override
	public String toString()
	{
		return "(ID=" + this.messageID + ",type=" + this.messageType + ",mexLocKey=" + this.messageLocationKey + ",HTL=" + this.HTL + ",lastHop=" + this.lastHopFPeer.getIdentifier() + ",pathClosestLocKey=" + this.pathClosestLocKey + ")";	
	}
}
