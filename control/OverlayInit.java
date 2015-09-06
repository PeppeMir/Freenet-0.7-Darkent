package control;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import peersim.config.Configuration;
import peersim.core.Network;
import protocol.LinkableProtocol;
import structure.FPeer;

/**
 *  Class that implements the initializer that parses the given Data Set and sets up the overlay network from it.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   February 9, 2015  
 **/

public class OverlayInit implements peersim.core.Control
{
	// the configuration file's string representing the prefix of the prototype node used during the simulation
	private final String FPeer_prefix;
	
	// the PID associated to the used Linkable Protocol
	private final int linkablePID;
	
	// the path of the Data Set to parse in order to set up the overlay network
	private final String datasetPath;				 	
	

	/**
	 * Constructor method. 
	 * @param prefix the prefix, in the PeerSim configuration file, of the initializer.
	 **/
	public OverlayInit(String prefix) 
	{
		// retrieves the prefix for manage nodes of the overlay
		this.FPeer_prefix = Configuration.getString(prefix + ".FPeer_prefix");

		// initialize the dataset path from which create the overlay network
		this.datasetPath = Configuration.getString(prefix + ".dataset_path");

		// get the PID of the used Linkable protocol
		this.linkablePID = Configuration.getPid(prefix + ".linkable_pid");
	} 
	
	/**
	 * Adds a new FPeer into the overlay network, generating an unique, pseudo-random, double-precision location key for it.
	 * @param ID	 the Data Set's identifier for the FPeer to add
	 * @return the created FPeer.
	 **/
	private FPeer createAndAddFPeer(String ID)
	{	
		try
		{
			// allocates an FPeer for the passed ID assigning it an uniform, pseudo-random, double-precision location key
			FPeer fpeer = new FPeer(FPeer_prefix, ID, LocationKeysManager.generateUniform(false));

			// add the created FPeer into the PeerSim overlay network
			Network.add(fpeer);

			return fpeer;
		}
		catch (UnsupportedOperationException exc) 
		{
			System.out.println("ID=" + ID + " FPeer's creation: " + exc.getMessage());
			return null;
		}
	}


	/**
	 * The method performs the following operations: <br>
	 * 1) Parses the given data set (from a path, specified in the configuration file) using the values in it as FPeer identifiers; <br>
	 * 2) Creates the correspondent overlay network, allocating FPeers and filling their neighborhoods; <br>
	 * 3) Assigns an unique location key to each allocated FPeer, pseudo-random and evenly generated.
	 * @return {@code true}, if a problem occurs and the execution must be stopped; {@code false} otherwise.
	 **/
	@Override
	public boolean execute() 
	{
		BufferedReader reader = null;
		long elapsedTime = -1;
		String line = "";

		try 
		{	
			// get current time (in ms)
			elapsedTime = System.currentTimeMillis();

			// create a buffered reader for the file at the specified path
			reader = new BufferedReader(new FileReader(this.datasetPath));

			System.out.println("OVERLAY INITIALIZER: Creating overlay network parsing dataset at path \"" + this.datasetPath + "\" ...");

			// HashMap used to check and retrieve efficiently FPeers during the parsing of the dataset
			HashMap<String, FPeer> hashmap = new HashMap<String, FPeer>();
			FPeer p_sx = null, p_dx = null;

			// read the file line-by-line
			while ((line = reader.readLine()) != null) 
			{			
				// split the line on comma
				String[] identifiers = line.split(",");

				// check if already exist an FPeer with identifiers[0]
				if ((p_sx = hashmap.get(identifiers[0])) == null)
				{
					// if not, allocates an FPeer for it and add it into the overlay
					p_sx = createAndAddFPeer(identifiers[0]);

					// add the association (id, FPeer_ref) for it in the HashMap
					hashmap.put(identifiers[0], p_sx);
				}

				// check if already exist an FPeer with identifiers[1]
				if ((p_dx = hashmap.get(identifiers[1])) == null)
				{
					// if not, allocates an FPeer for it and add it into the overlay
					p_dx = createAndAddFPeer(identifiers[1]);

					// add the association (id, FPeer_ref) for it in the HashMap
					hashmap.put(identifiers[1], p_dx);
				}

				// add p_sx -> p_dx link into the overlay network
				((LinkableProtocol) p_sx.getProtocol(linkablePID)).addNeighbor(p_dx);

				// add p_dx -> p_sx link into the overlay network
				((LinkableProtocol) p_dx.getProtocol(linkablePID)).addNeighbor(p_sx);
			}
		} 
		catch (IOException e) 
		{
			// if some problems occurs, stop the execution...
			System.out.println("OVERLAY INITIALIZER: Dataset parsing error at path \"" + this.datasetPath + "\" ...\nError details: " + e.getMessage() + "\n\n*** Execution stopped ***");
			return true;
		} 
		finally 
		{
			try 
			{
				// close the reading buffer after the parsing
				reader.close();
			} 
			catch (Exception e) { }
		}

		// get current time and compute parsing/overlay creation elapsed time (in ms)
		elapsedTime = System.currentTimeMillis() - elapsedTime;

		System.out.println("OVERLAY INITIALIZER: Dataset parsed successfully and overlay created (" + elapsedTime + " ms) ...");
		
		return false;
	}
}
