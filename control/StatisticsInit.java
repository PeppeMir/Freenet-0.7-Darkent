package control;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import peersim.config.Configuration;
import peersim.core.Network;
import protocol.LinkableProtocol;
import structure.FPeer;

/**
 *  Class that implements the initializer that performs some statistics on the already created overlay network.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   March 9, 2015  
 **/

public class StatisticsInit implements peersim.core.Control
{
	// the maximum distance used in Floyd-Warshall shortest paths computation
	private static int MAX_DISTANCE = 9999999;			

	// the configuration file PID associated to the used Linkable Protocol
	private final int linkablePID;

	// the DataSet name on which performs the statistics
	private final String dataSetName;

	// string that specifies which statistic performs on the DataSet
	private final String selectedOverlayStatistics;		

	/**
	 * Constructor method. 
	 * @param prefix the prefix, in the PeerSim configuration file, of the initializer.
	 **/
	public StatisticsInit(String prefix) 
	{
		// get the PID of the used Linkable protocol
		this.linkablePID = Configuration.getPid(prefix + ".linkable_pid");

		// get the DataSet path on which performs statistics and extract its name
		String datasetPath = Configuration.getString(prefix + ".dataset_path");
		this.dataSetName = datasetPath.substring(datasetPath.lastIndexOf('/') + 1, datasetPath.lastIndexOf('.'));

		// get the flag on statistics
		this.selectedOverlayStatistics = Configuration.getString(prefix + ".overlayStatistic");
	} 


	/**
	 * Computes and writes, on a statistic file, the local cluster coefficient (CC) for each FPeer of the overlay network
	 * having at least two neighbors, using the following formula: <br><br>
	 * 
	 * Let's {@code G=(V,E)} the undirected overlay network. <br>
	 * For each {@code v in V} with {@code |N(v)| >= 2} , {@code CC(v) = |Eind(v)| / |N(v)| * (|N(v)| - 1)} <br>
	 * where Eind(v) = {(j,k) in E | j,k in N(v)}
	 **/
	private void analizesFPeersLocalCC() 
	{	
		System.out.println("STATISTICS INITIALIZER: Performing Clustering Coefficient statistics on Dataset \"" + this.dataSetName + "\" ...");

		PrintWriter statFile = null;
		try 
		{
			// create/open the statistics file
			statFile = new PrintWriter(new BufferedWriter(new FileWriter("../statistics/stat_" + this.dataSetName + "_CC.stat", false)));		
			statFile.println("FPeer # \t CC");

			// counter for the number of CC-valid FPeers
			int validFPeers = 0;
			final int networkSize = Network.size();

			// for each FPeer of the overlay network
			for (int i = 0; i < networkSize; i++)
			{
				// get i-th FPeer "v", its Linkable protocol and its degree
				FPeer v = (FPeer) Network.get(i);
				LinkableProtocol v_lp = (LinkableProtocol) v.getProtocol(linkablePID);
				float v_degree = v_lp.degree();

				// local CC defined only for FPeers having at least two neighbors
				if (v_degree <= 1)
					continue;

				// variable that represent the value |Eind(v)|
				float cardEind_v = 0;

				Iterator<FPeer> v_it = v_lp.getNeighborsIterator();
				while (v_it.hasNext())
				{
					// get an FPeer "j" neighbor of "v" and its Linkable protocol
					FPeer j = v_it.next();

					Iterator<FPeer> j_it = ((LinkableProtocol) j.getProtocol(linkablePID)).getNeighborsIterator();
					while (j_it.hasNext())
					{
						// get an FPeer "k" neighbor of "j"
						FPeer k = j_it.next();

						// "j" is not "v" and exists and edge (j,k), count this edge
						if (k != v && v_lp.contains(k))
							cardEind_v++;
					}
				}

				// compute the Clustering Coefficient of "v" and write it on the statistics file
				float CC_v = cardEind_v / (v_degree * (v_degree - 1));
				statFile.println((validFPeers + 1) + "\t" + CC_v);

				// increase nodes counter
				validFPeers++;
			}

			// write ratio #tot-FPeers - #defined-CC-FPeers on statistics file
			statFile.println("#Tot FPeers: \t " + networkSize);
			statFile.println("#Defined-CC FPeers: \t " + validFPeers);

		}
		catch (IOException e) 
		{
			System.out.println("Error during statistics file opening/writing: \n" + e.getMessage()); 
		}
		finally
		{
			// close the file
			if (statFile != null)
				statFile.close();
		}	

		System.out.println("STATISTICS INITIALIZER: Clustering Coefficient statistics on Dataset \"" + this.dataSetName + "\" completed successfully ...");
	}


	/**
	 * Writes, on the statistics file, the degree (number of neighbors) of each FPeer of the overlay network.
	 **/
	private void analizesFPeersDegree() 
	{
		System.out.println("STATISTICS INITIALIZER: Performing FPeer's degrees statistics on Dataset \"" + this.dataSetName + "\" ...");

		PrintWriter statFile = null;
		try 
		{
			// create/open the statistics file
			statFile = new PrintWriter(new BufferedWriter(new FileWriter("../statistics/stat_" + this.dataSetName + "_degrees.stat", false)));		
			statFile.println("ID \t Degree");

			// for each FPeer of the overlay network
			for (int i = 0; i < Network.size(); i++)
			{
				// get i-th FPeer and its Linkable protocol
				FPeer fpeer = (FPeer) Network.get(i);
				LinkableProtocol fpeer_lp = (LinkableProtocol) fpeer.getProtocol(linkablePID);

				// print its degree on the statistics file
				statFile.println(fpeer.getIdentifier() + "\t" + fpeer_lp.degree());
			}
		}
		catch (IOException e) 
		{
			System.out.println("Error during statistics file opening/writing: \n" + e.getMessage());  
		}
		finally
		{
			// close the file
			if (statFile != null)
				statFile.close();
		}		

		System.out.println("STATISTICS INITIALIZER: FPeer's degrees statistics on Dataset \"" + this.dataSetName + "\" completed successfully ...");
	}

	
	/**
	 * Floyd–Warshall algorithm for the computation of the length of all-shortest paths between each pair of FPeers of 
	 * the overlay network in O(|V|^3).
	 * Algorithm from <a href="http://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm#Algorithm">Wikipedia</a>.
	 * @param networkSize	the size of the overlay network on which computes all the shortest paths
	 **/
	private int[][] FloydWarshall(int networkSize)
	{
		System.out.println("STATISTICS INITIALIZER: Performing Overlay's all shortest paths computation on Dataset \"" + this.dataSetName + "\" ...");

		// allocates the distance matrix which will contain shortest paths lengths
		int [][] distMatrix = new int [networkSize][networkSize];

		// initialize matrix with "0" on the diagonal and "infinity" on the other cells
		for (int k = 0; k < networkSize; k++)
			for (int i = 0; i < networkSize; i++)
				distMatrix[k][i] = ((k == i) ? 0 : MAX_DISTANCE);

		// initialize to "1" the cells representing FPeers adjacency
		for (int i = 0; i < networkSize; i++)
		{
			Iterator<FPeer>  i_it = ((LinkableProtocol) ((FPeer) Network.get(i)).getProtocol(linkablePID)).getNeighborsIterator();
			while (i_it.hasNext())
			{
				int index = i_it.next().getIndex();
				distMatrix[i][index] = 1;
			}
		}

		// computes all shortest path between each pair of FPeers of the overlay
		for (int k = 0; k < networkSize; k++)
			for (int i = 0; i < networkSize; i++)
				for (int j = 0; j < networkSize; j++)
					if (distMatrix[i][j] > (distMatrix[i][k] + distMatrix[k][j]))
						distMatrix[i][j] = (distMatrix[i][k] + distMatrix[k][j]);

		
		System.out.println("STATISTICS INITIALIZER: Overlay's all shortest paths computation on Dataset \"" + this.dataSetName + "\" completed successfully ...");

		return distMatrix;
	}
	
	
	/**
	 *  In order to find the longest shortest path (so the overlay network diameter), the method: <br><br>
	 *  
	 *  1) computes all the shortest paths between each pair of nodes of the overlay, using Floyd-Warshall algorithm; <br>
	 *  2) finds the overlay's diameter as maximum length between all the shortest paths and writes it on the 
	 *     configuration file. 
	 **/
	private void findDiameter() 
	{
		final int networkSize = Network.size();
		
		// the variable which will contain the length of the longest shortest path from each pair of nodes
		int maxShortestPath = -1;
				
		// computes all shortest paths between each pair of nodes of the network using Floyd-Warshall algorithm
		int [][] distMatrix = FloydWarshall(networkSize);
		
		System.out.println("STATISTICS INITIALIZER: Performing Overlay's diameter computation on Dataset \"" + this.dataSetName + "\" ...");

		// for each pair of FPeers of the network
		for (int i = 0; i < networkSize; i++)
			for (int j = 0; j < networkSize; j++)		
				if (distMatrix[i][j] > maxShortestPath)
					maxShortestPath = distMatrix[i][j];
		
		// suggest Garbage Collector invocation for distance matrix deallocation
		distMatrix = null;
		System.gc();
		
		PrintWriter statFile = null;
		try 
		{
			// create/open the statistics file
			statFile = new PrintWriter(new BufferedWriter(new FileWriter("../statistics/stat_" + this.dataSetName + "_diameter.stat", false)));		
			statFile.println("Number of FPeers: \t " + networkSize);
			statFile.println("Overlay diameter: \t " + maxShortestPath);
		}
		catch (IOException e)
		{
			System.out.println("Error during statistics file opening/writing: \n" + e.getMessage()); 
		}
		finally
		{
			// close the file
			if (statFile != null)
				statFile.close();
		}
		
		System.out.println("STATISTICS INITIALIZER: Overlay's diameter computation on Dataset \"" + this.dataSetName + "\" completed successfully ...");
	}


	/**
	 *  In order to find the average shortest path length, the method: <br><br>
	 *  
	 *  1) computes all the shortest paths between each pair of nodes of the overlay, using Floyd-Warshall algorithm; <br>
	 *  2) then sums all the lengths of these shortest paths and divides the obtained value by the number of undirected edges
	 *  between the nodes of the overlay and writes it on the configuration file.
	 **/
	private void findAvgShortestPathLength()
	{
		final int networkSize = Network.size();
		final double dNetworkSize = (double) Network.size();
		double sumShPathsLength = 0.0;
		
		// computes all shortest paths between each pair of nodes of the network using Floyd-Warshall algorithm
		int [][] distMatrix = FloydWarshall(networkSize);
		
		System.out.println("STATISTICS INITIALIZER: Overlay's average path length computation on Dataset \"" + this.dataSetName + "\" ...");
		
		// sum the shortest paths length between each pair of nodes of the network
		for (int i = 0; i < networkSize; i++)
			for (int j = 0; j < networkSize; j++)
				sumShPathsLength += distMatrix[i][j];
		
		// suggest Garbage Collector invocation for distance matrix deallocation
		distMatrix = null;
		System.gc();
		
		// compute the average shortest paths length
		double avgShPathLength = sumShPathsLength / (dNetworkSize * (dNetworkSize - 1.0));
		
		PrintWriter statFile = null;
		try 
		{
			// create/open the statistics file
			statFile = new PrintWriter(new BufferedWriter(new FileWriter("../statistics/stat_" + this.dataSetName + "_avgpath.stat", false)));		
			statFile.println("Number of FPeers: \t " + networkSize);
			statFile.println("Overlay Average Path Length: \t " + avgShPathLength);
		}
		catch (IOException e)
		{
			System.out.println("Error during statistics file opening/writing: \n" + e.getMessage()); 
		}
		finally
		{
			// close the file
			if (statFile != null)
				statFile.close();
		}
		
		System.out.println("STATISTICS INITIALIZER: Overlay's average path length computation on Dataset \"" + this.dataSetName + "\" completed successfully ...");
	}


	/**
	 * Performs some statistics on the already parsed and created overlay network, as specified in the configuration file, including:<br>
	 * 1) the {@code degree} of each FPeer of the overlay network; <br>
	 * 2) the local {@code clustering coefficient} of each FPeer of the overlay network; <br>
	 * 3) the {@code diameter} of the overlay network; <br>
	 * 4) the {@code average path length} of the overlay network. <br>
	 * All the computed informations are written on a statistics file.
	 * @return always {@code false}.
	 **/
	@Override
	public boolean execute() 
	{
		// if allowed, performs some statistics on the parsed Data Set
		switch (this.selectedOverlayStatistics)
		{
		case "degree" : 	{ analizesFPeersDegree(); break; }
		case "cc" : 		{ analizesFPeersLocalCC(); break; }
		case "diameter" :	{ findDiameter(); break; } 
		case "avgpl" :		{ findAvgShortestPathLength(); break; }
		case "null" : 
		default: 			{ break; }
		}

		return false;
	}

}
