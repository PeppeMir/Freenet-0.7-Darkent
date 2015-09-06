package control;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * Class that manages, using uniform probability distribution, the generation and manipulation of the location keys to assign
 * to overlay's FPeers and contents.
 * 
 *  @author  Giuseppe Miraglia
 *  @since   March 9, 2015  
 **/

public class LocationKeysManager 
{
	// enum for coins faces
	public static enum Coin {HEAD, TAIL}								

	// maximum number of iteration to use during location keys generation
	private static int MAX_ITERATIONS = 5000;

	// set of already generated Fpeer location keys
	private static HashSet<Double> hsFpeers = new HashSet<Double>();

	// set of already generated content location keys
	private static HashSet<Double> hsContents = new HashSet<Double>();	

	/**
	 * Implements the operation of tossing of a biased coin, using a pseudo-random generator allocated with seed equal to the current 
	 * system nano time. <br>
	 * Notice that when {@code biasFactor} = 0.5, the tossing operation is equivalent to the toss of a balanced coin (equiprobability).
	 * @param biasFactor the biasing factor of the coin to toss, in [0.0, 1.0]
	 * @return HEAD or TAIL, based on the random generator result w.r.t. the {@code biasFactor}.
	 * @throws UnsupportedOperationException if the {@code biasFactor} value is out of range [0.0, 1.0].
	 **/
	public static Coin tossCoin(double biasFactor)
	{
		if (biasFactor < 0.0 || biasFactor > 1.0)
			throw new UnsupportedOperationException("toss bias factor out of range...");

		final double val = new Random(System.nanoTime()).nextDouble();

		return (val < biasFactor) ? Coin.HEAD : Coin.TAIL;
	}

	/**
	 * Generates a pseudo-random, evenly distributed, unique, double-precision location key to assign to a FPeer or a content.
	 * @param useAsContent flag that specify if the generated location key must be a content location key
	 * @throws UnsupportedOperationException if, after MAX_ITERATION iterations, the method is unable to generate an unique location key.
	 * @return the generated location key.
	 **/
	public static double generateUniform(boolean useAsContent)
	{		
		for (int i = 0; i < MAX_ITERATIONS; i++)
		{
			// generate a pseudo-random, evenly distributed, double-precision number in [0.0, 1.0)
			double locKey = Math.random();

			// check the uniqueness of the generated number in the set of FPeers and contents location keys
			if (!hsFpeers.contains(locKey) && !hsContents.contains(locKey))
			{
				// add the generated location key to the list of FPeer's location keys or contents location keys
				if (!useAsContent) 
					hsFpeers.add(locKey);
				else
					hsContents.add(locKey);

				return locKey;
			}
		}

		// if after MAX_ITERATIONS we are unable to generate an unique number, throw an exception..
		throw new UnsupportedOperationException("unable to generate an unique location key...");
	}


	/**
	 * Selects, in a pseudo-random fashion, a content location key from the set of already generated and available
	 * content location keys.
	 * @return a pseudo-random selected existing content location key, if it exists.
	 * @throws UnsupportedOperationException if the set of already generate content location keys is empty.
	 **/
	public static double getAvailableContentLocationKey()
	{
		// if the set of generated content location keys is empty 
		if (hsContents.size() == 0)
			throw new UnsupportedOperationException("no content location keys available...");

		// ... otherwise, select pseudo-randomly an index in [0, size)
		double selectedLocKey = -1.0;
		int index = (new Random(System.nanoTime())).nextInt(hsContents.size());

		// iterate on the set to find the index-th content location key
		Iterator<Double> it = hsContents.iterator();
		for (int i = 0; i <= index && it.hasNext(); i++)
			selectedLocKey = it.next().doubleValue();

		return selectedLocKey;
	}

}
