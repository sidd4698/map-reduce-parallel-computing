//******************************************************************************
//
// File:    DnaCensus.java
//
// This Java source file is developed by Siddesh Pillai that uses Parallel Java  
// 2 library ("PJ2") written by Alan Kaminsky. 
//
//******************************************************************************

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.rit.pj2.vbl.LongVbl;
import edu.rit.pjmr.Combiner;
import edu.rit.pjmr.Customizer;
import edu.rit.pjmr.Mapper;
import edu.rit.pjmr.PjmrJob;
import edu.rit.pjmr.Reducer;
import edu.rit.pjmr.TextDirectorySource;
import edu.rit.pjmr.TextId;

/**
 * This is the main class for a PjmrJob map-reduce job which computes
 * the occurrences of the query sequence from a set of files containing the
 * DNA sequence. It prints the output in format of DNA sequence ID and the 
 * number of occurrences. It displays in descending order of occurrences. In 
 * cases of same number of occurrences it prints based on lexicographical 
 * order of the DNA sequence id.
 * Usage: java pj2 jar=<jarfile> DnaCensus <query> <directory> <node> 
 * [<node> ...]     
 * 
 * @author Siddesh Pillai
 * @version 19-Nov-2015
 */
public class DnaCensus extends PjmrJob<TextId, String, String, LongVbl> {

	public void main(String[] args) throws Exception {

		// Parse command line arguments.
		if(args.length < 2) usage("Invalid number of arguments.");
		else if (args.length == 2) usage("Missing Node names.");

		// Query parameter
		String query = args[0];

		// Directory Path 
		String directory = args[1];

		// Nodes
		String[] nodes = new String[args.length-2];

		for(int i = 0; i < args.length-2; i++) {
			nodes[i] = args[i+2];
		}

		// Determine number of mapper threads.
		int NT = Math.max(threads(), 1);

		// Configure mapper tasks.
		for(String node : nodes) 
			mapperTask (node) 
			.source (new TextDirectorySource(directory))
			.mapper (NT, MyMapper.class, query); 

		// Configure reducer task.
		reducerTask().runInJobProcess().customizer(MyCustomizer.class)
		.reducer(MyReducer.class);

		startJob();
	}


	/**
	 * Mapper class. 
	 */
	private static class MyMapper extends Mapper<TextId, String, String, LongVbl> {

		// Member Variables
		private static final LongVbl ONE = new LongVbl.Sum (1L);
		private String querySequence;
		private String sequenceID = null;
		private StringBuilder dnaSequence;

		// Initializing the querySequence
		public void start(String[] args, Combiner<String,LongVbl> combiner) {
			querySequence = args[0];
			dnaSequence = new StringBuilder();
		} 

		// Processing one line at a time
		public void map(TextId inKey, String inValue,          
				Combiner<String,LongVbl> combiner) {

			// Check whether the line is sequence ID or dna sequence
			if(inValue.contains(">")) {

				// check for the previous dna sequence and add to the combiner
				if(sequenceID != null) {
					performPatternMatch(combiner);
				} 

				sequenceID = inValue.substring(1, inValue.indexOf(" ")); 
				dnaSequence = new StringBuilder();

			} else {
				dnaSequence.append(inValue);
			}
		}

		// Once the file is done reading this performs the sequence check 
		// of the last read dna sequence.
		public void finish(Combiner<String,LongVbl> combiner)  {
			performPatternMatch(combiner);
		}

		/**
		 * This method is responsible to check for query sequence on the 
		 * dna sequence. If its a match then add to the combiner object.
		 * 
		 * @param combiner is a Combiner<String, LongVbl> object.
		 */
		private void performPatternMatch(Combiner<String,LongVbl> combiner) {

			// Current Index for the dna sequence
			int index = 0;

			Pattern seq = Pattern.compile(querySequence);
			Matcher matcher = seq.matcher(dnaSequence);

			// Sequentially traverse through the index of dna sequence to check 
			// for a match and then add to the combiner.
			while(matcher.find(index)) {
				combiner.add(sequenceID, ONE);
				index = matcher.start() + 1;
			}
		}
	}

	/**
	 * Reducer task customizer class.
	 */
	private static class MyCustomizer extends Customizer<String,LongVbl> {

		// Check for precedence based on the no. of occurence in descending order.
		// If both the occurence match then add in ascending order.
		public boolean comesBefore(String key_1, LongVbl value_1, 
				String key_2, LongVbl value_2) {

			if (value_1.item > value_2.item) return true;
			else if (value_1.item < value_2.item) return false;
			else return key_1.compareTo(key_2) < 0;
		}
	}

	/**
	 * Reducer class.
	 */
	private static class MyReducer extends Reducer<String,LongVbl> {

		// Prints the result
		public void reduce(String key, LongVbl value) {
			System.out.printf ("%s\t%s%n", key, value);
			System.out.flush();
		}
	}

	/**
	 * Print a usage message and exit.
	 */
	private void usage() {
		System.err.println("Usage: java pj2 jar=<jarfile> DnaCensus <query> "
				+ "<directory> <node> [<node> ...]");
		System.err.println("<jar> is the name of the JAR file containing "
				+ "all of the program's Java class files.");
		System.err.println("<query> is the query sequence.");
		System.err.println("<directory> is the name of the directory on each node's "
				+ "local hard disk containing the data files to be analyzed.");
		System.err.println("<node> is the name of a cluster node on which to run the "
				+ "analysis. One or more node names must be specified.");
		throw new IllegalArgumentException();
	}
	
	/**
	 * Printing the error message and exit
	 * @param message is the Error message a String value.
	 */
	private void usage(String message) {
		System.err.println(message);
		usage();
	}

}
