package it.unimi.dsi.webgraph.examples;

import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.CompressedIntLabel;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;


public class BitStreamArcLabelledImmutableGraphAsIntTriples {

	private BitStreamArcLabelledImmutableGraphAsIntTriples() {}

	public static void main(String arg[]) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException {
		SimpleJSAP jsap = new SimpleJSAP(
		    BitStreamArcLabelledImmutableGraphAsIntTriples.class.getName(),
		    "expose a BitStreamArcLabelledImmutableGraph as int triples",
		    new Parameter[] {
		        new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY,
		                             "The basename of the graph" ),
		        new Switch ( "profile", 'p', "profile", "Profile the graph before and after the updates" ),
		        new FlaggedOption( "interval", JSAP.INTEGER_PARSER, Integer.toString( 2 ), JSAP.REQUIRED, 'i', "interval",
		                           "The number of months since June/2006" ),
		        new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval",
		                           "The minimum time interval between activity logs in milliseconds." ),
		    }
		);

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		final int updateInterval = jsapResult.getInt( "interval" );
		if ( updateInterval > 0 && updateInterval < 12 ) {
			System.out.println( "# extracting updates for [" + Integer.toString(updateInterval) + "] months" );
		} else {
			System.out.println("[ERROR]: invalid updateInterval = [" + Integer.toString(updateInterval) + "]" );
			System.exit(1);
		}

		final ProgressLogger pl = new ProgressLogger();
		pl.logInterval = jsapResult.getLong( "logInterval" );
		final String basename = jsapResult.getString( "basename" );
		final BitStreamArcLabelledImmutableGraph graph;
		graph = BitStreamArcLabelledImmutableGraph.load( basename, pl );

		final int n = graph.numNodes();
		final long m = graph.numArcs();
		System.out.println( "# done loading graph, |V| = " + Integer.toString(n) + " |E| = " + Long.toString(m) );

		// try {
		// 	RandomAccessFile tripleListStream = new RandomAccessFile( basename + ".itv-" + Integer.toString(updateInterval), "rw" );
		// 	FileChannel outChannel = tripleListStream.getChannel();
		// } catch (Exception excp) { System.out.println( excp ); }

		// ArcLabelledNodeIterator nodeIter;
		ArcLabelledNodeIterator.LabelledArcIterator succIter;

		long cnt1 = 0;
		long ttlArcCnt = 0;
		long arcCnt[] = new long[12];

		long arcIncCntAL[] = new long[11];
		long arcDecCntAL[] = new long[11];
		long arcIncCntAF[] = new long[11];
		long arcDecCntAF[] = new long[11];

		long arc01, arc10, arc010, arcOthers;
		arc01 = arc10 = arc010 = arcOthers = 0;

		int nodeIncCntAL[] = new int[11];
		int nodeDecCntAL[] = new int[11];
		int nodeIncCntAF[] = new int[11];
		int nodeDecCntAF[] = new int[11];

		int nodeCnt[] = new int[12];


		for ( int v = 0; v < n; v++ ) {
			// nodeIter = graph.nodeIterator( v );
			int deg = graph.outdegree( v );
			succIter = graph.successors( v );
			// int prevLabel = -1; int prevVtx = -1;
			int prevBit = 0; int firstBit = 0; int nodeLabel = 0;
			while ( deg-- != 0 ) {
				ttlArcCnt++;
				if ( ttlArcCnt % 100000000 == 0 ) {
					System.out.println("processed " + Long.toString(ttlArcCnt / 100000000) + "00M arcs");
				}
				int succ = succIter.nextInt();
				int arcLabel = succIter.label().getInt();
				// int intLabel = succIter.label().getInt();

				nodeLabel = nodeLabel | arcLabel;
				firstBit = prevBit = arcLabel & 1;
				if ( firstBit != 0 ) { arcCnt[0]++; }

				if ( arcLabel != 0 ) {
					int lastBit = arcLabel & (1 << 11);
					int highestBitOfst = 31 - Integer.numberOfLeadingZeros(arcLabel);
					int lowestBitOfst = Integer.numberOfTrailingZeros(arcLabel);
					int bitNumInBetween = highestBitOfst - lowestBitOfst + 1;
					if (bitNumInBetween == Integer.bitCount(arcLabel)) {
						if (firstBit == 0 && lastBit == 0) {
							arc010++;
						} else if (firstBit == 0) {
							arc01++;
						} else if (lastBit == 0) {
							arc10++;
						}
					} else { arcOthers++; }
				}

				for ( int i = 1; i < 12; i++ ) {
					int bit = (arcLabel >> i) & 1;
					if ( bit != 0 ) {
						arcCnt[i]++;
						if (prevBit == 0) { arcIncCntAL[i - 1]++; }
						if (firstBit == 0) { arcIncCntAF[i - 1]++; }
					} else { // bit == 0
						if (prevBit != 0) { arcDecCntAL[i - 1]++; }
						if (firstBit != 0) { arcDecCntAF[i - 1]++; }
					}
					prevBit = bit;
				}

				// int oneBit;
				// if (prevLabel != -1 && intLabel != prevLabel) {
				// 	System.out.println("NOT all labels from a source node are the same!");
				// 	System.out.println("e1 : " + Integer.toString(v) + ", " + Integer.toString(prevVtx) + "," + Integer.toBinaryString(prevLabel));
				// 	System.out.println("e1 : " + Integer.toString(v) + ", " + Integer.toString(succ) + "," + Integer.toBinaryString(intLabel));
				// 	System.exit(1);
				// }
				// prevLabel = intLabel; prevVtx = succ;
				// if (ttlArcCnt == 1) { System.out.println("label.spec = \n\t" + succIter.label().toSpec()); }
				// if ( (intLabel & 1) != 0 ) { cnt1++; }
				// // System.out.println("\t(" + Integer.toString(v) + ", " + Integer.toString(succ) + ", " + Integer.toBinaryString(intLabel) + ")");
				// while ((oneBit = Integer.highestOneBit(intLabel)) != 0) {
				// 	int ofst = 31 - Integer.numberOfLeadingZeros(oneBit);
				// 	if (ofst >= 12) {
				// 		System.out.println("[ERROR]: label bit ofst too large : " + Integer.toString(ofst));
				// 		System.exit(1);
				// 	}
				// 	arcCnt[ofst]++;
				// 	intLabel = intLabel & ~oneBit;
				// }
			}

			prevBit = firstBit = nodeLabel & 1;
			if ( firstBit != 0 ) { nodeCnt[0]++; }
			for ( int i = 1; i < 12 ; i++ ) {
				int bit = (nodeLabel >> i) & 1;
				if ( bit != 0 ) {
					nodeCnt[i]++;
					if (prevBit == 0) { nodeIncCntAL[i - 1]++; }
					if (firstBit == 0) { nodeIncCntAF[i - 1]++; }
				} else { // bit == 0
					if (prevBit != 0) { nodeDecCntAL[i - 1]++; }
					if (firstBit != 0) { nodeDecCntAF[i - 1]++; }
				}
				prevBit = bit;
			}

		}
		System.out.println("Finished reading arc labels!");
		// for ( int k = 0; k < 12; k++ ) {
		// 	System.out.println("\tArcCount[" + Integer.toString(k) + "] = " + Long.toString(arcCnt[k]));
		// }
		System.out.println("Printing (ttl_vtx, ttl_edge, dv+(f), dv-(f), de+(f), de-(f), dv+(l), dv-(l), de+(l), de-(l))");
		for ( int k = 0; k < 12 ; k++ ) {
			System.out.println("# Month-" + Integer.toString(k) + " :");
			System.out.println("\t- Graph size |V|: " + Integer.toString(nodeCnt[k]) + " |E|: " + Long.toString(arcCnt[k]));
			if ( k == 0 ) { continue; }
			System.out.println("\t- Delta G(first) size |V+|: " +
			                   Integer.toString(nodeIncCntAF[k - 1]) + " |V-|: " + Integer.toString(nodeDecCntAF[k - 1]) +
			                   " |E+|: " + Long.toString(arcIncCntAF[k - 1]) + " |E-|: " + Long.toString(arcDecCntAF[k - 1]) );
			System.out.println("\t- Delta G(last) size |V+|: " +
			                   Integer.toString(nodeIncCntAL[k - 1]) + " |V-|: " + Integer.toString(nodeDecCntAL[k - 1]) +
			                   " |E+|: " + Long.toString(arcIncCntAL[k - 1]) + " |E-|: " + Long.toString(arcDecCntAL[k - 1]) );
		}
		System.out.println("Edges of bits 1*0*: " + Long.toString(arc10));
		System.out.println("Edges of bits 0*1*: " + Long.toString(arc01));
		System.out.println("Edges of bits 0*1*0*: " + Long.toString(arc010));
		System.out.println("Edges of other bits: " + Long.toString(arcOthers));
	}
}