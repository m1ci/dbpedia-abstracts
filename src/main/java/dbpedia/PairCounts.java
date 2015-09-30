
package dbpedia;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class PairCounts{ 
    public static void main(String[] args) {
        System.out.println("started pairCounts");
        String loc = args[0];
        new PairCountsProcessor().process(loc);
        System.out.println("finished");
    }
}
