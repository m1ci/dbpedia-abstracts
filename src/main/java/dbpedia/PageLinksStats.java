
package dbpedia;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class PageLinksStats {
    public static void main(String[] args) {
        System.out.println("started");
//        new PageLinksProcessor().processOutLinksCounts("en","/Users/Milan/Downloads/page-links_fr.hdt");
        new PageLinksProcessor().processInLinksCounts("en","/home/milan/tmp/pagelinks/merged_ru.hdt");
        System.out.println("finished");
    }
}
