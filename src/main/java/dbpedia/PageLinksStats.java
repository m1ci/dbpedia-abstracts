
package dbpedia;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class PageLinksStats{ 
    public static void main(String[] args) {
        System.out.println("started");
//        new PageLinksProcessor().processOutLinksCounts("en","/Users/Milan/Downloads/page-links_fr.hdt");
        new PageLinksProcessor().processInLinksCounts("/home/milan/tmp/pagelinks/merged_ru.ttl");
        System.out.println("finished");
    }
}
