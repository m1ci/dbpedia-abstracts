
package dbpedia;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class TrainDataCreator {
    public void create(HashMap hm) {
        try {
            BufferedReader br = null;
            String path = "/Users/Milan/Documents/programming/dbpedia-labels-stats/counts/event.tsv";
            br = new BufferedReader(new FileReader(path));
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                // process the line.
                String[] parts = line.split("\t");
                String article = parts[0];
                article = article.substring(1,article.length()-1);
                System.out.println(article);
                if(hm.containsKey(article)) {
                    String loc = hm.get(article).toString();
                    System.out.println(loc);
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
