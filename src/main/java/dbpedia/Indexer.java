
package dbpedia;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.out.SinkTripleOutput;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.SyntaxLabels;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class Indexer {
    
    private HashMap hm = new HashMap();
    
    public void index() {
        String dir = "/Users/Milan/Downloads/db-abstracts-en/";
        File folder = new File(dir);
        File[] listOfFiles = folder.listFiles();
        
        Model model = ModelFactory.createDefaultModel();
        
//        FileOutputStream fos = null;
        boolean go = false;
        try {
//            fos = new FileOutputStream("abstracts-en-index.tsv");
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    System.out.println("File " + listOfFiles[i].getName());
//                    if(listOfFiles[i].getName().equals("abstracts_en84.ttl")) {
                        go = true;
//                    }
                    if(go && listOfFiles[i].getName().endsWith(".ttl")) {
//                        Sink<Triple> output = new SinkTripleOutput(fos, null, SyntaxLabels.createNodeToLabel());
//                        Sink<Triple> output = new SinkTripleOutput(System.out, null, SyntaxLabels.createNodeToLabel());
                        PrintWriter dest = new PrintWriter(new BufferedWriter(new FileWriter(listOfFiles[i].getName()+".txt", true)));
                        StreamRDF filtered = new FilterSinkTSV(dest,
                                listOfFiles[i].getName(),
                                model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#isString")) ;
                        String fileLoc = dir+listOfFiles[i].getName();
                        RDFDataMgr.parse(filtered, fileLoc) ;
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AbstractsAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
//            try {
////                fos.close();
//            } catch (IOException ex) {
//                Logger.getLogger(AbstractsAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
//            }
        }    
    }
    
    public void readIndex() {
        try {
            BufferedReader br = null;
            String path = "/Users/Milan/Documents/programming/dbpedia-labels-stats/index/index.tsv";
            br = new BufferedReader(new FileReader(path));
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                String[] parts = line.split("\t");
                String doc = parts[0];
                String article = parts[1];
                getHm().put(article, doc);
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * @return the hm
     */
    public HashMap getHm() {
        return hm;
    }

    /**
     * @param hm the hm to set
     */
    public void setHm(HashMap hm) {
        this.hm = hm;
    }

}
