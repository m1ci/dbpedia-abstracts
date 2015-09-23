
package dbpedia;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.RDF;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.out.SinkTripleOutput;
import org.apache.jena.riot.system.SyntaxLabels;
import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.atlas.lib.Sink ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.out.SinkTripleOutput ;
import org.apache.jena.riot.system.StreamRDF ;
import org.apache.jena.riot.system.StreamRDFBase ;
import org.apache.jena.riot.system.SyntaxLabels ;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class AbstractsAnalyzer {
    public void buildLabelCounts() {
        String dir = "/Users/Milan/Downloads/db-abstracts-en/";
        File folder = new File(dir);
        File[] listOfFiles = folder.listFiles();
        
        Model model = ModelFactory.createDefaultModel();
        
        FileOutputStream fos = null;
        boolean go = false;
        try {
            fos = new FileOutputStream("out.nt");
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    System.out.println("File " + listOfFiles[i].getName());
//                    if(listOfFiles[i].getName().equals("abstracts_en84.ttl")) {
                        go = true;
//                    }
                    if(go && listOfFiles[i].getName().endsWith(".ttl")) {
                        Sink<Triple> output = new SinkTripleOutput(fos, null, SyntaxLabels.createNodeToLabel());
    //                    Sink<Triple> output = new SinkTripleOutput(System.out, null, SyntaxLabels.createNodeToLabel());
                        StreamRDF filtered = new FilterSinkRDF(output,
                                model.getProperty("http://www.w3.org/2005/11/its/rdf#taIdentRef"),
                                model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#anchorOf")) ;
                        String fileLoc = dir+listOfFiles[i].getName();
                        // Call the parsing process.
                        RDFDataMgr.parse(filtered, fileLoc) ;
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AbstractsAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                fos.close();
            } catch (IOException ex) {
                Logger.getLogger(AbstractsAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
