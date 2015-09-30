
package dbpedia;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.riot.RDFDataMgr;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class PairCountsProcessor {
    
    public void process(String dataLoc) {
        
        PrintWriter out = null;
        try {
            //        String dir = "/Users/Milan/Downloads/db-abstracts-en/";
            out = new PrintWriter(new BufferedWriter(new FileWriter(dataLoc+"train-data/pairCounts", true)));
            HashMap<String,Occurrence> hm = new HashMap();
            File folder = new File(dataLoc+"dbpedia-abstracts/");
            File[] listOfFiles = folder.listFiles();
            System.out.println(dataLoc);
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    if(listOfFiles[i].getName().endsWith(".ttl")) {
                        System.out.println("File " + dataLoc+"dbpedia-abstracts/"+listOfFiles[i].getName());
                        convertOneFile(dataLoc+"dbpedia-abstracts/"+listOfFiles[i].getName(), dataLoc, hm);
                    }
                }
            }
            
            Iterator it = hm.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
//                System.out.println(pair.getKey() + " = " + pair.getValue());
                Occurrence occ = (Occurrence)pair.getValue();
                out.write(occ.getLabel()+"\t"+occ.getLink()+"\t"+occ.getCount()+"\n");
                it.remove(); // avoids a ConcurrentModificationException
            }
        } catch (IOException ex) {
            Logger.getLogger(PairCountsProcessor.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            out.close();
        }
    }
    
    public void convertOneFile(String fileLoc, String dataLoc, HashMap<String,Occurrence> hm) {
            
            Model model = RDFDataMgr.loadModel(fileLoc);
            System.out.println(fileLoc);
            StmtIterator ctxtIter = model.listStatements(null, RDF.type, model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#Context"));
            
            while(ctxtIter.hasNext()) {
                PrintWriter out = null;
                
                Statement ctxtStm = ctxtIter.nextStatement();
                Resource ctxtRes = ctxtStm.getSubject();
                try {
//                    String docId = ctxtRes.getURI().split("/")[ctxtRes.getURI().split("/").length-2];
//                    String[] splitParts = ctxtRes.getURI().split("dbpedia.org/resource/");
//                    String[] secondParts = splitParts[1].split("/abstract");
//                    String docId = secondParts[0];
//                    docId = docId.replaceAll("/", "_");
                    
//                    System.out.println(docId);
//                    out = new PrintWriter(new BufferedWriter(new FileWriter(dataLoc+"train-data/"+docId, true)));
                    StmtIterator entityIter = model.listStatements(null, model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#referenceContext"), ctxtRes);

                    
                    while(entityIter.hasNext()) {
                        Statement entityStm = entityIter.nextStatement();
                        Resource entityRes = entityStm.getSubject();

                        String anchor = entityRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#anchorOf")).getString();
                        
                        String link = null;
                        Statement linkStm = entityRes.getProperty(model.getProperty("http://www.w3.org/2005/11/its/rdf#taIdentRef"));
                        if(linkStm != null) {
                            link = linkStm.getObject().asResource().getURI();
                            if(hm.containsKey(anchor+":"+link)) {
                                Occurrence occ = hm.get(anchor+":"+link);
                                int counter = occ.getCount();
                                counter++;
                                occ.setCount(counter);
//                                System.out.println(occ.getLabel()+":"+ occ.getLink()+":"+occ.getCount());
                                hm.put(anchor+":"+link, occ);
                            } else {
                                // first occurrence
                                Occurrence occ = new Occurrence();
                                occ.setLabel(anchor);
                                occ.setLink(link);
                                occ.setCount(1);
                                hm.put(anchor+":"+link, occ);
                            }
                        }
                        
                    }

                } catch (Exception ex) {
                    System.out.println("problem:" + ex.getMessage());
                    System.out.println("problem:" + ex.fillInStackTrace());
                } finally {
                    try {
//                        out.close();
                    } catch (Exception ex) {
                        System.out.println("problem2:" + ex.getMessage());
                        System.out.println("problem2:" + ex.fillInStackTrace());
                    
                    }
                }
            }

    }
    
    public class Occurrence {
        
        private int count;
        private String label;
        private String link;

        /**
         * @return the count
         */
        public int getCount() {
            return count;
        }

        /**
         * @param count the count to set
         */
        public void setCount(int count) {
            this.count = count;
        }

        /**
         * @return the label
         */
        public String getLabel() {
            return label;
        }

        /**
         * @param label the label to set
         */
        public void setLabel(String label) {
            this.label = label;
        }

        /**
         * @return the link
         */
        public String getLink() {
            return link;
        }

        /**
         * @param link the link to set
         */
        public void setLink(String link) {
            this.link = link;
        }
        
    }

}
