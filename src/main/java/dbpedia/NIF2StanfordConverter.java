
package dbpedia;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
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
 http://dojchinovski.mk
 */
public class NIF2StanfordConverter {
    
    public void convertAll(String dataLoc) {
//        String dir = "/Users/Milan/Downloads/db-abstracts-en/";
        File folder = new File(dataLoc+"dbpedia-abstracts/");
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                if(listOfFiles[i].getName().endsWith(".ttl")) {
                    System.out.println("File " + dataLoc+"dbpedia-abstracts/"+listOfFiles[i].getName());
//                    convertOneFile(dataLoc+"dbpedia-abstracts/"+listOfFiles[i].getName(), dataLoc);
                }
            }
        }
    }
    
    
    public void convertOneFile(String fileLoc, String dataLoc) {
            
            
            
//            String dir = "/Users/Milan/Downloads/db-abstracts-en/abstracts_en0.ttl";
            DBpediaOntologyHelper dbp = DBpediaOntologyHelper.getInstance(dataLoc);
            DBpediaQuery dbQuery = DBpediaQuery.getInstance(dataLoc);
            
            Model model = RDFDataMgr.loadModel(fileLoc);
            System.out.println(fileLoc);
            StmtIterator ctxtIter = model.listStatements(null, RDF.type, model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#Context"));
            
            while(ctxtIter.hasNext()) {
                PrintWriter out = null;
                HashMap<String,String> hm = new HashMap();
                
                Statement ctxtStm = ctxtIter.nextStatement();
                Resource ctxtRes = ctxtStm.getSubject();
                try {
                    String docId = ctxtRes.getURI().split("/")[ctxtRes.getURI().split("/").length-2];
                    System.out.println(docId);
                    out = new PrintWriter(new BufferedWriter(new FileWriter(dataLoc+"train-data/"+docId, true)));
//                    out = new PrintWriter(new BufferedWriter(new FileWriter("/Users/Milan/Documents/research/repositories/dbpedia-abstracts-processor/train-data/"+docId, true)));
                    String ctxtStr = ctxtRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#isString")).getString();
                    StmtIterator entityIter = model.listStatements(null, model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#referenceContext"), ctxtRes);
                    while(entityIter.hasNext()) {
                        Statement entityStm = entityIter.nextStatement();
                        Resource entityRes = entityStm.getSubject();
                        int begin = entityRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex")).getInt();
                        int end = entityRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex")).getInt();
                        String anchor = entityRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#anchorOf")).getString();
                        
                        String link = null;
                        Statement linkStm = entityRes.getProperty(model.getProperty("http://www.w3.org/2005/11/its/rdf#taIdentRef"));
                        if(linkStm != null) {
                            link = linkStm.getObject().asResource().getURI();
                        }
                        
                        PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<>(new StringReader(anchor),
                        new CoreLabelTokenFactory(), "");
                        while (ptbt.hasNext()) {
                            CoreLabel label = ptbt.next();
                            int beginIndex = begin+label.beginPosition();
                            int endIndex = begin+label.endPosition();
                            hm.put(label+":"+beginIndex+":"+endIndex, link);
                        }
                    }

                    PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<>(new StringReader(ctxtStr), new CoreLabelTokenFactory(), "");
                    while (ptbt.hasNext()) {
//                        System.out.println("entity");
                        CoreLabel label = ptbt.next();
                        if(hm.containsKey(label+":"+label.beginPosition()+":"+label.endPosition())) {
//                            System.out.println(label+":"+label.beginPosition()+":"+label.endPosition()+":ENTITY");

                            out.write(label+"\t"+dbp.getCoarseGrainedType(dbQuery.getType(hm.get(label+":"+label.beginPosition()+":"+label.endPosition())))+"\n");
//                            out.write(label+"\t"+"ENTITY\n");
                        } else {
//                            System.out.println(label+":"+label.beginPosition()+":"+label.beginPosition()+":0");
                            out.write(label+"\t"+"O\n");
                        }
                    }
                } catch (Exception ex) {
                    System.out.println("problem:" + ex.getMessage());
                    System.out.println("problem:" + ex.fillInStackTrace());
                } finally {
                    out.close();
                }
            }

    }

    public void convert() {
        PrintWriter out = null;
        try {
//            while (ptbt.hasNext()) {
//                CoreLabel label = ptbt.next();
////                label.
//                System.out.println(label);
//                System.out.println(label.beginPosition());
//                System.out.println(label.endPosition());
//            }
//            String myString = "hey, how are you, U.S. today.";
//            String[] parts = myString.split("(?=[,.])|\\\\s+");
//            for(String s : parts) {
//                System.out.println(s);
//            }
            out = new PrintWriter(new BufferedWriter(new FileWriter("out.tsv", true)));
//            out.println("the text");
//            out.close();
//            
            String dir = "/Users/Milan/Downloads/db-abstracts-en/small.ttl";
            
            Model model = RDFDataMgr.loadModel(dir) ;
            StmtIterator ctxtIter = model.listStatements(null, RDF.type, model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#Context"));
            
            while(ctxtIter.hasNext()) {
                
                HashMap<String,String> hm = new HashMap();
                
                Statement ctxtStm = ctxtIter.nextStatement();
                Resource ctxtRes = ctxtStm.getSubject();
                String ctxtStr = ctxtRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#isString")).getString();
                StmtIterator entityIter = model.listStatements(null, model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#referenceContext"), ctxtRes);
                while(entityIter.hasNext()) {
                    Statement entityStm = entityIter.nextStatement();
                    Resource entityRes = entityStm.getSubject();
                    int begin = entityRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex")).getInt();
                    int end = entityRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex")).getInt();
                    String anchor = entityRes.getProperty(model.getProperty("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#anchorOf")).getString();
//                System.out.println(entityRes);
//                System.out.println(begin);
//                System.out.println(end);
//                System.out.println(anchor);
                    
                    PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<>(new StringReader(anchor),
                    new CoreLabelTokenFactory(), "");
                    while (ptbt.hasNext()) {
                        CoreLabel label = ptbt.next();
//                        System.out.println(label);
                        int beginIndex = begin+label.beginPosition();
                        int endIndex = begin+label.endPosition();
//                        System.out.println(label.beginPosition());
//                        System.out.println(label.endPosition());
                        hm.put(label+":"+beginIndex+":"+endIndex, "YES");
//                        System.out.println(label+":"+beginIndex+":"+endIndex);
                    }
                            
//                    String[] splitStringArray = anchor.split(" ");
//                    
//                    int offset = -1;
//                    for(String s: splitStringArray) {
//                        offset = anchor.indexOf(s, offset + 1); // avoid duplicates
//                        if(!s.equals("")) {
////                        System.out.println(s);
//                            int beginIndex = begin+offset;
//                            int endIndex = begin+offset+s.length();
////                        System.out.println(s+":"+beginIndex+":"+endIndex+":ENTITY");
//                            hm.put(s+":"+beginIndex+":"+endIndex, "YES");
//                        }
//                    }
                }
                
                    PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<>(new StringReader(ctxtStr),
                    new CoreLabelTokenFactory(), "");
                    while (ptbt.hasNext()) {
                        CoreLabel label = ptbt.next();
//                        System.out.println(label);
//                        System.out.println(label.beginPosition());
//                        System.out.println(label.endPosition());
                        if(hm.containsKey(label+":"+label.beginPosition()+":"+label.endPosition())) {
                            System.out.println(label+":"+label.beginPosition()+":"+label.endPosition()+":ENTITY");
                            out.write(label+"\t"+"ENTITY\n");
                        } else {
                            System.out.println(label+":"+label.beginPosition()+":"+label.beginPosition()+":0");
                            out.write(label+"\t"+"O\n");
                        }
                    }
//                System.out.println("printing");
//                String[] splitStringArray = ctxtStr.split("(?=[,.])|\\s+");
//                int offset = -1;
//                for(String s: splitStringArray) {
//                    offset = ctxtStr.indexOf(s, offset + 1); // avoid duplicates
//                    if(!s.equals("")) {
////                        System.out.println(s);
//                        int beginIndex = offset;
//                        int endIndex = offset+s.length();
//                        if(hm.containsKey(s+":"+beginIndex+":"+endIndex)) {
//                            System.out.println(s+":"+beginIndex+":"+endIndex+":ENTITY");
////                            out.write
//                        } else {
////                        System.out.println(s+":"+beginIndex+":"+endIndex+":0");
//                        }
//                    }
//                }
                
            }
        } catch (Exception ex) {
            Logger.getLogger(NIF2StanfordConverter.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            out.flush();
            out.close();
        }
    }    
    
    
}
