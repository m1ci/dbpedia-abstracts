
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
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDFS;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class PageLinksProcessor {
    public void processOutLinksCounts(String lang, String dataLoc) {
        try {
            HDT hdt;
            HDTGraph graph;
            Model model;
            
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(dataLoc+".outlinks", true)));
            
            hdt = HDTManager.mapIndexedHDT(dataLoc, null);
            graph = new HDTGraph(hdt);
            model = ModelFactory.createModelForGraph(graph);
            String sparql = "SELECT ?subj (COUNT(?subj) AS ?count ) WHERE { " +
                        "?subj  <http://dbpedia.org/ontology/wikiPageWikiLink> ?obj . "
                    + "} GROUP BY ?subj ORDER BY DESC(?count)";

            Query qry = QueryFactory.create(sparql);
            QueryExecution qe = QueryExecutionFactory.create(qry, model);
            ResultSet rs = qe.execSelect();

            while(rs.hasNext())
            {
                QuerySolution sol = rs.nextSolution();
                RDFNode subj = sol.get("subj"); 
                RDFNode count = sol.get("count"); 
                String[] splitParts = subj.asResource().getURI().split("dbpedia.org/resource/");
                String docId = splitParts[1];
                docId = docId.replaceAll("/", "_");
                System.out.println(docId);
                System.out.println(count.asLiteral().getInt());
                out.write(docId+"\t"+count.asLiteral().getInt()+"\n");
                out.flush();
            }
            out.close();

            qe.close(); 
        } catch (IOException ex) {
            Logger.getLogger(PageLinksProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void processInLinksCounts(String dataLoc) {
        try {
            HDT hdt;
            HDTGraph graph;
            Model model;
            int totalCounter = 0;
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(dataLoc+".inlink", true)));
            
            hdt = HDTManager.mapIndexedHDT(dataLoc, null);
            graph = new HDTGraph(hdt);
            model = ModelFactory.createModelForGraph(graph);
            
            HashMap<String, Instance> hm = new HashMap();

            System.out.println("data loaded");
            
            String sparql = "SELECT ?subj ?obj WHERE { " +
                        "?subj  <http://dbpedia.org/ontology/wikiPageWikiLink> ?obj . "
                    + "} ";
            System.out.println("now query");
            Query qry = QueryFactory.create(sparql);
            QueryExecution qe = QueryExecutionFactory.create(qry, model);
            ResultSet rs = qe.execSelect();
            System.out.println("listing");

            while(rs.hasNext()) {
                QuerySolution sol = rs.nextSolution();
                Resource subj = sol.get("subj").asResource();
                Resource obj = sol.get("obj").asResource();
                
                if(!hm.containsKey(subj.getURI())) {
                    Instance i = new Instance();
                    i.setCount(1);
                    i.setLink(subj.getURI());
                    hm.put(subj.getURI(), i);
                }
                
                if(!hm.containsKey(obj.getURI())) {
                    Instance i = new Instance();
                    i.setCount(1);
                    i.setLink(obj.getURI());
                    hm.put(obj.getURI(), i);
                }
//                System.out.println(obj);
                
//                StmtIterator iter = model.listStatements(null, model.getProperty("http://dbpedia.org/ontology/wikiPageWikiLink"), obj.asResource());
//                int count = 0;
//                while(iter.hasNext()) {
//                    iter.nextStatement();
//                    count++;
//                    totalCounter++;
//                }
//
//                StmtIterator labelIter = model.listStatements(obj.asResource(), RDFS.label, (RDFNode)null);
//                if(labelIter.hasNext()) {
////                    myobj.setLabel(labelIter.nextStatement().getObject().asLiteral().toString());
//                    out.write(labelIter.nextStatement().getObject().asLiteral().getValue()+"\t"+obj.asResource().getURI()+"\t"+count+"\n");
//                    out.flush();
//                }
            }
            System.out.println("size: " + hm.size());
            Iterator it = hm.entrySet().iterator();
            
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();                
//                System.out.println(pair.getKey() + " = " + pair.getValue());
                Instance i = (Instance)pair.getValue();
                StmtIterator iter = model.listStatements(null, model.getProperty("http://dbpedia.org/ontology/wikiPageWikiLink"), model.getResource(i.getLink()));
                while(iter.hasNext()) {
                    iter.nextStatement();
                    i.setCount(i.getCount()+1);
                }
                it.remove(); // avoids a ConcurrentModificationException
                out.write(i.getLink()+"\t"+i.getCount()+"\n");
            }
            System.out.println(totalCounter);
            out.close();

            qe.close(); 
        } catch (IOException ex) {
            Logger.getLogger(PageLinksProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void processInLinksCountsBK(String lang, String dataLoc) {
        try {
            HDT hdt;
            HDTGraph graph;
            Model model;
            int totalCounter = 0;
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(dataLoc+".inlink", true)));
            
            hdt = HDTManager.mapIndexedHDT(dataLoc, null);
            graph = new HDTGraph(hdt);
            model = ModelFactory.createModelForGraph(graph);

            System.out.println("data loaded");
//            HDT hdtLabels = HDTManager.mapIndexedHDT("", null);
//            HDTGraph graphLabels = new HDTGraph(hdtLabels);
//            Model modelLabels = ModelFactory.createModelForGraph(graphLabels);
            
           String sparql = "SELECT distinct ?obj WHERE { " +
                        "?subj  <http://dbpedia.org/ontology/wikiPageWikiLink> ?obj . "
                    + "} ";
            System.out.println("now query");
            Query qry = QueryFactory.create(sparql);
            QueryExecution qe = QueryExecutionFactory.create(qry, model);
            ResultSet rs = qe.execSelect();
            System.out.println("listing");

//            ArrayList<MyObject> list = new ArrayList();
            
            while(rs.hasNext()) {
                QuerySolution sol = rs.nextSolution();
                RDFNode obj = sol.get("obj");
//                System.out.println(obj);
                
                StmtIterator iter = model.listStatements(null, model.getProperty("http://dbpedia.org/ontology/wikiPageWikiLink"), obj.asResource());
                int count = 0;
                while(iter.hasNext()) {
                    iter.nextStatement();
                    count++;
                    totalCounter++;
                }
                
//                String[] splitParts = obj.asResource().getURI().split("dbpedia.org/resource/");
//                String docId = splitParts[1];
                
//                MyObject myobj = new MyObject();
//                myobj.setUri(docId);
//                myobj.setCount(count);

                StmtIterator labelIter = model.listStatements(obj.asResource(), RDFS.label, (RDFNode)null);
                if(labelIter.hasNext()) {
//                    myobj.setLabel(labelIter.nextStatement().getObject().asLiteral().toString());
                    out.write(labelIter.nextStatement().getObject().asLiteral().getValue()+"\t"+obj.asResource().getURI()+"\t"+count+"\n");
                    out.flush();
                }
                
//                docId = docId.replaceAll("/", "_");
                
//                list.add(myobj);
            }
            System.out.println(totalCounter);
//            System.out.println("sorting");
//            Collections.sort(list, new CustomComparator());
//            System.out.println("finished sorting");
            
//            for(MyObject m : list) {
//                System.out.println(m.getLabel());
//                System.out.println(m.getUri());
//                System.out.println(m.getCount());
//                
//                if(m.getLabel() != null) {
//                    out.write(m.getLabel()+"\t"+m.getUri()+"\t"+m.getCount()+"\n");
//                    out.flush();
//                }
//            
//            }
                
//            String sparql = "SELECT ?obj (COUNT(?obj) AS ?count ) WHERE { " +
//                        "?subj  <http://dbpedia.org/ontology/wikiPageWikiLink> ?obj . "
//                    + "} GROUP BY ?obj ORDER BY DESC(?count)";
//
//            Query qry = QueryFactory.create(sparql);
//            QueryExecution qe = QueryExecutionFactory.create(qry, model);
//            ResultSet rs = qe.execSelect();
//
//            while(rs.hasNext())
//            {
//                QuerySolution sol = rs.nextSolution();
//                RDFNode subj = sol.get("obj"); 
//                RDFNode count = sol.get("count"); 
//                String[] splitParts = subj.asResource().getURI().split("dbpedia.org/resource/");
//                String docId = splitParts[1];
//                docId = docId.replaceAll("/", "_");
//                System.out.println(docId);
//                System.out.println(count.asLiteral().getInt());
//                out.write(docId+"\t"+count.asLiteral().getInt()+"\n");
//                out.flush();
//            }
            out.close();

            qe.close(); 
        } catch (IOException ex) {
            Logger.getLogger(PageLinksProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public class CustomComparator implements Comparator<MyObject> {
        @Override
        public int compare(MyObject o1, MyObject o2) {
            if(o1.getCount() > o2.getCount())
                return -1;

            if(o1.getCount() == o2.getCount())
                return 0 ;

            if(o1.getCount() < o2.getCount())
                return 1 ;

            return 0;
        }
    }
    
    public class MyObject {
    
        private String uri;
        private int count;
        private String label;

        /**
         * @return the uri
         */
        public String getUri() {
            return uri;
        }

        /**
         * @param uri the uri to set
         */
        public void setUri(String uri) {
            this.uri = uri;
        }

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
    }
    
    public class Instance {
        private int count;
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
