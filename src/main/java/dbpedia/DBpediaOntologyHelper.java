
package dbpedia;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import static org.rdfhdt.hdt.enums.RDFNotation.RDFXML;

/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class DBpediaOntologyHelper {
    private static DBpediaOntologyHelper instance = null;
    private static Model model;
    
    public static DBpediaOntologyHelper getInstance(String dataLoc) {
        
        if(instance == null) {
            model = RDFDataMgr.loadModel(dataLoc+"dbpedia_2015-04.owl",org.apache.jena.riot.Lang.RDFXML);
            System.out.println("ontology loaded..." + model.size());
            instance = new DBpediaOntologyHelper();
        }
        return instance;
    }
    
    public String getCoarseGrainedType(String type) {
        if(type == null) {
            return "MISC";
        }
//        String coarseType = "";
        
        Resource resource = model.getResource(type);
        
        while(!resource.getURI().equals("http://www.w3.org/2002/07/owl#Thing")) {
            Resource superClass = resource.getProperty(model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")).getObject().asResource();
            System.out.println(superClass);
            resource = superClass;
            switch (superClass.getURI()) {
                case "http://dbpedia.org/ontology/Place":
                    return "LOC";
                case "http://dbpedia.org/ontology/Person":
                    return "PER";
                case "http://dbpedia.org/ontology/Work":
                    return "WOR";
                case "http://dbpedia.org/ontology/Species":
                    return "SPP";
                case "http://dbpedia.org/ontology/Organisation":
                    return "ORG";
                case "http://dbpedia.org/ontology/Event":
                    return "EVN";
            }
        }
        return "MISC";
    }
}
