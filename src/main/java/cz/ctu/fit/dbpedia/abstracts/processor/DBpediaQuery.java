
package dbpedia;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdtjena.HDTGraph;
/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class DBpediaQuery {
    
    private static DBpediaQuery instance = null;
    private static HDT hdt;
    private static HDTGraph graph;
    private static Model model;
    
    public static DBpediaQuery getInstance(String dataLoc) {
        if(instance == null) {
            try {
                instance = new DBpediaQuery();
                hdt = HDTManager.mapIndexedHDT(dataLoc+"instance-types_en.hdt", null);
                graph = new HDTGraph(hdt);
                model = ModelFactory.createModelForGraph(graph);
            } catch (IOException ex) {
                Logger.getLogger(DBpediaQuery.class.getName()).log(Level.SEVERE, null, ex);
            }

           
        }
        return instance;
    
    }
    
    public String getType(String resource) {
        String type = null;
        if(resource == null)
            return null;
        try{
            
            StmtIterator iter = model.listStatements(model.getResource(resource), null, (RDFNode) null);
            
            while(iter.hasNext()) {
                Statement stm = iter.nextStatement();
                Resource object = stm.getObject().asResource();
                return object.getURI();
                
            }
            
        }catch (Exception ex) {
        
        }
        return type;

    }
    
    
}
