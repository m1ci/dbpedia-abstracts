package dbpedia;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.riot.system.StreamRDFBase;


/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class FilterSinkRDF extends StreamRDFBase
    {
        private final Node[] properties ;
        // Where to send the filtered triples.
        private final Sink<Triple> dest ;
       
        Model model = ModelFactory.createDefaultModel();
        public Resource previousRes = null;

        FilterSinkRDF(Sink<Triple> dest, Property... properties)
        {
            this.dest = dest ;
            this.properties = new Node[properties.length] ;
            for ( int i = 0 ; i < properties.length ; i++ ) 
                this.properties[i] = properties[i].asNode() ;
        }

        @Override
        public void triple(Triple triple)
        {
            for ( Node p : properties )
            {
                if ( triple.getPredicate().equals(p) ) {
//                    if(triple.getPredicate().equals(model.getProperty("http://www.w3.org/2005/11/its/rdf#taIdentRef"))) {
                        dest.send(triple) ;
//                    }
                }
            }
        }
        
        @Override
        public void finish()
        {
            // Output may be buffered.
            dest.flush() ;
        }
}
