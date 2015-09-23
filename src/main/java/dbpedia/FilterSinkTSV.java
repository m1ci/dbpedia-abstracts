package dbpedia;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.riot.system.StreamRDFBase;


/**
 *
 * @author Milan Dojchinovski <milan.dojchinovski@fit.cvut.cz>
 * http://dojchinovski.mk
 */
public class FilterSinkTSV extends StreamRDFBase  {
        private final Node[] properties ;
        // Where to send the filtered triples.
//        private final Sink<Triple> dest ;
        PrintWriter dest; //= new PrintWriter(new BufferedWriter(new FileWriter("myfile.txt", true)));
        String loc;
//        Model model = ModelFactory.createDefaultModel();
//        public Resource previousRes = null;

        FilterSinkTSV(PrintWriter dest, String loc, Property... properties)
        {
            this.dest = dest ;
            this.loc = loc;
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
                    String subj = triple.getSubject().getURI().split("#")[0];
                    subj = subj.substring(0,subj.length()-9);
                    dest.write(loc+"\t"+subj+"\n") ;
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
