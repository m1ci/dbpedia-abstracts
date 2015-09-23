package dbpedia;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hello world!
 *
 */
public class Main 
{
    public static void main( String[] args )
    {
//        String file = "/Users/Milan/Downloads/en/en/pairCounts";
//        HashMap<Integer,Integer> hm = new HashMap();
//        
////        hm.put(1, 5);
////        System.out.println(hm.get(1));
////        hm.put(1, 15);
////        System.out.println(hm.get(1));
//        int counter = 0;
//        
        try {
            
//            new AbstractsAnalyzer().buildLabelCounts();
//            new NIF2StanfordConverter().convert();
//            new Indexer().index();
//            Indexer idx = new Indexer();
//            idx.readIndex();
//            HashMap hm = idx.getHm();
//            System.out.println(hm.get("http://dbpedia.org/resource/Bobby_van_Jaarsveld"));
//            
//            TrainDataCreator td = new TrainDataCreator();
//            td.create(hm);
            String dir = args[0];
//            String dir = "/Users/Milan/Downloads/db-abstracts-en/";            
            new NIF2StanfordConverter().convertAll(dir);
//            DBpediaOntologyHelper helper = DBpediaOntologyHelper.getInstance();
//            System.out.println(helper.getCoarseGrainedType("http://dbpedia.org/ontology/AdministrativeRegion"));
//            System.out.println(helper.getCoarseGrainedType("http://dbpedia.org/ontology/PopulatedPlace"));
//            System.out.println(helper.getCoarseGrainedType("http://dbpedia.org/ontology/SportsManager"));
//            System.out.println(helper.getCoarseGrainedType("http://dbpedia.org/ontology/City"));
//            System.out.println(helper.getCoarseGrainedType("http://dbpedia.org/ontology/Airline"));
//            System.out.println(helper.getCoarseGrainedType("http://dbpedia.org/ontology/AmericanFootballLeague"));
//            System.out.println(helper.getCoarseGrainedType("http://dbpedia.org/ontology/Election"));
            
//            System.out.println(DBpediaQuery.getInstance().getType("http://dbpedia.org/resource/Berlin"));
            
            
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } 
//catch (IOException ex) {
//            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }
}
