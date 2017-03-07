package mongo1;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 *
 * @author oracle
 */
public class Mongo1 {

    MongoCollection<Document> coleccion;
    MongoClient cliente;
    MongoDatabase base;
    BasicDBObject consulta;
    BasicDBObject claves;

    public void conexion(String bd, String colection) {
        cliente = new MongoClient("localhost", 27017);
        base = cliente.getDatabase(bd);
        coleccion = base.getCollection(colection);
    }

    /**
     * Mostrar todos los elementos con valor essay select * from scores where
     * kind = "essay";
     *
     * @param clave
     * @param valor
     */
    public void consultas(String clave, String valor) {
        consulta = new BasicDBObject(clave, valor);
        FindIterable<Document> cursor = coleccion.find(consulta);
        MongoCursor<Document> iterator = cursor.iterator();
        while (iterator.hasNext()) {
            Document d = iterator.next();
            String kind = d.getString(clave);
            Double sc = d.getDouble("scores");
            Double st = d.getDouble("student");
            System.out.println("Kind: " + kind + "\n"
                    + "Scores: " + sc + "\n"
                    + "Student: " + st);
        }
        iterator.close();
    }

    /* 
     * Mostrar los elementos score y student select score, student from scores
     * where kind = "essay";
     */
    public void consultaConProyeccion() {
        claves = new BasicDBObject("_id", new ObjectId());
        claves.put("_id", 0);
        claves.put("score", 1);
        claves.put("student", 1);
        FindIterable<Document> cursor = coleccion.find(consulta).projection(claves);
        MongoCursor<Document> iterator = cursor.iterator();
        while (iterator.hasNext()) {
            Document d = iterator.next();
            Double sc = d.getDouble("scores");
            Double st = d.getDouble("student");
            System.out.println("Scores: " + sc + "\n"
                    + "Student: " + st);
        }
        iterator.close();
    }

    public void consultaPorId() {
        claves = new BasicDBObject("_id", new ObjectId("4c90f2543d937c033f42473b"));
        claves.put("_id", 0);
        claves.put("score", 1);
        claves.put("student", 1);
        Document d = coleccion.find(consulta).projection(claves).first();
        Double sc = d.getDouble("score");
        Double st = d.getDouble("student");
        System.out.println("Scores: " + sc + "\n"
                + "Student: " + st);

    }

    public void cerrarConexion() {
        cliente.close();
    }

    public static void main(String[] args) {
        Mongo1 mongo = new Mongo1();
        mongo.conexion("training", "scores");
       //mongo.consultas("kind", "essay");
        // mongo.consultaConProyeccion();
        mongo.consultaPorId();
        mongo.cerrarConexion();
    }
}
