package org.mongo;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class DemoMongoConnector implements Closeable{
    private static final ServerAddress r0 = new ServerAddress("localhost", 27017);
    private static final ServerAddress r1 = new ServerAddress("localhost", 27018);
    private static final ServerAddress r2 = new ServerAddress("localhost", 27019);
    private static final List<ServerAddress> reps = new ArrayList<>(Arrays.asList(new ServerAddress[]{r0, r1, r2}));

    private final MongoClient mc;
    private final MongoCollection<Document> inventory;
    private final MongoCollection<Document> shipment;



    public DemoMongoConnector() {
        mc =  new MongoClient(reps);
        inventory = mc.getDatabase("test").getCollection("inventory");
        shipment = mc.getDatabase("test").getCollection("shipment");
    }

    public  MongoClient getMongoClient() {
        return mc;
    }


    public MongoCollection getInventory() {
        return inventory;
    }

    public MongoCollection getShipment() {
        return shipment;
    }

    @Override
    public void close() throws IOException {
        mc.close();
    }
}
