package org.mongo;

/**
 This file is part of mongo-4-demo.
 mongo-4-demo is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 mongo-4-demo is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 You should have received a copy of the GNU General Public License
 along with mongo-4-demo.  If not, see <http://www.gnu.org/licenses/>.
 @Author Jai Hirsch
 @github https://github.com/JaiHirsch/mongo-4-demo
 */

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class DemoMongoConnector implements Closeable {
    private static final ServerAddress r0 = new ServerAddress("localhost", 27017);
    private static final ServerAddress r1 = new ServerAddress("localhost", 27018);
    private static final ServerAddress r2 = new ServerAddress("localhost", 27019);
    private static final List<ServerAddress> reps = new ArrayList<>(Arrays.asList(new ServerAddress[]{r0, r1, r2}));

    private final MongoClient mc;
    private final MongoDatabase db;
    private final MongoCollection<Document> inventory;
    private final MongoCollection<Document> shipment;


    public DemoMongoConnector() {
        this.mc = new MongoClient(reps);
        this.db = mc.getDatabase("test");
        this.inventory = db.getCollection("inventory");
        this.shipment = db.getCollection("shipment");
    }

    public MongoClient getMongoClient() {
        return this.mc;
    }

    public MongoDatabase getDatabase() { return this.db; }


    public MongoCollection getInventory() {
        return this.inventory;
    }

    public MongoCollection getShipment() {
        return this.shipment;
    }

    @Override
    public void close() throws IOException {
        mc.close();
    }
}
