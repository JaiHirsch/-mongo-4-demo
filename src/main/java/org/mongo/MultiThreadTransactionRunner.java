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

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.assertions.Assertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.mongodb.client.model.Filters.eq;

public class MultiThreadTransactionRunner {

    public static void main(String[] args) throws IOException {
        try (DemoMongoConnector dmc = new DemoMongoConnector()) {

            setUpMongoForTransactionTest(dmc);

            launchThreadsAndRunTransactions(dmc);

            Document skuAbc123 = (Document) dmc.getInventory().find(eq("sku", "abc123")).first();

            System.out.println("++++++++++ " + skuAbc123);
            Assertions.isTrue("qty should have been 500", 500 == skuAbc123.getInteger("qty"));
        }

    }

    private static void setUpMongoForTransactionTest(DemoMongoConnector dmc) {
        MongoDatabase db = dmc.getMongoClient().getDatabase("test");
        db.drop();
        db.createCollection("inventory");
        db.createCollection("shipment");
        dmc.getInventory().insertOne(new Document("sku", "abc123").append("qty", 500));
    }

    private static void launchThreadsAndRunTransactions(DemoMongoConnector dmc) {

        submitTransactionThreads(dmc).forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }

    private static List<Future> submitTransactionThreads(DemoMongoConnector dmc) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future> futures = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            futures.add(executorService.submit(new TransactionRetryModule().iterateTransactions(100, 5)));
        }

        for (int i = 0; i < 4; i++) {
            futures.add(executorService.submit(new TransactionRetryModule().iterateTransactions(-100, 5)));
        }

        executorService.shutdown();

        return futures;
    }

}
