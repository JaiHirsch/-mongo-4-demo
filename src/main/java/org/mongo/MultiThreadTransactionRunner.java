package org.mongo;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.assertions.Assertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future> futures = new ArrayList<>();

        submitTransactionThreads(dmc, executorService, futures);

        executorService.shutdown();

        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void submitTransactionThreads(DemoMongoConnector dmc, ExecutorService executorService, List<Future> futures) {
        for (int i = 0; i < 4; i++) {
            futures.add(executorService.submit(new TransactionRetryModule().retryTransaction(100, 5)));
        }

        for (int i = 0; i < 4; i++) {
            futures.add(executorService.submit(new TransactionRetryModule().retryTransaction(-100, 5)));
        }
    }


}
