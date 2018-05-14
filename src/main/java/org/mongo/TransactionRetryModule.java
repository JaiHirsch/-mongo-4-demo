package org.mongo;

import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.mongo.utils.Retry;

import java.util.Arrays;
import java.util.List;

public class TransactionRetryModule {


    private static final int MAX_RETRIES = 10;
    private static final long DELAY_BETWEEN_RETRIES_MILLIS = 300L;
    private static final List<Integer> transactionErrorCodes = Arrays.asList(112, 244, 251);


    public Runnable retryTransaction(final int amount, final int iterations) {
        return () -> {
            try (DemoMongoConnector dmc = new DemoMongoConnector()) {
                String threadName = RandomStringUtils.randomNumeric(4);

                for (int i = 0; i < iterations; i++) {


                    Retry retryLoop = new Retry().withAttempts(MAX_RETRIES).withDelay(DELAY_BETWEEN_RETRIES_MILLIS);

                    while (retryLoop.shouldContinue()) try {
                        System.out.println(threadName + " : " + retryLoop.getTimesAttempted());

                        doTransaction(dmc, amount, threadName);

                        retryLoop.markAsComplete();
                        System.out.println(threadName + " complete : " + retryLoop.completedOk());

                    } catch (Throwable e) {
                        retryLoop.takeException(e);
                        System.out.println(threadName + " : " + e.getMessage());
                    }

                    if (!retryLoop.completedOk()) {
                        throw new RuntimeException("Transaction failed after " + MAX_RETRIES + " retries.", retryLoop.getLastException()); //retryLoop.getExceptions().get(retryLoop.getExceptions().size() - 1));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

    }

    private void doTransaction(DemoMongoConnector dmc, final int amount, String threadName) {

        try (ClientSession clientSession = dmc.getMongoClient().startSession()) {
            clientSession.startTransaction(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());

            dmc.getInventory().updateOne(clientSession, Filters.eq("sku", "abc123"), Updates.inc("qty", amount));
            dmc.getShipment().insertOne(clientSession, new Document("sku", "abc123").append("qty", -amount).append("tname", threadName));
            clientSession.commitTransaction();


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
