package org.mongo;

/**
 * This file is part of mongo-4-demo.
 * mongo-4-demo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * mongo-4-demo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with mongo-4-demo.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @Author Jai Hirsch
 * @github https://github.com/JaiHirsch/mongo-4-demo
 */

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
    private static final long DELAY_BETWEEN_RETRIES_MILLIS = 30L;
    private static final List<Integer> transactionErrorCodes = Arrays.asList(112, 244, 251);


    public Runnable iterateTransactions(final int amount, final int iterations) {
        return () -> {
            try (DemoMongoConnector dmc = new DemoMongoConnector()) {
                String threadName = RandomStringUtils.randomNumeric(4);

                for (int i = 0; i < iterations; i++) {

                    handleTransactionClientSession(amount, dmc, threadName);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

    }

    private void handleTransactionClientSession(int amount, DemoMongoConnector dmc, String threadName) {
        try (ClientSession clientSession = dmc.getMongoClient().startSession()) {

            transactionRetryLoop(amount, dmc, threadName, clientSession);
            clientSession.close();

        } catch (Exception e) {

            throw new RuntimeException("Transaction failed: " + e);
        }
    }

    private void transactionRetryLoop(int amount, DemoMongoConnector dmc, String threadName, ClientSession clientSession) {
        Retry retryLoop = new Retry().withAttempts(MAX_RETRIES).withDelay(DELAY_BETWEEN_RETRIES_MILLIS);


        while (retryLoop.shouldContinue()) try {
            System.out.println(threadName + " : " + retryLoop.getTimesAttempted());

            doTransaction(amount, dmc, threadName, clientSession);

            retryLoop.markAsComplete();
            System.out.println(threadName + " complete : " + retryLoop.completedOk());

        } catch (Throwable e) {
            retryLoop.takeException(e);
            System.out.println(threadName + " Aborting transaction: " + e.getMessage());
            clientSession.abortTransaction();
        }
        if (!retryLoop.completedOk()) {
            throw new RuntimeException("Transaction failed after " + MAX_RETRIES + " retries.", retryLoop.getLastException()); //retryLoop.getExceptions().get(retryLoop.getExceptions().size() - 1));
        }
    }

    private void doTransaction(int amount, DemoMongoConnector dmc, String threadName, ClientSession clientSession) {
        clientSession.startTransaction(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());

        dmc.getInventory().updateOne(clientSession, Filters.eq("sku", "abc123"), Updates.inc("qty", amount));
        dmc.getShipment().insertOne(clientSession, new Document("sku", "abc123").append("qty", -amount).append("tname", threadName));

        clientSession.commitTransaction();
    }

}
