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

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

import java.util.concurrent.atomic.AtomicBoolean;

public class ChangeStreamWatcher implements Runnable {

    private final MongoDatabase database;

    public ChangeStreamWatcher(MongoDatabase database) {
        this.database = database;
    }

    @Override
    public void run() {
        MongoCursor<ChangeStreamDocument<Document>> cursor = database.watch().iterator();

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        }
    }

}
