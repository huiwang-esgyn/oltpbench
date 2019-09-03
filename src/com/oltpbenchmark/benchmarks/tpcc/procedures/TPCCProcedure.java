/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.sun.xml.bind.v2.runtime.JAXBContextImpl;
import org.apache.log4j.Logger;

public abstract class TPCCProcedure extends Procedure {

    private static final Logger LOG = Logger.getLogger(TPCCProcedure.class);

    HashMap<String, Long> duration = new HashMap<String, Long>();
    HashMap<String, Long> count = new HashMap<String, Long>();

    private void increment(String s, long inc) {
        if (duration.containsKey(s)) {
            long d = duration.get(s);
            long c = count.get(s);
            duration.put(s, d+inc);
            count.put(s, c+1);
        }
        else {
            duration.put(s, inc);
            count.put(s, 1l);
        }
    }

    protected  ResultSet executeQuery(PreparedStatement stmt, SQLStmt s) throws SQLException {
        long start = System.nanoTime();
        ResultSet rs = stmt.executeQuery();
        long end = System.nanoTime();
        increment(s.getSQL(), end - start);
        return rs;
    }

    protected int executeUpdate(PreparedStatement stmt, SQLStmt s) throws SQLException {
        long start = System.nanoTime();
        int r = stmt.executeUpdate();
        long end = System.nanoTime();
        increment(s.getSQL(), end - start);
        return r;
    }

    protected void executeBatch(PreparedStatement stmt, SQLStmt s) throws SQLException {
        long start = System.nanoTime();
        stmt.executeBatch();
        long end = System.nanoTime();
        increment(s.getSQL(), end - start);
        return;
    }

    public void dump_stats(int id) {
        duration.forEach((k, v) -> {
            long c = count.get(k);
            LOG.info(String.format("[%03d:%20s:%50s] %020d %010d %010d", id, toString(), k.substring(0, Math.min(k.length(), 50)), v, c, v / c));
        });
    }

    public abstract ResultSet run(Connection conn, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException;

}
