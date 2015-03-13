package com.bettercloud; /**
 * Created by amit on 3/4/15.
 */

import com.bettercloud.util.CQLUnitD;
import com.datastax.driver.core.Session;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class CassandraIndexTestBase {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    @Rule
    public CQLUnitD cassandraCQLUnit;
    protected Random rand = new Random();
    protected String[] states = new String[]{"CA", "LA", "TX", "NY", "CA", "LA", "TX", "NY", "CA", "LA", "TX", "NY", "NONEXISTENT"};


    protected void dropTable(String ksName, String tName) {
        getSession().execute("DROP table " + ksName + "." + tName);
    }

    protected void createKS(String ksName) {
        String q = "CREATE KEYSPACE " + ksName + " WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1}";
        getSession().execute(q);
    }

    protected void dropKS(String ksName) {
        getSession().execute("DROP keyspace " + ksName);
    }

    protected Session getSession() {
        return cassandraCQLUnit.session();
    }

}
