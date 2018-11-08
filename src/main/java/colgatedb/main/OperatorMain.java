package colgatedb.main;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.operators.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Op;
import colgatedb.tuple.StringField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static colgatedb.tuple.Type.STRING_TYPE;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public class OperatorMain {

    public static void main(String[] argv)
            throws DbException, TransactionAbortedException, IOException {

        // file named college.schema must be in colgatedb directory
        String filename = "college.schema";
        System.out.println("Loading schema from file: " + filename);
        Database.getCatalog().loadSchema(filename);
        /*
        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        StringField alice = new StringField("alice", Type.STRING_LEN);
        Predicate p = new Predicate(1, Op.EQUALS, alice);
        DbIterator filterStudents = new Filter(p, scanStudents);


        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        filterStudents.open();
        while (filterStudents.hasNext()) {
            Tuple tup = filterStudents.next();
            System.out.println("\t"+tup);
        }
        filterStudents.close();
        */

        //SQL Query:
        //SELECT S.name
        //FROM Students S, Takes T, Profs P
        //WHERE S.sid = T.sid AND
        //      T.cid = P.favoriteCourse AND
        //      P.name = "hay"
        //algebra translation: rename(P1, select_{name="hay"}(P)), ----left child
        //                     rename(R1, S join_{S.sid = T.sid} T), ----right child
        //                     rename(R2, R1 join_{T.cid = P.favoriateCourse} P1), ---root
        //                     pi_{S.name}(R2) --project on root
        TransactionId tid = new TransactionId();
        DbIterator P = new SeqScan(tid, Database.getCatalog().getTableId("Profs"), "P");
        DbIterator S = new SeqScan(tid, Database.getCatalog().getTableId("Students"), "S");
        DbIterator T = new SeqScan(tid, Database.getCatalog().getTableId("Takes"), "T");
        DbIterator P1 = new Filter(new Predicate(P.getTupleDesc().fieldNameToIndex("P.name"), Op.EQUALS, new StringField("hay")), P);
        DbIterator R1 = new Join(new JoinPredicate(S.getTupleDesc().fieldNameToIndex("S.sid"),
                Op.EQUALS, T.getTupleDesc().fieldNameToIndex("T.sid")), S, T);
        DbIterator R2 = new Join(new JoinPredicate(R1.getTupleDesc().fieldNameToIndex("T.cid"),
                Op.EQUALS, P1.getTupleDesc().fieldNameToIndex("P.favoriteCourse")), R1, P1);
        DbIterator projectNames = new Project(new ArrayList<>(Arrays.asList(R2.getTupleDesc().fieldNameToIndex("S.name"))),
                new Type[]{STRING_TYPE}, R2);

        projectNames.open();
        while (projectNames.hasNext()) {
            Tuple tup = projectNames.next();
            System.out.println("\t"+tup);
        }
        projectNames.close();
    }

}