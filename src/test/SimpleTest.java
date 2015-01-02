package test;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import bank.BankManager;
import bank.BankManagerImpl;
import bank.Operation;

/**
 * The squeleton of a simple test program for {@link BankManagerImpl}.
 * 
 * @author Busca
 * 
 */
public class SimpleTest {

    //
    // CONSTANTS
    //
    private static final int MAX_ACCOUNTS = 10;
    private static final int MAX_CUSTOMERS = 5;

    //
    // CLASS FIELDS
    //
    private static int testTotal = 0;
    private static int testOK = 0;

    //
    // HELPER CLASSES
    //
    static class CustomerEmulator extends Thread {

		private BankManager manager;
		private String customer;
	
		public CustomerEmulator(BankManager m, String c) {
		    manager = m;
		    customer = c;
		}
	
		public String toString() {
		    return customer + "[" + manager + "]";
		}
	
		public void run() {
		    System.out.println(this + ": starting");
		    // TODO complete the test
		    System.out.println(this + ": exiting");
		}

    }

    //
    // HELPER METHODS
    //
    private static void check(String test, boolean ok) {
		testTotal += 1;
		System.out.print(test + ": ");
		if (ok) {
		    testOK += 1;
		    System.out.println("ok");
		} else {
		    System.out.println("FAILED");
		}
    }

    private static void singleUserTests(BankManager m, String c) throws SQLException {

		// deposit 1000 on account #1
		double b = m.addBalance(1, 1000.0);
		check("addBalance",  b == 1000.0);
	
		// transfert 500 from account #1 to account #2
		boolean s = m.transfer(1, 2, 250.0);
		check("transfert-1",  s);
		check("transfert-2",  m.getBalance(1) == 750.0);
		check("transfert-3",  m.getBalance(2) == 250.0);
		
		// check operations on account #1 between yesterday and now
		Date now = new Date();
		List<Operation> o1 = m.getOperations(1, new Date(now.getTime() - 24*60*60*1000), now);
		System.out.println("operations on account #1 = " + o1);
		check("getOperations-1", o1.size() == 2);
		List<Operation> o2 = m.getOperations(2, new Date(now.getTime() - 24*60*60*1000), now);
		System.out.println("operations on account #2 = " + o2);
		check("getOperations-1", o2.size() == 1);
		
		// TODO complete the test
		
    }

    //
    // MAIN
    //
    public static void main(String[] args) {

		// check parameters
		if (args.length != 3) {
		    System.err.println("usage: SimpleTest <url> <user> <password>");
		    System.exit(-1);
		}
	
		try {
		    // create ReservationManager object
		    BankManager manager = new BankManagerImpl(args[0], args[1], args[2]);
	
		    // create the database
		    manager.createDB();
		    
		    // populate the database
		    for (int i = 0; i < MAX_ACCOUNTS; i++) {
			manager.createAccount(i + 1);
		    }
	
		    // execute single-user tests
		    singleUserTests(manager, "single-customer");
	
		    // execute multi-user tests
		    for (int i = 0; i < MAX_CUSTOMERS; i++) {
			BankManager m = new BankManagerImpl(args[0], args[1], args[2]);
			new CustomerEmulator(m, "multi-customer" + i).start();
		    }
		    
		} catch (Exception e) {
		    System.err.println("test aborted: " + e);
		    e.printStackTrace();
		}
	
		// print test results
		if (testTotal == 0) {
		    System.out.println("no test performed");
		} else {
		    String r = "test results: ";
		    r += "total=" + testTotal;
		    r += ", ok=" + testOK + "(" + ((testOK * 100) / testTotal) + "%)";
		    System.out.println(r);
		}

		
    }
}
