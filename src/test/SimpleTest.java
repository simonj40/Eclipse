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
		    System.out.println(this + ": starting for "+ customer);
		    int accountNumBase = (int)Thread.currentThread().getId();
		    int accNum1 = accountNumBase * 1;
		    int accNum2 = accountNumBase * 2;
		    
		    try{
		    //create two accounts for tests (per customer)
		    manager.createAccount(accNum1);
		    manager.createAccount(accNum2);
		    
		    // deposit 1000 on account #1
			double b = manager.addBalance(accNum1, 1000.0);
			check("addBalance for "+ customer,  b == 1000.0);
		
			// transfer 250 from account #1 to account #2
			boolean s = manager.transfer(accNum1, accNum2, 250.0);
			check("transfer-1 for "+ customer,  s);
			check("transfer-2 for "+ customer,  manager.getBalance(accNum1) == 750.0);
			check("transfer-3 for "+ customer,  manager.getBalance(accNum2) == 250.0);
			
			// check operations on account #1 between yesterday and now
			Date now = new Date();
			List<Operation> o1 = manager.getOperations(accNum1, new Date(now.getTime() - 24*60*60*1000), now);
			System.out.println("operations on account #1 for "+ customer + " = " + o1);
			check("getOperations-1 for "+ customer, o1.size() == 2);
			List<Operation> o2 = manager.getOperations(accNum2, new Date(now.getTime() - 24*60*60*1000), now);
			System.out.println("operations on account #2 for "+ customer + " = " + o2);
			check("getOperations-2 for "+ customer, o2.size() == 1);
		    }
		    catch(Exception e){
		    	System.err.println("multi-user test aborted for "+ customer + e);
			    e.printStackTrace();
		    }
		    System.out.println(this + ": exiting for "+ customer);
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
	
		// transfer 250 from account #1 to account #2
		boolean s = m.transfer(1, 2, 250.0);
		check("transfer-1",  s);
		check("transfer-2",  m.getBalance(1) == 750.0);
		check("transfer-3",  m.getBalance(2) == 250.0);
		
		// check operations on account #1 between yesterday and now
		Date now = new Date();
		List<Operation> o1 = m.getOperations(1, new Date(now.getTime() - 24*60*60*1000), now);
		System.out.println("operations on account #1 = " + o1);
		check("getOperations-1", o1.size() == 2);
		List<Operation> o2 = m.getOperations(2, new Date(now.getTime() - 24*60*60*1000), now);
		System.out.println("operations on account #2 = " + o2);
		check("getOperations-2", o2.size() == 1);
		
		// TODO complete the test
		
		// deposit 500 on account #2
		b = m.addBalance(2, 500.0);
		check("addBalance",  b == 750.0);
	
		// transfer 250 from account #1 to account #3
		s = m.transfer(1, 3, 250.0);
		check("transfer-1",  s);
		check("transfer-2",  m.getBalance(1) == 500.0);
		check("transfer-3",  m.getBalance(3) == 250.0);
		
		// check operations on account #1 between yesterday and now
		now = new Date();
		o1 = m.getOperations(1, new Date(now.getTime() - 24*60*60*1000), now);
		System.out.println("operations on account #1 = " + o1);
		check("getOperations-3", o1.size() == 3);
		o2 = m.getOperations(2, new Date(now.getTime() - 24*60*60*1000), now);
		System.out.println("operations on account #2 = " + o2);
		check("getOperations-4", o2.size() == 2);
		List<Operation> o3 = m.getOperations(3, new Date(now.getTime() - 24*60*60*1000), now);
		System.out.println("operations on account #3 = " + o3);
		check("getOperations-5", o3.size() == 1);
		
		
		
		
		
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
		    System.out.println("Starting single user tests...");
		    singleUserTests(manager, "single-customer");
		    System.out.println("...end of single user tests");
		    
		    // execute multi-user tests
		    //call CreateDB() method to re-initialize database
		    //this prevents any possible conflict between PKs used in single user tests and multi user tests
		    System.out.println("Preparing database for multi user tests...");
		    manager.createDB();
		    System.out.println("Starting multi user tests...");
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
