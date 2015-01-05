package bank;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A simple implementation of the ReservationManager interface. Each object of
 * this class must create a dedicated connection to the database.
 * <p>
 * <b>Note: DO NOT alter this class's interface.</b>
 * 
 * @author Busca
 * 
 */
public class BankManagerImpl implements BankManager {

    // CLASS FIELDS
    //
    // example of a create table statement executed by createDB()
    private static final String CREATE_TABLE_ACCOUNTS = "create table ACCOUNTS (" + 
	    "NUMBER int not null, " + 
	    "BALANCE double, " + 
	    "primary key (NUMBER)" + 
	    ")";
    
    private static final String CREATE_TABLE_OPERATIONS = "create table OPERATIONS (" + 
    	    "NUMBER int not null, " + 
    	    "AMOUNT double, " + 
    	    "DATE date, " + 
    	    "primary key (NUMBER)," + 
    	    "constraint account_fk foreign key (NUMBER) references ACCOUNTS(NUMBER) " +
    	    ")";
    
    private static final String DROP_TABLE_OPERATIONS = "drop table OPERATIONS;";
    private static final String DROP_TABLE_ACCOUNTS = "drop table ACCOUNTS;";
    
    private static final String INSERT_ACCOUNT = "insert into ACCOUNTS (NUMBER, BALANCE) values (?, 0) ;";
    private static final String SELECT_BALANCE = "select BALANCE from ACCOUNTS where NUMBER=";
    private static final String UPDATE_BALANCE = "update ACCOUNTS set BALANCE=? where NUMBER=? ;";
    private static final String SELECT_OPERATIONS = "select * from OPERATIONS where DATE between ";
  
    private Connection con;
    
    private Statement statement;
    private PreparedStatement psInsertAccount;
    private PreparedStatement psUpdateBalance;
    private ResultSet result;
    
    
    /**
     * Creates a new ReservationManager object. This creates a new connection to
     * the specified database.
     * 
     * @param url
     *            the url of the database to connect to
     * @param user
     *            the login name of the user
     * @param password
     *            his password
     */
    public BankManagerImpl(String url, String user, String password) throws SQLException {
    	
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	//Create a new connection with the database
    	con = DriverManager.getConnection(url, user, password);
    	//disable autocommit
    	con.setAutoCommit(false);
    	//create new statement et prepared statement objects
    	statement = con.createStatement();
    	psInsertAccount = con.prepareStatement(INSERT_ACCOUNT);
    	psUpdateBalance = con.prepareStatement(UPDATE_BALANCE);


    }

    /**
     * Drop the existing tables if so, and creates the tables accounts and operations in the database
     */
    public void createDB() throws SQLException {

    	try {
			//drop tables if exists
			statement.executeUpdate(DROP_TABLE_OPERATIONS);
			con.commit();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		}
    	
    	try {
			statement.executeUpdate(DROP_TABLE_ACCOUNTS);
			con.commit();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		}
    	
    	
    	try{
    		//Execute two table creation queries
        	statement.executeUpdate(CREATE_TABLE_ACCOUNTS);
        	statement.executeUpdate(CREATE_TABLE_OPERATIONS);
        	//Commit the executed queries
        	con.commit();
    	}catch(Exception e){
    		//roll-back the transaction if errors occured
    		con.rollback();
    		System.out.println("System failed to create database tables : " + e.getMessage());
    	}
    	
    }

    /**
     * insert the account with number in parameter and balance 0
     * 
     * @param number
     * 		the account number
     * @return 
     * 		true if success
     * 		false if failed
     */
    public boolean createAccount(int number) throws SQLException {
    	//initialize success variable
    	boolean success = true;
    	
    	try {
    		//set prepared statement parameters and execute it
			psInsertAccount.setInt(1, number);
			psInsertAccount.execute();
			//commit the transaction
			con.commit();
		} catch (Exception e) {
			success = false;
			//roll-back the transaction if errors occured
			con.rollback();
			e.printStackTrace();
			//set success variable to false, as account creation failed
			
		}
    	
    	return success;
    	
    }


    

    /**
     * retrieve and return the balance of account with number in parameter
     * 
     * @param number
     * 		the account number
     * @return 
     * 		the account balance if success
     * 		0 if account doesn't exist
     */
    public double getBalance(int number) throws SQLException {
    	//Retrives the account balance
    	result = statement.executeQuery( SELECT_BALANCE + number + ";" );
    	if(result.next()){
    		// return the balance if the account exists, 
    		return result.getDouble(1);	
    	}else{
    		//return 0 if the account does not exist
    		return 0;
    	}
    }

    
    /**
     * add the amount in param to the account with number in param
     * 
     * @param number 
     * 		the account number
     * @param amount
     * 		the balance to add
     * @return 
     * 		the account new balance 
     */
    public double addBalance(int number, double amount) throws SQLException {
    	//retrives the balance and calculate its new value
    	double balance = getBalance(number);
    	double newBalance = balance + amount;
    	try {
    		//set the parameters in the prepared statement an execute it
			psUpdateBalance.setDouble(1, newBalance);
			psUpdateBalance.setInt(2, number);
			psUpdateBalance.execute();
			//commit the transaction
			con.commit();
		} catch (Exception e) {
			//roll-back the transaction if errors occured
			con.rollback();
			e.printStackTrace();
			//set newbalance to its original value since the update failed
			newBalance = balance;
		}
    	
    	return newBalance;
    }

    
    @Override
    public boolean transfer(int from, int to, double amount) throws SQLException {
    	boolean success = true;
    	
    	//retrives the balances and calculates their new values
    	double balanceFrom = getBalance(from);
    	double newBalanceFrom = balanceFrom - amount;

    	double balanceto = getBalance(to);
    	double newBalanceTo = balanceto + amount;
    	
    	try {
			//set prepared statements parameters 
			psUpdateBalance.setDouble(1, newBalanceFrom);
			psUpdateBalance.setInt(2, from);
			psUpdateBalance.execute();
			
			psUpdateBalance.setDouble(1, newBalanceTo);
			psUpdateBalance.setInt(2, to);
			psUpdateBalance.execute();
			//commit the transaction
			con.commit();
		} catch (Exception e) {
			success = false;
			//roll-back the transaction if errors occured
			con.rollback();
			e.printStackTrace();
			
		}
    	
    	return success;
    }

    @Override
    public List<Operation> getOperations(int number, Date from, Date to) throws SQLException {
    	
    	List<Operation> list = new ArrayList<Operation>();
    	
    	java.sql.Date from1 = new java.sql.Date(from.getTime());
    	java.sql.Date to1 = new java.sql.Date(to.getTime());
    	
    	
    	String query = SELECT_OPERATIONS + from1.toString() + " and " + to1.toString() + " and NUMBER=" + number + ";";
    	result = statement.executeQuery(query);
    	
    	while( result.next() ){

    	    double amount = result.getDouble(2);
    	    Date date = result.getDate(3);
    		Operation op = new Operation(number, amount, date); 
    		list.add(op);
    	}
    	
    	return list;
    }

}
