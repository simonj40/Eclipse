package bank;

import java.sql.*;
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
	    "NUMBER int, " + 
	    "BALANCE double, " + 
	    "primary key (NUMBER)" + 
	    ")";
    
    private static final String CREATE_TABLE_OPERATIONS = "create table OPERATIONS (" + 
    	    "NUMBER int, " + 
    	    "ACCOUNT int, " + 
    	    "DATE int, " + 
    	    "primary key (NUMBER)" + 
    	    ")";
  
    private Connection con;
    
    
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


    }

    @Override
    public void createDB() throws SQLException {
	// TODO Auto-generated method stub
    	//create a new statement object
    	Statement statement = con.createStatement();
    	//Execute two table creation queries
    	statement.executeUpdate(CREATE_TABLE_ACCOUNTS);
    	statement.executeUpdate(CREATE_TABLE_OPERATIONS);
    	//Commit the executed queries
    	try{
    		con.commit();
    	}catch(Exception e){
    		con.rollback();
    		System.out.println("System failed to create database tables : " + e.getMessage());
    	}
    }

    @Override
    public boolean createAccount(int number) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public double getBalance(int number) throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public double addBalance(int number, double amount) throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public boolean transfer(int from, int to, double amount) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public List<Operation> getOperations(int number, Date from, Date to) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

}
