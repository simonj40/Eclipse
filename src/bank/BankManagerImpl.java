package bank;

import java.sql.*;
import java.text.SimpleDateFormat;
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
    //DateCreated DATETIME NOT NULL DEFAULT(GETDATE())
    
   private static final String CREATE_TABLE_OPERATIONS = "create table OPERATIONS (" + 
		   	"ID int NOT NULL AUTO_INCREMENT, " +
    	    "NUMBER int not null, " + 
    	    "AMOUNT double, " + 
    	    "DATE timestamp not null default NOW(), " +
    	    "primary key (ID)," + 
    	    "constraint account_fk foreign key (NUMBER) references ACCOUNTS(NUMBER) " +
    	    ")";
   
   /**
    * Trigger to check balance is not negative upon update of account balance
    * It was necessary to use DBMS specific (non-standard SQL) language in order to force the trigger to raise an
    * exception in MYSQL database (ver. 5.x)
    */
   private static final String CREATE_TRIGGER_VALIDATE_BALANCE = "CREATE TRIGGER BalanceUpdateTrigger " + 
		   "BEFORE UPDATE ON ACCOUNTS " +
		   "FOR EACH ROW " +
		   "IF NEW.BALANCE < 0 THEN " +
		   "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Balance cannot be less than 0. This transaction has been reversed.';" +
		   "END IF; ";
   
   
   /**
    * Trigger to automatically log update operations performed on accounts
    * It was necessary to use DBMS specific (non-standard SQL) language in order to force the trigger to raise an
    * exception in MYSQL database (ver. 5.x)
    */
   private static final String CREATE_TRIGGER_LOG_UPDATE_OPERATIONS = "CREATE TRIGGER OperationsLogUpdateTrigger " + 
		   "AFTER UPDATE ON ACCOUNTS " +
		   "FOR EACH ROW " +
		   "BEGIN " +
		   "INSERT INTO OPERATIONS(NUMBER, AMOUNT, DATE) VALUES(NEW.NUMBER, (NEW.BALANCE-OLD.BALANCE), SYSDATE()); " +
		   "END ";   
    
    private static final String DROP_TABLE_OPERATIONS = "drop table if exists OPERATIONS;";
    private static final String DROP_TABLE_ACCOUNTS = "drop table if exists ACCOUNTS;";
    
    
    private static final String INSERT_ACCOUNT = "insert into ACCOUNTS (NUMBER, BALANCE) values (?, 0) ;";
    private static final String SELECT_BALANCE = "select BALANCE from ACCOUNTS where NUMBER=";
    private static final String UPDATE_BALANCE = "update ACCOUNTS set BALANCE=(BALANCE+?) where NUMBER=? ;";
    private static final String SELECT_OPERATIONS = "select * from OPERATIONS ";
  
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
    		//drop tables if they exists in database...
    		statement.executeUpdate(DROP_TABLE_OPERATIONS);
    		statement.executeUpdate(DROP_TABLE_ACCOUNTS);
			con.commit();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		}   	    	   	
    	
    	
    	try{
    		//Execute table creation statements
        	statement.executeUpdate(CREATE_TABLE_ACCOUNTS);
        	statement.executeUpdate(CREATE_TABLE_OPERATIONS);
        	//Commit the executed queries
        	con.commit();
    	}catch(Exception e){
    		//roll-back the transaction if errors occured
    		con.rollback();
    		System.out.println("System failed to create database tables : " + e.getMessage());
    	}
    	
    	try{
    		//Execute two trigger creation queries
        	statement.executeUpdate(CREATE_TRIGGER_VALIDATE_BALANCE);
        	statement.executeUpdate(CREATE_TRIGGER_LOG_UPDATE_OPERATIONS);
        	//Commit the executed queries
        	con.commit();
    	}catch(Exception e){
    		//roll-back the transaction if errors occured
    		con.rollback();
    		System.out.println("System failed to create triggers: " + e.getMessage());
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
    	
    	try {
    		//set the parameters in the prepared statement an execute it
			psUpdateBalance.setDouble(1, amount);
			psUpdateBalance.setInt(2, number);
			psUpdateBalance.execute();
			//commit the transaction
			con.commit();
		} catch (Exception e) {
			/**
			 * If transaction causes the balance of account to be less than 0, a trigger will raise a validation exception
			 * This will cause DBMS to automatically roll back the transaction.
			 */
			System.err.println("Error: " + e.getMessage());
			
		}
    	
    	return getBalance(number);
    }

    
    @Override
    public boolean transfer(int from, int to, double amount) throws SQLException {
    	boolean success = true;    	
    	try {
			//set prepared statements parameters 
			psUpdateBalance.setDouble(1, -amount);
			psUpdateBalance.setInt(2, from);
			psUpdateBalance.execute();
			
			psUpdateBalance.setDouble(1, amount);
			psUpdateBalance.setInt(2, to);
			psUpdateBalance.execute();
			//commit the transaction
			con.commit();
		} catch (Exception e) {
			success = false;
			/**
			 * If transfer causes balance of crediting account to be less than 0, a trigger will raise an exception
			 * This will cause the DBMS to automatically roll back he commit.
			 */
			System.err.println("Error: " + e.getMessage());		
			
		}
    	
    	return success;
    }

    @Override
    public List<Operation> getOperations(int number, Date from, Date to) throws SQLException {
    	//date formatter to ensure date format works with the date generted by MySQL DBMS during logging
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	//array list to return
    	List<Operation> list = new ArrayList<Operation>();
    	//Operation object to be added to array list
    	Operation objOperation;
    	try{
    	String query = SELECT_OPERATIONS + "where (DATE between '"+ sdf.format(from) +"' and '"+sdf.format(to)+"')" + " and NUMBER=" + number + ";";
    	result = statement.executeQuery(query);
    	//iterate through resultset and populate Operation list
    	while( result.next() ){
    		objOperation = new Operation(result.getInt(2), result.getDouble(3), result.getDate(4));
    	    list.add(objOperation);
    	}
    	
    	}catch(Exception e){
    		/**
			 * If query fails catch exception...
			 */
			System.err.println("Error: " + e.getMessage());		
    	}    	
    	
    	return list;
    }

}
