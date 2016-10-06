package scmldap;
import com.ca.harvest.jhsdk.*;
import com.ca.harvest.jhsdk.hutils.*;
import com.ca.harvest.jhsdk.logger.*;

import java.sql.*;

import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

import java.io.*;
import java.util.*;

import javax.naming.*;
import javax.naming.directory.*;

// Main Class
public class scmldap implements IJCaLogStreamListener
{
	private static JCaHarvest harvest;
	static int iReturnCode = 0;

	String LogName = "scmldap.log";
	private static PrintWriter Log = null;
	private boolean debug = false;
	private static boolean bAttest = false;
	private static boolean deleteDisabledUsers = false;
	
	scmldap()
	{
	       	try
	        {
	            FileOutputStream osLogStream = new FileOutputStream(LogName);
	            Log = new PrintWriter(osLogStream, true);
	            DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.LONG);
	        }
	        catch (FileNotFoundException ex)
	        {
	            if (debug) System.err.println("Error: creating log file");
	            if (debug) System.err.println("\t" + ex.getMessage());
	        }
	}

	private void setDebug()
	{
		debug = true;
	}

	// method to print log to logfile
	private static void printLog(String str)
	{
		System.out.println(str);
		Log.println(str);
	}

	public void handleMessage(String sMessage)
	{
		printLog(sMessage);
	}

	private static void ProcessLDAPAttrs(Attributes attributes, 
            JCaContainer cLDAP,
            boolean isNormalUser) 
	{
		int cIndex = 0;		
		if (cLDAP.getKeyCount() > 0)
		{
			cIndex = cLDAP.getKeyElementCount("sAMAccountName");
		}

		if (attributes.size() >= 3)
		{
			try {
				for (NamingEnumeration ae = attributes.getAll(); ae.hasMore();) {
					Attribute attr = (Attribute)ae.next();
					//printLog("attribute: " + attr.getID());
					
					/* Process each value */
					for (NamingEnumeration e = attr.getAll(); 
					e.hasMore();
					//printLog("value: " + e.next()) ) ;
					cLDAP.setString(attr.getID(), (String)e.next(), cIndex) );
				}

				if (attributes.size() == 3)
				{
					cLDAP.setString("mail", "unknown", cIndex);
				}


				String sID = cLDAP.getString("sAMAccountName", cIndex);
		
				cLDAP.setString("haspmfkey", 
								(isNormalUser &&
				//TODO need a separate routine for validating the user id belongs to a user or not.						        		
							    sID.length() == 7 &&
							    !sID.equalsIgnoreCase("clate98") &&
							    !sID.equalsIgnoreCase("clate99") &&
							    !sID.equalsIgnoreCase("BEStest"))? "Y" : "N", 
							   cIndex);

			} catch (NamingException e) {
				// Handle the error
				iReturnCode = 2;
				System.err.println(e);
				System.exit(iReturnCode);
			}
		}
} // end ProcessLDAPAttrs


	private static void ProcessLDAPRegion(DirContext ctx, 
	             String region, 
	             JCaContainer cLDAP,
	             boolean isNormalUser) 
	{
		try {
			// Search directory for containers
			// Create the default search controls
			SearchControls ctls = new SearchControls();
	
			// Specify the search filter to match
			String filter = "(&(!(objectclass=computer))(&(objectclass=person)(sAMAccountName=*)))";
			
			// Specify the ids of the attributes to return
			String[] attrIDs = {"sAMAccountName", "displayName", "mail", "distinguishedName"};
			ctls.setReturningAttributes(attrIDs);
	
			// Search for objects that have those matching attributes
			NamingEnumeration enumeration = ctx.search(region, filter, ctls);
			
			while (enumeration.hasMore()) {
				SearchResult sr = (SearchResult)enumeration.next();
				//printLog(">>>" + sr.getName());
				ProcessLDAPAttrs(sr.getAttributes(), cLDAP, isNormalUser);
			}			
		} catch (javax.naming.AuthenticationException e) {
			iReturnCode = 1;
			System.err.println(e);
			System.exit(iReturnCode);			
		} catch (NamingException e) {
			// attempt to reacquire the authentication information
			// just skip region
			//iReturnCode = 2;
			//System.err.println(e);
			//System.exit(iReturnCode);
		}	
	} // end ProcessLDAPRegion

// main processing routine    
    
	private static void ProcessHarvestDatabase(String dburl, 
			                               	   JCaContainer cLDAP,
			                               	   DirContext ctx ) 
	{
		String sqlError = "DB2 Database connection Failed";
		String dbname1 = dburl.substring(dburl.indexOf("databaseName=")+13,dburl.indexOf("user=")-1);
		String dbname;
		if (dbname1.indexOf(";")==-1) 
			dbname=dbname1;
		else
			dbname = dbname1.substring(0,dbname1.indexOf(";"));
		
		if (!bAttest)
			printLog("Processing: " + dbname);
		
		try {
			JCaContainer cUsers = new JCaContainer();
			int iIndex = 0;

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			  
			// Set URL for data source
			// Create connection
			Connection conn = DriverManager.getConnection(dburl);
			if (conn != null)  
			{    
				PreparedStatement pstmt = null; 
				ResultSet rset=null;	
				
				sqlError = "DB. User Query Statement Failed";
				String sqlStmt = "select U.username, U.realname, UD.accountexternal, UD.accountdisabled from haruser U inner join haruserdata UD on UD.usrobjid = U.usrobjid where UD.accountdisabled='N' order by 1";
				pstmt=conn.prepareStatement(sqlStmt);  
				rset=pstmt.executeQuery();    

				if(rset!=null)  
				{   
					sqlError = "DB. Error processing return record";
					while(rset.next())   
					{   
						cUsers.setString("USERNAME",        rset.getString("USERNAME"), iIndex);
						cUsers.setString("REALNAME",        rset.getString("REALNAME"), iIndex);
						cUsers.setString("ACCOUNTEXTERNAL", rset.getString("ACCOUNTEXTERNAL"), iIndex);
						cUsers.setString("ACCOUNTDISABLED", rset.getString("ACCOUNTDISABLED"), iIndex);
						iIndex++;
					}
				}
				
				if (bAttest)
				{
					String sBroker1 = dburl.substring(dburl.indexOf(";databaseName=")+14,dburl.indexOf(";user="));
					String sBroker;
					if (sBroker1.indexOf(";")==-1)
						sBroker = sBroker1;
					else
						sBroker = sBroker1.substring(0,sBroker1.indexOf(";"));
					
					for (int i=0; i<cUsers.getKeyElementCount("USERNAME"); i++ )
					{
						printLog(sBroker+"\t"+
								 cUsers.getString("REALNAME", i)+"\t"+
								 cUsers.getString("USERNAME", i)+"\t"+
								 cUsers.getString("ACCOUNTEXTERNAL", i)+"\t"+
								 cUsers.getString("ACCOUNTDISABLED", i));
					}
				}
				else
				{
					String sList = "";
	
					for (int i=0; i<cUsers.getKeyElementCount("USERNAME"); i++ )
					{
						String sID = cUsers.getString("USERNAME", i);
						
						int iLDAP[] = cLDAP.find("sAMAccountName", sID);
						
						if (iLDAP.length == 0)
							iLDAP = cLDAP.find("sAMAccountName", sID.toLowerCase());
						
						if (iLDAP.length == 0 &&
							cUsers.getString("ACCOUNTEXTERNAL", i).equalsIgnoreCase("Y") )
						{
							// Account not found in CA.COM
							printLog(">>>User (disabled): "+cUsers.getString("REALNAME",i)+ "("+ sID+") ");
							if (sList.length() > 0)
								sList += ",";
							sList +="'"+sID+"'";
						}
					}  //loop over user accounts
			
					int iResult = 0;
					if (sList.length()>0)
					{
						sqlStmt = "update haruserdata set accountdisabled='Y' where usrobjid in (select usrobjid from haruser where username in ("+ sList+ ") )";
						pstmt=conn.prepareStatement(sqlStmt);  
						iResult = pstmt.executeUpdate();
						if (iResult > 0) printLog(">>>:Update Succeeded");
					}
					
					if (deleteDisabledUsers)
					{
						/* Show users to be deleted */
						sqlStmt = "select username, realname from haruser where usrobjid in (select usrobjid from haruserdata where accountexternal='Y' and accountdisabled='Y' )";
						pstmt=conn.prepareStatement(sqlStmt);  
						rset=pstmt.executeQuery();    
	
						if(rset!=null)  
						{   
							sqlError = "DB. Error processing return record";
							while(rset.next())   
							{   
								String sID   = rset.getString("USERNAME");
								String sUser = rset.getString("REALNAME");
								printLog(">>>User (deleting): "+sUser+ "("+ sID +") ");
							}
						}
	
						sqlStmt = "delete from haruser where usrobjid in (select usrobjid from haruserdata where accountexternal='Y' and accountdisabled='Y' )";
						pstmt=conn.prepareStatement(sqlStmt);  
						iResult = pstmt.executeUpdate();
						if (iResult > 0) printLog(">>>:"+iResult+" External, Disabled Users Deleted ");					
					}
					
					printLog("\r\n");
				}
				
				conn.close();
			} // have connection       
			else  
			{           
				printLog(sqlError);
			}			  
		} catch (ClassNotFoundException e) {
			iReturnCode = 3;
		    System.err.println(e);
			//e.printStackTrace();
		    System.exit(iReturnCode);
		} catch (SQLException e) {     
			iReturnCode = 4;
		    System.err.println(sqlError);
			//e.printStackTrace(); 
		    System.exit(iReturnCode);
		}
	} // end ProcessHarvestDatabase
	



	public static void main(String[] args)
	{
		int iParms = args.length;
		String sBroker = null;
		String sEncrypted = null;
		boolean debugflag = false;
		int iReturnCode = 0;
		String LogName = "C:";
		
		if (iParms <= 0)
		{
			//printLog("Usage: scmldap - scmldap"); 
			//System.exit(iReturnCode);
		}

		// check parameters
		for (int i = 0; i < iParms; i++)
		{
			if (args[i].compareToIgnoreCase("-h") == 0 || 
				args[i].compareToIgnoreCase("-?") == 0)
			{
				printLog("Usage: scmldap [-attest] [-del] [-log pathname] [-h]");
				printLog(" -attest option will display all users");
				printLog(" -del  option will delete all external, disabled users");
				printLog(" -log  option specifies location of log file");
				System.exit(iReturnCode);
			}

			if (args[i].compareToIgnoreCase("-attest") == 0 )
			{
				bAttest = true;
			}			
			
			if (args[i].compareToIgnoreCase("-del") == 0 )
			{
				deleteDisabledUsers = true;
			}			
			
			if (args[i].compareToIgnoreCase("-log") == 0 )
			{
				LogName = args[++i];
			}			
		} // end for

		// execution from here
		String[] regions = { "ou=users,ou=north america",
				             "ou=users,ou=itc hyderabad",
				             "ou=users,ou=europe middle east africa",
				             "ou=users,ou=asia pacific",
				             "ou=users,ou=south america",
				             "ou=joint venture consultants",
				             "ou=role-based,ou=north america",
				             "ou=role-based,ou=itc hyderabad",
				             "ou=role-based,ou=europe middle east africa",
				             "ou=role-based,ou=asia pacific",
				             "ou=role-based,ou=south america",
				             "cn=users"
				           };
		JCaContainer cLDAP = new JCaContainer();
		
		String cscrBrokers[] = /* 191, 229, 231, 232, 233, 234 */
		{			
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr001;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr003;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr004;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr005;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr007;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr009;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr101;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr102;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr104;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr105;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr106;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr108;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr109;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr110;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr111;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr112;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr113;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr201;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr402;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr403;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr501;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr502;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr503;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr504;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr601;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr602;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr603;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr604;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr605;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr606_12.5;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr607;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr608;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr609;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr610;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr611;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr612;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr616;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr617;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr618;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr619;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr620;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr621;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr622;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr623;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr624;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr625;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr626;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr701;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr702;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr703;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr704;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr706;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr707;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr708;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr709;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr801;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr802;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr803;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr804;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr805;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr806;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr807;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr808;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr809;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr810;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr811;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr901;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr902;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr903;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr904;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr905;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr906;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr907;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr911;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr911-a;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr912;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr913;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr914;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr917;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr919;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr920;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr921;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr922;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr924;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr925;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr927;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr929;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1001;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1002;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1101;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1102;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1103;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1201;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1203;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1301;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			//"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr1302;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1303;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1304;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1305;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1306;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1307;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1308;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1309;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1400;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;password=V724TF@y3Adawt$;",
			"jdbc:sqlserver://usildb206:1433;databaseName=cscrarchive;user=harvest;password=Qu3$t4$aF3;"
		};

		//Read user containers for CA.COM
		Hashtable env = new Hashtable();
		env.put(Context.PROVIDER_URL, "ldap://usildc04.ca.com:389/dc=ca,dc=com");
		env.put(Context.SECURITY_PRINCIPAL, "harvestcscr");
		env.put(Context.SECURITY_CREDENTIALS, "w3G0Th3Be3t");
		env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");

		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd@HH_mm_ss");
			Date date = new Date();
			LogName += "\\scmldap_" +dateFormat.format(date) +(bAttest?".tsv":".log");
	        FileOutputStream osLogStream = new FileOutputStream(LogName);
	        Log = new PrintWriter(osLogStream, true);
	        
	        DirContext ctx = new InitialDirContext(env);
/* */		
		
			for (int i=0; i<regions.length; i++)
			{
				ProcessLDAPRegion(ctx, regions[i], cLDAP, true);
			}

			// Show cLDAP statistics
			if (bAttest)
				printLog("Broker\tDisplay Name\tPMFKEY\tDomain Account\tDisabled Account");
			else
				printLog("Number of CA.COM user containers read: " + cLDAP.getKeyElementCount("sAMAccountName"));
			
			for (int i=0; i<cscrBrokers.length; i++)
			{
				if (!cscrBrokers[i].contains("cscrarchive") || !deleteDisabledUsers)
					ProcessHarvestDatabase( cscrBrokers[i], 
					           				cLDAP, 
					           				ctx);
			};
		
		} catch (javax.naming.AuthenticationException e) {
			iReturnCode = 1;
		    System.err.println(e);
		    System.exit(iReturnCode);
		} catch (NamingException e) {
		    // attempt to reacquire the authentication information
		    // Handle the error
			iReturnCode = 2;
		    System.err.println(e);
		    System.exit(iReturnCode);			    
		} catch (FileNotFoundException e) {
			iReturnCode = 6;
		    System.err.println(e);			
			//e.printStackTrace();
		    System.exit(iReturnCode);		    
		}
		
		System.exit(iReturnCode);	

	} // end main
} //end of class
