package scmldap;

import commonldap.CommonLdap;
import commonldap.JCaContainer;

import java.sql.*;
import java.util.*;


// Main Class
public class HarvestLdap {
	private static int iReturnCode = 0;
	private static CommonLdap frame;

	private static boolean bAttest = false;
	private static boolean deleteDisabledUsers = false;
	
	private static String sTagUsername = "USERNAME";
	private static String sTagRealname = "REALNAME";
	private static String sTagAcctExternal = "ACCOUNTEXTERNAL";
	private static String sTagAcctDisabled = "ACCOUNTDISABLED";
	private static String sTagPmfkey       = "sAMAccountName";
	
	HarvestLdap()
	{
		// Leave blank for now
	}
    
	private static void processHarvestDatabase(String dburl, 
			                               	   JCaContainer cLDAP,
			                               	   String sHarvestDBPassword) 
	{
		String sqlError = "DB2 Database connection Failed";
		String dbname1 = dburl.substring(dburl.indexOf("databaseName=")+13,dburl.indexOf("user=")-1);
		String dbname;
		if (dbname1.indexOf(";")==-1) 
			dbname=dbname1;
		else
			dbname = dbname1.substring(0,dbname1.indexOf(";"));
		
		if (!bAttest)
			frame.printLog("Processing: " + dbname);
		
		try {
			JCaContainer cUsers = new JCaContainer();
			int iIndex = 0;

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			  
			// Set URL for data source
			// Create connection
			Connection conn = DriverManager.getConnection(dburl+"password="+sHarvestDBPassword+";");
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
						cUsers.setString(sTagUsername,     rset.getString(sTagUsername), iIndex);
						cUsers.setString(sTagRealname,     rset.getString(sTagRealname), iIndex);
						cUsers.setString(sTagAcctExternal, rset.getString(sTagAcctExternal), iIndex);
						cUsers.setString(sTagAcctDisabled, rset.getString(sTagAcctDisabled), iIndex);
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
					
					for (int i=0; i<cUsers.getKeyElementCount(sTagUsername); i++ )
					{
						frame.printLog(sBroker+"\t"+
								 	   cUsers.getString(sTagRealname, i)+"\t"+
								       cUsers.getString(sTagUsername, i)+"\t"+
								       cUsers.getString(sTagAcctExternal, i)+"\t"+
								       cUsers.getString(sTagAcctDisabled, i));
					}
				}
				else
				{
					String sList = "";
	
					for (int i=0; i<cUsers.getKeyElementCount(sTagUsername); i++ )
					{
						String sID = cUsers.getString(sTagUsername, i);
						
						int iLDAP[] = cLDAP.find(sTagPmfkey, sID);
						
						if (iLDAP.length == 0)
							iLDAP = cLDAP.find(sTagPmfkey, sID.toLowerCase());
						
						if (iLDAP.length == 0 &&
							cUsers.getString(sTagAcctExternal, i).equalsIgnoreCase("Y") )
						{
							// Account not found in CA.COM
							frame.printLog(">>>User (disabled): "+cUsers.getString(sTagRealname,i)+ "("+ sID+") ");
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
						if (iResult > 0) 
							frame.printLog(">>>:Update Succeeded");
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
								String sID   = rset.getString(sTagUsername);
								String sUser = rset.getString(sTagRealname);
								frame.printLog(">>>User (deleting): "+sUser+ "("+ sID +") ");
							}
						}
	
						sqlStmt = "delete from haruser where usrobjid in (select usrobjid from haruserdata where accountexternal='Y' and accountdisabled='Y' )";
						pstmt=conn.prepareStatement(sqlStmt);  
						iResult = pstmt.executeUpdate();
						if (iResult > 0) 
							frame.printLog(">>>:"+iResult+" External, Disabled Users Deleted ");					
					}
					
					frame.printLog("\r\n");
				}
				
				conn.close();
			} // have connection       
			else  
			{           
				frame.printLog(sqlError);
			}			  
		} catch (ClassNotFoundException e) {
			iReturnCode = 101;
		    frame.printErr(e.getLocalizedMessage());
		    System.exit(iReturnCode);
		} catch (SQLException e) {     
			iReturnCode = 102;
		    frame.printErr(sqlError);
		    System.exit(iReturnCode);
		}
	} // end ProcessHarvestDatabase
	

	public static void main(String[] args) {
		int iParms = args.length;
		String sBCC = "";
		String sLogPath = "scmldap.log";
		String sImagDBPassword  = "";
		String sHarvestDBPassword = "";
		String sProblems = "";
		
		String[] cscrBrokers = /* 191, 229, 231, 232, 233, 234 */
		{			
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr001;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr003;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr004;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr005;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr007;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr009;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr101;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr102;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr104;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr105;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr106;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr108;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr109;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr110;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr111;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr112;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr113;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr201;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr402;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr403;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr501;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr502;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr503;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr504;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr601;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr602;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr603;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr604;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr605;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr606_12.5;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			//"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr607;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr608;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr609;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr610;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr611;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr612;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr616;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr617;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr618;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr619;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr620;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr621;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr622;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr623;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr624;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr625;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr626;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr701;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr702;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr703;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr704;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr706;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr707;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr708;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr709;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr801;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr802;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr803;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr804;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr805;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr806;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr807;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr808;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr809;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr810;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr811;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr901;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr902;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr903;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr904;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr905;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr906;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr907;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr911;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr911-a;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr912;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr913;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr914;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr917;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr919;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr920;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr921;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr922;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr924;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr925;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr927;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr929;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1001;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1002;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1101;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1102;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1103;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1201;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1203;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1301;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			//"jdbc:sqlserver://L1AGUSDB004P-1;databaseName=cscr1302;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1303;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1304;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1305;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1306;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1307;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1308;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB003P-1;databaseName=cscr1309;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;",
			"jdbc:sqlserver://L1AGUSDB002P-1;databaseName=cscr1400;integratedSecurity=false;selectMethod=cursor;multiSubnetFailover=true;user=harvest;"
		};
		
		// check parameters
		for (int i = 0; i < iParms; i++)
		{
			if (args[i].compareToIgnoreCase("-h") == 0 || 
				args[i].compareToIgnoreCase("-?") == 0)
			{
				frame.printLog("Usage: scmldap [-attest] [-del] [-log pathname] [-h]");
				frame.printLog(" -attest option will display all users");
				frame.printLog(" -del  option will delete all external, disabled users");
				frame.printLog(" -log  option specifies location of log file");
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
				sLogPath = args[++i];
			}			
		} // end for

		// execution from here
		JCaContainer cLDAP = new JCaContainer();
		frame = new CommonLdap("scmldap",
                               sLogPath,
                               sBCC,
                               cLDAP);


		try {
			Map<String, String> environ = System.getenv();
	        for (String envName : environ.keySet()) {
	        	if (envName.equalsIgnoreCase("HARVEST_DB_PASSWORD"))        
	        		sHarvestDBPassword = frame.AESDecrypt(environ.get(envName));
	        	if (envName.equalsIgnoreCase("IMAG_DB_PASSWORD"))        
	        		sImagDBPassword = frame.AESDecrypt(environ.get(envName));
	        }
	        
			// Show cLDAP statistics
			if (bAttest)
				frame.printLog("Broker\tDisplay Name\tPMFKEY\tDomain Account\tDisabled Account");
			
			for (int i=0; i<cscrBrokers.length; i++)
			{
					processHarvestDatabase( cscrBrokers[i], 
					           				cLDAP,
					           				sHarvestDBPassword);
			};
		
		} catch (Exception e) {
			iReturnCode = 1;
		    frame.printErr(e.getLocalizedMessage());			
		}
		
		System.exit(iReturnCode);	

	} // end main
} //end of class
