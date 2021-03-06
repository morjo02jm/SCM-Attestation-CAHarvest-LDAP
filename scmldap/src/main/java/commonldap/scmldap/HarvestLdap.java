package commonldap.scmldap;


import commonldap.commonldap.CommonLdap;
import commonldap.commonldap.JCaContainer;
import commonldap.commonldap.SDTicket;

import java.sql.*;
import java.util.*;


import org.json.*;


// Main Class
public class HarvestLdap {
	private static int iReturnCode = 0;
	private static CommonLdap frame;

	private static boolean bAttest = false;
	private static boolean deleteDisabledUsers = false;
	private static String sProblems = "";
	private static List<String> ticketProblems = new ArrayList<String>();
	
	// Repository columns
	private static String sTagUsername     = "USERNAME";
	private static String sTagRealname     = "REALNAME";
	private static String sTagAcctExternal = "ACCOUNTEXTERNAL";
	private static String sTagAcctDisabled = "ACCOUNTDISABLED";
	private static String sTagUserID       = "USERID";
	private static String sTagProject      = "PROJECT";
	private static String sTagContact      = "CONTACT";
	private static String sTagApp          = "APP";
	private static String sTagProduct      = "PRODUCT";
	
	// LDAP columns
	private static String sTagPmfkey       = "sAMAccountName";
	
	// Notification
	static String tagUL = "<ul> ";
	
	HarvestLdap()
	{
		// Leave blank for now
	}
	
	private static String combineAttributes(String sLastAccess, String sAccess) {
		// Combine the access levels
		String sToken = sAccess;
		String sCombAccess = sLastAccess;
		String sNext = "";
		
		while (!sToken.isEmpty()) {
			int mIndex = sToken.indexOf(';');
			if (mIndex > 0) {
				sNext = sToken.substring(0, mIndex);
				sToken = sToken.substring(mIndex+1);
			}
			else {
				sNext = sToken;
				sToken = "";							
			}
			
			if (!sCombAccess.contains(sNext)) {
				if (!sCombAccess.isEmpty())
					sCombAccess += ";";
				sCombAccess += sNext;
			}
		} // loop parsing out individual access tokens	
		
		return sCombAccess;
	}
	
	private static int readDBToRepoContainer(JCaContainer cRepoInfo,
			                                  String sJDBC,
            								  String sHarvestDBPassword,
            								  String sProjectFilter) {
		PreparedStatement pstmt = null; 
		String sqlStmt;
		int iIndex = 0;
		ResultSet rSet;
		boolean byState = false;
		
		String sqlError = "MSSQLServer. Unable to execute query.";
		
		try {			
			int nIndex, lIndex;
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			String sURL = sJDBC + "password=" + sHarvestDBPassword+";";
			nIndex = sJDBC.indexOf("jdbc:sqlserver://")+17;
			lIndex = sJDBC.indexOf(";databaseName=");
			String sAppInstance = sJDBC.substring(nIndex, lIndex);
			nIndex = lIndex+14;
			lIndex = sJDBC.indexOf(";integratedSecurity");
			String sBroker = sJDBC.substring(nIndex, lIndex);
			
			Connection conn = DriverManager.getConnection(sURL);
			
			sqlError = "SQLServer. Error reading Harvest records from broker, "+ sBroker + ".";
			sqlStmt = frame.readTextResource("Harvest_Attestation_DB_Query.txt", 
					                         sBroker, 
					                         !byState? "" : "S.statename,", 
					                         !byState? "order by 1,2,3,6,8,7" : "order by 1,2,3,4,7,9,8",
					                         sProjectFilter
					                        );
			pstmt=conn.prepareStatement(sqlStmt); 
			rSet = pstmt.executeQuery();
			String sLastRecord = "", sLastAccess = "", sLastUserGroup = "";
			
			while (rSet.next()) {		
				String sApp         = rSet.getString("APP").trim();
				sBroker             = rSet.getString("BROKER").trim();
				String sProject     = rSet.getString("ENVIRONMENTNAME").trim();
				String sState       = byState? rSet.getString("STATENAME").trim() : "***All***";
				String sUserID      = rSet.getString("USERNAME").trim();
				String sAcctExt     = rSet.getString("ACCOUNTEXTERNAL").trim();
				String sRealname    = rSet.getString("REALNAME");
				//sRealname           = sRealname==null? "" : sRealname.trim().replace(',', '|');
				sRealname           = sRealname==null? "" : sRealname.trim();
				String sUserGroup   = rSet.getString("USERGROUPNAME").trim().replace(',', ';');
				String sAccess      = rSet.getString("ACCESSLEVEL").trim().replace(',', ';');
				
				String sRecord      = sBroker+";"+sProject+";"+sState+";"+sUserID;
				
				if (sRecord.equalsIgnoreCase(sLastRecord)) {
					sAccess    = combineAttributes(sLastAccess, sAccess);
					sUserGroup = combineAttributes(sLastUserGroup, sUserGroup);
				}
				else {
					if (!sLastRecord.isEmpty())
						iIndex++;
				}
				cRepoInfo.setString("APP",             sApp,         iIndex);
				cRepoInfo.setString("APP_INSTANCE",    sAppInstance, iIndex);
				cRepoInfo.setString("BROKER",          sBroker,      iIndex);
				cRepoInfo.setString("PRODUCT",         "",           iIndex);
				cRepoInfo.setString("PROJECT",         sProject,     iIndex);
				cRepoInfo.setString("STATE",           sState,       iIndex);
				cRepoInfo.setString("CONTACT",         "",           iIndex);
				cRepoInfo.setString("USERID",          sUserID,      iIndex);
				cRepoInfo.setString("ACCOUNTEXTERNAL", sAcctExt,     iIndex);
				cRepoInfo.setString("REALNAME",        sRealname,    iIndex);
				cRepoInfo.setString("ACCESSLEVEL",     sAccess,      iIndex);
				cRepoInfo.setString("USERGROUP",       sUserGroup,   iIndex);
				
				sLastRecord = sRecord;
				sLastAccess = sAccess;
				sLastUserGroup = sUserGroup;
			} // loop over record sets
			
			if (!sLastRecord.isEmpty())
				iIndex++;
			
			if (iIndex>0)
				frame.printLog(">>>:"+iIndex+" Records Read From Harvest Broker, " +sBroker+ "(filter="+ sProjectFilter+").");
			
		} catch (ClassNotFoundException e) {
			iReturnCode = 101;
			frame.printErr(sqlError);
			frame.printErr(e.getLocalizedMessage());			
			System.exit(iReturnCode);
		} catch (SQLException e) {     
			iReturnCode = 102;
			frame.printErr(sqlError);
			frame.printErr(e.getLocalizedMessage());			
			System.exit(iReturnCode);
		}	

		return iIndex;
	}  
	
	private static void writeDBFromRepoContainer(JCaContainer cRepoInfo, String sImagDBPassword, String sBroker, String sFilter) {
		PreparedStatement pstmt = null; 
		String sqlStmt;
		int iResult;
		
		String sqlError = "";
		String sJDBC = "jdbc:sqlserver://AWS-UQAPA6ZZ:1433;databaseName=GMQARITCGISTOOLS;user=gm_tools_user;password="+sImagDBPassword+";";
		String sqlStmt0 = "insert into GITHUB_REVIEW "+
	              "( Application, ApplicationLocation, EntitlementOwner1, EntitlementOwner2, EntitlementName, EntitlementAttributes, ContactEmail, User_ID, UserAttributes) values ";
		
		String sEntitlementAttrs = "";
		String sUserAttrs = "";
		String sContact = "";
		String sValues = "";
		
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			Connection conn = DriverManager.getConnection(sJDBC);
	
			String sApp = "CA Harvest SCM";

			sqlError = "DB. Error deleting previous records.";
			sqlStmt = "delete from GITHUB_REVIEW where Application in ('"+ sApp + "')"+
			          " and EntitlementOwner1 like ('"+sBroker+"%')"+
					  " and EntitlementOwner2 like ('"+sFilter+"')";
			pstmt=conn.prepareStatement(sqlStmt);  
			iResult = pstmt.executeUpdate();
			if (iResult > 0) 
				frame.printLog(">>>:"+iResult+" Previous IMAG Feed Records Deleted.");
			
			sqlError = "DB. Error inserting record.";
			int nRecordsWritten = 0;
			int nBlock = 100;
			
			for (int iIndex=0,nRecords=0; iIndex<cRepoInfo.getKeyElementCount(sTagApp); iIndex++) {
				if (!cRepoInfo.getString(sTagApp, iIndex).isEmpty() &&
					!cRepoInfo.getString(sTagContact, iIndex).contains("exempt")) { 
					if (nRecords%nBlock == 0)
						sqlStmt = sqlStmt0;
					else 
						sqlStmt += " , ";
					
					sEntitlementAttrs = "product=" + cRepoInfo.getString("PRODUCT", iIndex);
					sUserAttrs = "external=" + cRepoInfo.getString("ACCOUNTEXTERNAL", iIndex) + ";" +
					             "access="   + cRepoInfo.getString("ACCESSLEVEL", iIndex) + ";" +
							     "group="    + cRepoInfo.getString("USERGROUP", iIndex) ;
					
					if (sUserAttrs.length()>255)
						sUserAttrs = "external=" + cRepoInfo.getString("ACCOUNTEXTERNAL", iIndex) + ";" +
					                 "access="   + cRepoInfo.getString("ACCESSLEVEL", iIndex);
					
//					sContact = cRepoInfo.getString(sTagContact,iIndex);
					String sContactEmail = "";
					String[] aContacts = frame.readAssignedApprovers(cRepoInfo.getString(sTagContact, iIndex));
					for (int j=0; j<aContacts.length; j++) {
						if (!sContactEmail.isEmpty())
							sContactEmail += ";";
						if (aContacts[j].equalsIgnoreCase("toolsadmin"))
							sContactEmail += "Toolsadmin@ca.com";
						else
							sContactEmail += aContacts[j]+"@ca.com";
					}
					
					sValues = "('"  + sApp + "',"+
							  "'"   + cRepoInfo.getString("APP_INSTANCE", iIndex) + "',"+
							  "'"   + cRepoInfo.getString("BROKER", iIndex) + "',"+
							  "'"   + cRepoInfo.getString(sTagProject, iIndex).replace("'", "''") + "',"+
							  "'"   + cRepoInfo.getString("STATE", iIndex) + "',"+
							  "'"   + sEntitlementAttrs + "',"+
							  "'"   + sContactEmail + "',"+
							  "'"   + cRepoInfo.getString(sTagUserID, iIndex) + "',"+
							  "'"   + sUserAttrs.replace("'", "''") + "')";
					
				    sqlStmt += sValues;
				    
				    if (nRecords%nBlock == (nBlock-1)) {
						pstmt=conn.prepareStatement(sqlStmt);  
						iResult = pstmt.executeUpdate();
						if (iResult > 0) 
							nRecordsWritten += iResult;	
						sqlStmt = "";
				    }
					nRecords++;	
				}
			} // loop over records
			
			if (!sqlStmt.isEmpty()) {
				pstmt=conn.prepareStatement(sqlStmt);  
				iResult = pstmt.executeUpdate();
				if (iResult > 0) 
					nRecordsWritten += iResult;					
			}
			frame.printLog(">>>:"+nRecordsWritten+" Inserted Records Made to DB.");
		
		} catch (ClassNotFoundException e) {
			iReturnCode = 301;
		    frame.printErr(sqlError);
		    frame.printErr(e.getLocalizedMessage());			
		    System.exit(iReturnCode);
		} catch (SQLException e) {     
			iReturnCode = 302;
		    frame.printErr(sqlError);
		    frame.printErr(e.getLocalizedMessage());			
		    System.exit(iReturnCode);
		}
	} // writeDBFromRepoContainer		
	
	
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
						
						if (sID.equalsIgnoreCase("harvestcscr")) {
							sID="Harvestcscr";
						}
						
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
					
					/* temp
					sqlStmt = "update haruserdata set accountdisabled='N' where usrobjid in (select usrobjid from haruser where username in ('harvestcscr') )";
					pstmt=conn.prepareStatement(sqlStmt);  
					iResult = pstmt.executeUpdate();
					*/						
					
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
		String sOutputFile = "";
		String sContactFile = "";
		String sImagDBPassword  = "";
		String sHarvestDBPassword = "";
		boolean bReport = false;
		
		// check parameters
		for (int i = 0; i < iParms; i++)
		{

			if (args[i].compareToIgnoreCase("-attest") == 0 )
			{
				bAttest = true;
			}
			else if (args[i].compareToIgnoreCase("-report") == 0 )
			{
				bReport = true;
			}			
			else if (args[i].compareToIgnoreCase("-del") == 0 )
			{
				deleteDisabledUsers = true;
			}						
			else if (args[i].compareToIgnoreCase("-bcc") == 0 )
			{
				sBCC = args[++i];
			}			
			else if (args[i].compareToIgnoreCase("-log") == 0 )
			{
				sLogPath = args[++i];
			}			
			else if (args[i].compareToIgnoreCase("-outputfile") == 0 )
			{
				sOutputFile = args[++i];
			}			
			else if (args[i].compareToIgnoreCase("-contactfile") == 0 )
			{
				sContactFile = args[++i];
			}			
			else {
					frame.printLog("Usage: scmldap [-attest | -del | -report [ -outputfile fullpathname ]] [-log pathname] [-h]");
					frame.printLog(" -attest option will display all users");
					frame.printLog(" -report option will generate an IMAG feed");
					frame.printLog(" -del  option will delete all external, disabled users");
					frame.printLog(" -outputfile option will write the report data to a designated file (.tsv)");
					frame.printLog(" -log  option specifies location of log file");
					System.exit(iReturnCode);
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
	        
	        String[] cscrBrokers = frame.getHarvestJDBCConnections();
	        
			// Show cLDAP statistics
			if (bAttest)
				frame.printLog("Broker\tDisplay Name\tPMFKEY\tDomain Account\tDisabled Account");
			
			JCaContainer cHarvestContacts = new JCaContainer();
			if (bReport) {
				if (!sContactFile.isEmpty())
					frame.sContactFile = sContactFile;
				frame.readSourceMinderContacts(cHarvestContacts, "Harvest", cLDAP);
			} // GovernanceMinder report

			JCaContainer cRepoInfo = new JCaContainer();
			
			for (int i=0; i<cscrBrokers.length; i++) {
				if (!bReport) {
					processHarvestDatabase( cscrBrokers[i], 
					           				cLDAP,
					           				sHarvestDBPassword);
				}
				else {		
					String sJDBC = cscrBrokers[i];
					int nIndex = sJDBC.indexOf("databaseName=")+13;
					int lIndex = sJDBC.indexOf(";integratedSecurity");
					String sBroker = sJDBC.substring(nIndex, lIndex);
					int mIndex = sBroker.indexOf('_');
					if (mIndex >= 0)
						sBroker = sBroker.substring(0, mIndex);
					
					String[] sProjectFilter = {"%", "A%", "B%", "C%", "D%", "E%", "F%", "G%", "H%", "I%", "J%", "K%","L%", "M%", "N%", "O%", "P%", "Q%", "R%", "S%", "T%", "U%", "V%", "W%", "X%", "Y%", "Z%"};
					
					for (int j=0; j<sProjectFilter.length; j++) {
						if (sBroker.equalsIgnoreCase("cscr1001")) {
							if (j==0) continue;
						}
						else {
							if (j>0) continue;
						}
							
						if (readDBToRepoContainer(cRepoInfo,
	                  			  cscrBrokers[i],
	                  			  sHarvestDBPassword,
	                  			  sProjectFilter[j]) > 0) {
							boolean bFound = false;
							
							// Apply contact information for records
							// a. from SourceMinder Contacts
							for (int iIndex=0; iIndex<cHarvestContacts.getKeyElementCount("Approver"); iIndex++) {
								String sProduct = cHarvestContacts.getString("Product", iIndex);
								String sLocation = cHarvestContacts.getString("Location", iIndex).toLowerCase();
								String[] sProjects = frame.readAssignedBrokerProjects(sLocation, sBroker);
								String[] sApprovers = frame.readAssignedApprovers(cHarvestContacts.getString("Approver", iIndex));
								boolean bActive = !cHarvestContacts.getString("Active", iIndex).contentEquals("N");
								boolean bExempt = cHarvestContacts.getString("Active", iIndex).contentEquals("E");
								String sReleases = cHarvestContacts.getString("Release", iIndex);
								
								if (sProjects.length > 0) {
									String sApprover = "";
									for (int jIndex=0; jIndex<sApprovers.length; jIndex++) {
										if (!sApprover.isEmpty()) sApprover += ";";
										sApprover += sApprovers[jIndex];
									}
									
									if (sApprover.isEmpty() && bActive) {
										sApprover = "toolsadmin"; // "inactive"
							    		if (sProblems.isEmpty()) 
							    			sProblems = tagUL;			    		
							    		sProblems+= "<li>The active Harvest product, <b>"+sProduct+"</b>, has no valid contact.</li>\n";									
									}
									
									for (int k=0; k<sProjects.length; k++) {
										if (sProjects[k].isEmpty()) {
											// process all the unassigned approvers in the project
											for (int kIndex=0; kIndex<cRepoInfo.getKeyElementCount(sTagProject); kIndex++) {
												if (cRepoInfo.getString(sTagContact, kIndex).isEmpty()) {
													cRepoInfo.setString(sTagContact, (bActive? (bExempt?"exempt":sApprover):"inactive"), kIndex);
													cRepoInfo.setString(sTagProduct, sProduct, kIndex);
													bFound = true;
												}
											}
										}
										else { // process each project prefix 
											String sPrefix = sProjects[k].replace("*", "").toLowerCase();
											
											for (int kIndex=0; kIndex<cRepoInfo.getKeyElementCount(sTagProject); kIndex++) {
												String sProject = cRepoInfo.getString(sTagProject, kIndex).toLowerCase();
												if (sProject.startsWith(sPrefix)) {
													boolean bIsActive = frame.processProjectReleases(sProject, sReleases, bActive);
													cRepoInfo.setString(sTagContact, (bIsActive?(bExempt?"exempt":sApprover):"inactive"), kIndex);
													cRepoInfo.setString(sTagProduct, sProduct, kIndex);
													bFound = true;
												}
											}											
										}  // list of prefixes present
									} // loop over project prefixes
								} 	// broker record exists in contact info					
							} // loop over contact records
							
							if (!bFound && j==0) {
								// Didn't find any contact entry for this broker
					    		if (sProblems.isEmpty()) sProblems = tagUL;
					    		sProblems+= "<li>The broker, <b>"+sBroker+"</b>, currently has no contact information from SourceMinder</li>\n";			    					    		
							}
							
							// Process all end of life projects (make them inactive projects in Harvest)
							for (int k=0; k<cRepoInfo.getKeyElementCount(sTagProject); k++) {
								String sProject = cRepoInfo.getString(sTagProject, k);
								if (cRepoInfo.getString(sTagContact, k).equalsIgnoreCase("inactive") &&
									!cRepoInfo.getString(sTagApp, k).isEmpty()) {
									if (frame.deactivateEndOfLifeProject(sBroker, sProject, sHarvestDBPassword)) {
							    		if (sProblems.isEmpty()) sProblems = tagUL;
							    		sProblems+= "<li>The source project, <b>"+sProject+"</b>, in broker, <b>"+sBroker+"</b>, has been deactived because the project is now End of Life.</li>\n";
							    		
										int[] iProjects = cRepoInfo.find(sTagProject, sProject);
										for (int iIndex=0; iIndex<iProjects.length; iIndex++) {
											cRepoInfo.setString(sTagApp, "", iProjects[iIndex]);
										}
									}
								} // end of life entry					
							} //loop over broker entries
							
							// Check for internal user accounts and marked them as unmapped
							for (int k=0; k<cRepoInfo.getKeyElementCount(sTagProject); k++) {
								if (!cRepoInfo.getString(sTagApp, k).isEmpty()) {
									String sID = cRepoInfo.getString(sTagUserID,k);
									if (!sID.endsWith("?")) {
										int[] iLDAP = cLDAP.find(sTagPmfkey, sID);
										if (iLDAP.length == 0) {
											int[] iUsers = cRepoInfo.find(sTagUserID, sID);
											for (int kIndex=0; kIndex<iUsers.length; kIndex++) {
												cRepoInfo.setString(sTagUserID, sID+"?", iUsers[kIndex]);
											}
										}
									}
								}
							}
							
							// Append records to output file, if any
							if (!sOutputFile.isEmpty()) {
								String sFile = sOutputFile.replace("broker", sBroker);
								frame.setFileAppend(i>0); 
								frame.writeCSVFileFromListGeneric(cRepoInfo, sFile, '\t', cLDAP);	
								frame.setFileAppend(false);
							}
							
							// Write out processed records to database
							writeDBFromRepoContainer(cRepoInfo, sImagDBPassword, sBroker, sProjectFilter[j]);
							
							cRepoInfo.clear();
						}
					}	//loop over filters				
					
				} // -report flag set
			} // loop over brokers
			
			if (!sProblems.isEmpty()) {
				String email = frame.expandDistributionListforEmail("cn=Team - GIS - githubcom - Tools Services - Contacts,ou=self service groups,ou=groups", cLDAP);
				String sSubject, sTicket, sScope;
				if (email.startsWith(";"))
					email = email.substring(1);
				
				sSubject = "Notification of CA Harvest SCM Governance Problems and Changes";
				sScope = "Harvest SQLServer Database";
				sTicket = "Application:UserAssistance Harvest CSCR";
				
		        //create a service desk ticket from ticketProblem
				frame.createServiceTicket(sProblems, sTicket, ticketProblems, "", "");
				
				sProblems+="</ul>\n";				
		        String bodyText = frame.readTextResource("Notification_of_Noncompliant_Harvest_Contacts.txt", sScope, sProblems, "", "");								        								          
		        frame.sendEmailNotification(email, sSubject, bodyText, true);
			} // had some notifications
		
		} catch (Exception e) {
			iReturnCode = 1;
		    frame.printErr(e.getLocalizedMessage());			
		}
		
		System.exit(iReturnCode);	

	} // end main
} //end of class
