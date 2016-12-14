# scmldap
Harvest User Account Processing and Attestation Reporting.

Usage: scmldap [-attest | -report  [-outputfile fullpathname] | -del] [-log pathname] [-h]
	 -attest option will display all users
     -report option will create an attestation and load it into the GM IMAG DB
     -del  option will delete all external, disabled users
	 
     -outputfile option will write the report data to a designated file (.tsv)
     -log  option specifies location of log file

Prerequisites (Environment):
	HARVEST_DB_PASSWORD			AES Encrypted value for Harvest database user (i.e. rtc_user) password.
	IMAG_DB_PASSWORD			AES Encrypted value for Governance Minder database user, gm_tools_user.
	DL_ADMINISTRATOR_PASSWORD	AES Encrypted value for the DL owner (i.e. Toolsadmin@ca.com) password.
	
Files:

HarvestLdap.java									Class file implementing the administrative processes for Harvest.
Harvest_Attestation_DB_Query.txt					Template for extracting Harvest attestation data.  
                                                    Used in Harvest SCM customization of the haccess.exe
SourceMinder_Contact_DB_Query.txt					Query for extracting product/contact information from CA SourceMinder
Notification_of_Noncompliant_Harvest_Contacts.txt	Email Notification template for issues discovered during processing
SourceMinder_Product_Contacts.tsv					ToolsServices worksheet for holding CA SourceMinder contact information for
                                                    Harvest and Endevor.
