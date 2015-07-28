# bi_data_stage
The script is for stage data from production for Business Intelligence ETLs

1. Script bi_snapshot_stage.sh:



1.1 Variables:

PGUSER : default database user to connect

PGPASSWORD : password for database user

PGENGINE: location of the postgresql bin where the script is running

TRG_HOST: BI data stage host

DB: database name in TRG_HOST



1.2 Tables to be staged:

TABLES[N]="<host_address of source>:<database name>:<schema name>:<table name>:<mode>"
Mode should be refresh/snapshot
Examples:

TABLES[1]="10.5.1.2:reportdb:schema_partitions:events:snapshot"

TABLES[2]="10.5.1.2:reportdb1:schema_partitions:events_repo_$PREV_DT:refresh"

Here we have declared to import 2 tables from production - 1) events and 2) events_repo of yesterday

Function "fn_extract_parameter_values" extract the values from the array.

If mode is "refresh", the script drop the table if it already exists.

Else if mode is "snapshot", the script rename the table with table_name_$PREV_DT



1.3 Kill Connection:

In the beginning of the process, it kills all the connection to the target database.

1.4 Loop through process:

The parent process loop through the array to import all the tables declared here. It is sequential at this moment. Whenever process starts to import a table, it inserts a row in dba.staging_job_status with end_time null.

Then it imports the table with data, get the row count from source and target and match if it is matched or not. If matched then ok, if not, it truncates the target table and tries to import again. The job will try to import the same table maximum up to $v_max_try declared in the beginning.



1.5 Check the status of the job:

select * from dba.staging_job_status order by 1;



1.5.1 When the job is running:

 

 staging_job_status_id 	          start_time           	           end_time            	         comments          	 source_row_count 	 destination_row_count 	 completed 
 	 	 	 	 	 	 
9	 2015-07-28 01:29:01.983076-05 	 	 <<Parent job>>            	                  	                       	 
10	 2015-07-28 01:29:06.221193-05 	 2015-07-28 02:13:23.773344-05 	events            	57431354	57431360	 t
16	 2015-07-28 02:31:34.816309-05 	 	 events_repo_20150727 	 	 	





It means, the process is still running. It has completed events table import job and now it is running for events_repo_20150727.



1.5.2 When the job completes:



 staging_job_status_id 	          start_time           	           end_time            	         comments          	 source_row_count 	 destination_row_count 	 completed 
 	 	 	 	 	 	 
9	 2015-07-28 01:29:01.983076-05 	 2015-07-28 03:17:54.908045-05 	 <<Parent job>>            	                  	                       	 t
10	 2015-07-28 01:29:06.221193-05 	 2015-07-28 02:13:23.773344-05 	events            	57431354	57431360	 t
16	 2015-07-28 02:31:34.816309-05 	 2015-07-28 03:17:52.452345-05 	 events_repo_20150727 	25479340	25479340	 t


1.6 Cron jobs:

Cron job has been configured as below:

      0 1 * * * cd /opt/msp/pkg/postgres/dba/BIscripts && ./bi_snapshot_stage.sh.attmpp >>log/biupload_att.`date +\%F`.log 2>&1

So, there should be log in opt/msp/pkg/postgres/dba/BIscripts/log/biupload_att.2015-07-28.log



2. Configure the script to run for a database:

Create the followings in the target database - 

2.1 Create schemas 

2.2 Create sequences

2.3. Import parent tables from source

2.4. Create status table

 

