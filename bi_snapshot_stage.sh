#!/bin/bash
#
# This script is installed on "wdmg12"
#exit 1 # Stop run
LOG=/opt/msp/pkg/postgres/dba/BIscripts/import_ad_ed_vsp.log
export PGPASSWORD=1q2w3e4r PAGER=less PGUSER=postgres
PGENGINE=/opt/msp/pkg/postgres/bin
TRG_HOST="10.3.1.1"
DB="database"
echo "job started ..."
echo "$(date)"

PREV_DT=`date "--date=${date} -1 day" +%Y%m%d`
TODAY=`date +%Y%m%d`


TABLES[1]="10.5.1.2:reportdb:schema_partitions:events:snapshot"
TABLES[2]="10.5.1.2:reportdb1:schema_partitions:events_repo_$PREV_DT:refresh"



tLen=${#TABLES[@]}
v_allowable_diff=50
v_max_try=2

########
fn_extract_parameter_values () {
  SRC_HOST=`echo ${TABLES[$i]} | cut -f1 -d ':'`
  SRC_DB=`echo ${TABLES[$i]} | cut -f2 -d ':'`
  SCHEMA_NAME=`echo ${TABLES[$i]} | cut -f3 -d ':'`
  TABLE_NAME=`echo ${TABLES[$i]} | cut -f4 -d ':'`
  v_MODE=`echo ${TABLES[$i]} | cut -f5 -d ':'`
  SNAPSHOT_TABLE_NAME=${TABLE_NAME}_${PREV_DT}

}


fn_kill_conn () {
printf "\nkilling existing connections to Target $DB ..."
$PGENGINE/psql -1  -h $TRG_HOST -U postgres -c "SELECT pg_terminate_backend(procpid) as killed from pg_stat_activity where datname = '$DB'" -t -A
printf "DONE\n"
}


fn_drop_rename_tables () {
	vCMD="BEGIN;"
	for ((i=1;i<=$tLen;i++))
	do
	   fn_extract_parameter_values
	   if [ $v_MODE = 'snapshot' ]; 
	   then
	      v_found=0
        # if table exists, then find out table_name_prev_dt exist or not, if not, then rename 
	      v_found=`$PGENGINE/psql -h  $TRG_HOST  $DB -U postgres -c "SELECT count(1) FROM pg_stat_user_tables WHERE relname = '${TABLE_NAME}' AND schemaname = '${SCHEMA_NAME}'" -t -A`

	      if [ ${v_found} -gt 0 ]; 
	      then
	         #looking for snapshot table; if not found then rename; if found, then drop table
           v_found=`$PGENGINE/psql -h  $TRG_HOST  $DB -U postgres -c "SELECT count(1) FROM pg_stat_user_tables WHERE relname = '${TABLE_NAME}_${PREV_DT}' AND schemaname = '${SCHEMA_NAME}'" -t -A`
           if [ ${v_found} -eq 0 ];   # snapshot table found
	         then
			         vCMD=${vCMD}"ALTER TABLE ${SCHEMA_NAME}.${TABLE_NAME} RENAME TO ${TABLE_NAME}_${PREV_DT};"
			         #Dependent index
			         vSQL="select string_agg('alter index '|| schemaname ||'.'||indexrelname ||' rename to ' || indexrelname ||'_${PREV_DT}',';') from pg_stat_user_indexes  where  relname = '${TABLE_NAME}' and schemaname = '${SCHEMA_NAME}'"
			         vSTR=`$PGENGINE/psql -h $TRG_HOST $DB -c "${vSQL}" -t -A`
			         if [ -n "${vSTR}" ];
			         then
			           vCMD="${vCMD}${vSTR};"
			         fi
			         #Dependent sequences     
			         vSQL="select string_agg('alter sequence '|| schemaname ||'.'||cs.relname ||' rename to ' || cs.relname ||'_${PREV_DT}',';') from pg_class as cs inner join pg_depend as d on cs.oid = d.objid and d.deptype in ('a','n') and cs.relkind = 'S' inner join pg_stat_user_tables as t on d.refobjid = t.relid  where t.relname = '${TABLE_NAME}' and t.schemaname = '${SCHEMA_NAME}'"
			         vSTR=`$PGENGINE/psql -h $TRG_HOST $DB -c "${vSQL}" -t -A`
			         if [ -n "${vSTR}" ];
			         then
			           vCMD="${vCMD}${vSTR};"
			         fi
		       else
				      vCMD=${vCMD}"DROP TABLE IF EXISTS ${SCHEMA_NAME}.${TABLE_NAME} CASCADE;"
		       fi    
	      fi ##v_found  
	   elif [ $v_MODE = 'refresh' ];
	   then
	      vCMD=${vCMD}"DROP TABLE IF EXISTS ${SCHEMA_NAME}.${TABLE_NAME} CASCADE;"

	     	     # echo $vCMD
	   else
	      echo "!FATAL! Unknown option ${vMODE} for ${TABLE_NAME} "
	      exit 1
	   fi
	done
	vCMD=${vCMD}"COMMIT;"
	#echo $vCMD
	
$PGENGINE/psql -1 -h $TRG_HOST $DB <<_eof_
	   \set ON_ERROR_STOP on
	    $vCMD
_eof_
	if [ $? -gt 0 ];
	then
	  printf "\n FATAL: Unknown error ocured during drop table\n"
	  exit 1
	fi
} #fn_drop_rename_tables 

fn_restore_data () {
  
  vTry=1
  while true
  do
  
  v_pgdump_fail=0
     cmd="$PGENGINE/pg_dump   -h $SRC_HOST  -t ${SCHEMA_NAME}.$TABLE_NAME $SRC_DB --disable-triggers   |  $PGENGINE/psql -1 -h $TRG_HOST $DB"
     echo $cmd
  	$PGENGINE/pg_dump   -h $SRC_HOST  -t ${SCHEMA_NAME}.$TABLE_NAME $SRC_DB --disable-triggers   |  $PGENGINE/psql -1 -h $TRG_HOST $DB 

		v_pgdump_fail=`echo "${PIPESTATUS[0]}"`
		if [ ${v_pgdump_fail} -gt 0 ];
	  then
	      if [ ${vTry} -le ${v_max_try} ];
	      then
	         printf "\nPgDump failed and retrying ...\n"
	         let vTry=${vTry}+1
                  $PGENGINE/psql -1 -h $TRG_HOST  $DB -c "TRUNCATE TABLE ${SCHEMA_NAME}.${TABLE_NAME}"
	         continue
	      else
		      echo "FATAL: error occured during data dump for table = ${SCHEMA_NAME}.$TABLE_NAME and source host = $SRC_HOST"
	    		echo "$(date)"
	    		exit 1
	      fi  #if [ ${vTry} -le 3 ];
   else
       printf "\nData restore completed for ${TABLE_NAME} \n"
       break
   fi
  
    let vTry=${vTry}+1  
  done
	
}
########



v_job_status_id=`$PGENGINE/psql -h $TRG_HOST  $DB -U postgres -c "insert into dba.staging_job_status ( start_time,comments) values ( now(), '<<Parent job>>') returning staging_job_status_id" -t -A -q`

fn_kill_conn 

printf "\nDropping tables ...\n"
fn_drop_rename_tables
printf "\nDONE\n"



printf "\nData copy started ...\n"
echo "$(date)"
for ((i=1;i<=$tLen;i++))
do
  v_job_status_id_table=0
  fn_extract_parameter_values #this will extract table name from array
  v_job_status_id_table=`$PGENGINE/psql -h $TRG_HOST  $DB -U postgres -c "insert into dba.staging_job_status ( start_time, comments) values ( now(), '"${TABLE_NAME}"' ) returning staging_job_status_id" -t -A -q`

  printf "\nTable = $TABLE_NAME\n"
  echo "$(date)"  

  v_src_count=`$PGENGINE/psql -U postgres -h $SRC_HOST  $SRC_DB -c "SELECT count(1) FROM ${SCHEMA_NAME}.$TABLE_NAME" -t -A`
  #v_src_count=0
  v_pgdump_fail=0
  fn_restore_data 
   
  v_trg_count=`$PGENGINE/psql -U postgres -h $TRG_HOST  $DB -c "SELECT count(1) FROM ${SCHEMA_NAME}.$TABLE_NAME" -t -A`
  
  printf "\nComparing number of records ...\n"
  printf  "\nRecords in source = $v_src_count"
  printf  "\nRecords in target = $v_trg_count"
  let vDiff=${v_src_count}-${v_trg_count}
  #vDiff=0
  if [ $vDiff -gt ${v_allowable_diff} ];
  then
    printf "\nNot Matched"  
    printf "\nFATAL: $TABLE_NAME snapshot was not imported successfully, more than ${v_allowable_diff} records missed\n"
    exit 1
  else
    printf "\nDONE"
  fi  
  
  printf "\nGranting permissions ...\n"
  
$PGENGINE/psql -h $TRG_HOST $DB <<_eof_
   \set ON_ERROR_STOP on
    begin;
        alter table ${SCHEMA_NAME}.$TABLE_NAME owner to user1;
        grant all on table ${SCHEMA_NAME}.$TABLE_NAME to user1;
        grant all on table ${SCHEMA_NAME}.$TABLE_NAME to user2;
        grant select on table ${SCHEMA_NAME}.$TABLE_NAME to readonly;

   commit; 
_eof_
  printf "\nDONE\n"
  echo "$(date)"
  printf "SUCCESS : $TABLE_NAME import done\n "
  $PGENGINE/psql -h $TRG_HOST  $DB -U postgres -c "update dba.staging_job_status set end_time = now(), completed=true, source_row_count=${v_src_count}, destination_row_count=${v_trg_count}  where staging_job_status_id = '${v_job_status_id_table}'" -t -q

#
done # data copy for ((i=1;i<=$tLen;i++))
$PGENGINE/psql -h $TRG_HOST  $DB -U postgres -c "update dba.staging_job_status set end_time = now(), completed=true where staging_job_status_id = '${v_job_status_id}'" -t -q

echo "$(date): job completed"





