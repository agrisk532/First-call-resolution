#!/usr/bin/perl

use DBI;
use strict;
use utf8;

my $driver   = "Pg"; 
my $database = "infomart";
my $dsn = "DBI:$driver:dbname = $database;host = xxx;port = xxxx";
my $userid = "xxxx";
my $password = "xxxx";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
   or die $DBI::errstr;
print "Opened database successfully\n";

my $stmt = qq(select first_call, caller, agentid, agent_name, second_call, time_diff_minutes, disposition_code, 
	(case when time_diff_minutes < 24*60 then 24 
		when time_diff_minutes > 24*60 and time_diff_minutes < 48*60 then 48
		when time_diff_minutes > 48*60 and time_diff_minutes < 168*60 then 168
		else 0 end) as category,
		(case when time_diff_minutes < 24*60 then 1 else 0 end) as category_24,
		(case when time_diff_minutes > 24*60 and time_diff_minutes < 48*60 then 1 else 0 end) as category_48,
		(case when time_diff_minutes > 48*60 and time_diff_minutes < 168*60 then 1 else 0 end) as category_168,
		(case when time_diff_minutes > 168*60 then 1 else 0 end) as category_infinity from
(select first_call, caller, agentid, agent_name, second_call, 
	extract(days from (second_call - first_call))*24*60 + 
	extract(hours from (second_call - first_call))*60 +
	extract(minutes from (second_call - first_call)) as time_diff_minutes, concat('DC',floor(random()*100)) as disposition_code from 
(select first_call, caller, agentid, agent_name, second_call from
(select distinct if_.start_ts_time as first_call, source_address as caller, resource_name as agentid, concat(agent_first_name,' ',agent_last_name) as agent_name,
(
   SELECT
   		MIN(if__.start_ts_time)
		from interaction_fact_gi2 if__ 
			join interaction_resource_fact_gi2 irf__ using(interaction_id)
			join resource_gi2 r__ using(resource_key)
			join interaction_type_gi2 it__ on if__.interaction_type_key=it__.interaction_type_key
			join media_type mt__ on if__.media_type_key=mt__.media_type_key
				where mt__.media_name='Voice'
				and if__.source_address<>''
				and r__.agent_first_name is not null
				and r__.agent_last_name is not null 
				and r__.agent_first_name<>''
				and r__.agent_last_name<>''
				and if__.start_ts_time > if_.start_ts_time 
				and if__.source_address=if_.source_address
)  AS second_call 
from interaction_fact_gi2 if_ 
	join interaction_resource_fact_gi2 irf_ using(interaction_id)
	join resource_gi2 r_ using(resource_key)
	join interaction_type_gi2 it_ on if_.interaction_type_key=it_.interaction_type_key
	join media_type mt_ on if_.media_type_key=mt_.media_type_key
		where media_name='Voice'
		and source_address<>''
		and agent_first_name is not null 
		and agent_last_name is not null 
		and agent_first_name<>'' 
		and agent_last_name<>''
--		and to_char(if_.start_ts_time,'YYYY') = '2017'
--		and to_char(if_.start_ts_time,'MM') = '10'
 		order by if_.start_ts_time asc
) t
) t1
) t2
where second_call is not null
);
my $sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;
if($rv < 0)
{
   print $DBI::errstr;
}

my $filename = 'report.csv';
open(FH, '>', $filename) or die "Could not open file '$filename' $!";

$sth->{RaiseError} = 1;
my $ref = $sth->fetchall_arrayref;
print "Number of rows returned is ", 0 + @{$ref}, "\n";

my $fields = join(';', @{ $sth->{NAME} });
print FH "$fields\n";

foreach my $r (@{$ref})
{
    print FH join(";", @{$r}), "\n";
}

close FH;
print "Operation done successfully\n";
$dbh->disconnect();
