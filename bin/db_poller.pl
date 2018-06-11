#!/usr/bin/perl
#
# perl db_poller.pl
#
#

# Load a bunch of stuff
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname(abs_path($0))) . '/lib';
my $confdirname = dirname(dirname(abs_path($0))) . '/conf';

use TOC::Common;
use XML::LibXML;
use Config::Simple;
use REST::Client;
use MIME::Base64;
use Data::Dumper;



get_global_config_vals($confdirname . "/db_poller.ini");

$gbl_state_file = $confdirname . "/db_poller_state.txt";

my $eol = "\r\n";


# Setup the "say" output location (common.pl)
setupStdout("dp_poller_",$eol,$gbl_log_level);

# run it
poll_server();

#################################
# Main execution loop
# Check to see if there are any updates to the database since the last time
# run the boomi interface is there are.
#################################
sub poll_server {
	my $dsn = "Driver={$gbl_db_driver};Server=$gbl_db_server;Database=$gbl_db_dbname";
	$gbl_dbh = db_connect($dsn,$gbl_dblogin,$gbl_dbpassword,$gbl_updatedb);
	while (!$gbl_dbh) {
		sayLog("Cannot connect to database, waiting $gbl_wait_time seconds to reconnect");
		waitFor($gbl_wait_time);
		$gbl_dbh = db_connect($dsn,$gbl_dblogin,$gbl_dbpassword,$gbl_updatedb);
	}
	$gbl_need_to_cache_latest = 0;
	while(1) {
		
		my $last_event_id = get_last_event_id();
		my $latest_event_id = get_last_event_since_from_db($last_event_id); 
		if ($last_event_id != -1 && $latest_event_id != -1) {
			if ($latest_event_id > $last_event_id) {
				sayLog("Update required detected",INFO);
				if (trigger_boomi()) {
					sayLog("Boomi triggered",INFO);
					$gbl_need_to_cache_latest= 1;
				} else {
					sayLog("Boomi failed to trigger",SEVERE);
					$gbl_need_to_cache_latest= 0;
				}
			}
			if ($gbl_need_to_cache_latest == 1) {
			#if () {
				$gbl_need_to_cache_latest = 0;
				sayLog ("Printing to latest cache",DEBUG);
				save_latest_event_id($latest_event_id);
			}
		} else {
			db_disconnect($gbl_dbh,$dsn);
			sayLog("database query failed, reconnecting to database");
			$gbl_dbh = db_connect($dsn,$gbl_dblogin,$gbl_dbpassword,$gbl_updatedb);
		}
		sayLog("Waiting $gbl_wait_time seconds",DEBUG);
		waitFor($gbl_wait_time);
	}
}

#################################
# read in a bunch of global configuration options from the .ini file
#################################
sub get_global_config_vals {
	my ($cfg_file) = @_;
	my $cfg = new Config::Simple($cfg_file) or sayDie("Unable to load config file at $cfg_file");

	$gbl_dblogin = $cfg->param("traffic_db_user");
	$gbl_dbpassword = $cfg->param("traffic_db_pass");
	$gbl_db_driver = $cfg->param("traffic_db_driver");
	$gbl_db_server = $cfg->param("traffic_db_server");
	$gbl_db_dbname = $cfg->param("traffic_db_dbname");

	$gbl_boomi_user = $cfg->param("boomi_user");
	$gbl_boomi_pass = $cfg->param("boomi_pass");
	$gbl_boomi_process_id = $cfg->param("boomi_process_id");
	$gbl_boomi_atom_id = $cfg->param("boomi_atom_id");
	$gbl_boomi_account_id = $cfg->param("boomi_account_id");

	$gbl_wait_time = int($cfg->param("interval"));
	$gbl_wait_time = 1 if ($gbl_wait_time < 1);

	$gbl_log_level = $cfg->param("log_level");
	$gbl_updatedb = (($cfg->param("updatedb") eq 'true') ? 1 : 0);
	
	@gbl_event_types = $cfg->param("traffic_event_types");
}

#################################
# get_last_event_id()
# looks to see if there is a cached last_event_id on disk
# if not gets the last event id from the database of the event types
# SELECT MAX(ID) as max_id
# FROM [Logs].[dbo].[EventLog]
#  WHERE EventTypeID IN (<EVENT_TYPES>)
#################################

sub get_last_event_id {
	my $state = new Config::Simple($gbl_state_file);
	if (defined $state) {
		my $last_id = $state->param("last_event_id");	
		if ($last_id =~ /^\d+$/) {
			return int($last_id);
		} else {
			$gbl_need_to_cache_latest = 1;
			return get_last_event_from_db();
		}
	} else {
		$gbl_need_to_cache_latest = 1;
		return get_last_event_from_db();
	}
}

#################################
# Query the database for the last ID in the data base 
# correctonding to one of our event types
#################################
sub get_last_event_from_db {
	my @tmp_last_id = sql_exec_s("SELECT MAX(ID) as max_id FROM dbo.EventLog WHERE EventTypeID IN (" . convert_event_types() . ")",$gbl_dbh,1);
	if (@tmp_last_id) {
		return int(@tmp_last_id[0]->{max_id});
	} 
	return -1;
}

#################################
# Query the database for the last ID in the data base 
# correctonding to one of our event types, since the last
# ID.  This just shortens the query time, but it's not really
# logically any different than get_last_event_from_db.  If there
# are a lot of events coming in, we want to wait until
# the system quiesces.  We will wait a second to see if
# any more events come in, and continue this until
# no more events are coming in.  
#################################
sub get_last_event_since_from_db {
	my ($since) = @_;
	my $last_event_prev = $since;
	my $last_event_curr = -1;
	while ($last_event_prev != $last_event_curr) {
		if ($last_event_curr != -1) {
			$last_event_prev = $last_event_curr;
		}
		my @tmp_last_id = sql_exec_s("SELECT MAX(ID) as max_id FROM dbo.EventLog WHERE EventTypeID IN (" . convert_event_types() . ") AND ID >= $last_event_prev",$gbl_dbh,1);
		if (@tmp_last_id) {
			$last_event_curr = int(@tmp_last_id[0]->{max_id});
		} else {
			return -1;
		}
		if ($last_event_prev != $last_event_curr) {
			sayLog("Waiting a sec to ensure the system is quiesced",INFO);
			waitFor(1);
		} else {
			sayLog("nothing to see here... move along.",INFO);
		}
	} 

	return $last_event_curr;
}

#################################
# Create a comma, single quoted strin of the list of event types
# for use in the IN statement in sql
#################################
sub convert_event_types {
	my @tmplist = ();
	foreach my $type (@gbl_event_types) {
		push @tmplist, "\'" . $type . "\'";
	}
	return join(",",@tmplist);
}

#################################
# Save the latest ID back to the state file.  Create the state file
# if it does not exist.
#################################
sub save_latest_event_id {
	my ($latest) = @_;
	my $state = new Config::Simple($gbl_state_file);
	if (defined $state) {
		$state->param("last_event_id",$latest);
		$state->save();
	} else {
		$state = new Config::Simple(syntax=>'http');
		$state->param("last_event_id",$latest);
		$state->write($gbl_state_file);
	}
}

#################################
# Call boomi to start the integration to push new events to Salesforce
#################################
sub trigger_boomi {
	sayLog("Need to Trigger Boomi",INFO);
	my $rest_client = setup_rest_client();
	my $per = XML::LibXML::Element->new("ProcessExecutionRequest");
	$per->setAttribute("processId",$gbl_boomi_process_id);
	$per->setAttribute("atomId",$gbl_boomi_atom_id);
	$per->setAttribute("xmlns","http://api.platform.boomi.com/");
	#$per->addChild(createProcessPropertyChild("dpp_disregard_last_sync_time","TRUE"));
	sayLog($per->toString(),5);
	if ($gbl_updatedb == 1) {
		$rest_client->POST("https://api.boomi.com/api/rest/v1/$gbl_boomi_account_id/executeProcess",$per->toString(),$gbl_post_headers);
		return check_for_api_errors($rest_client);
	} else {
		sayLog("not calling boomi (updatedb off)",INFO);
	}
	return 1;

}

#################################
# helper function to create arbitrary process properties for the boomi job
# to run with.
#################################
sub createProcessPropertyChild {
	my ($prop,$val) = @_;
	my $procProp = XML::LibXML::Element->new("ProcessProperty");
	my $name_child = $procProp->addChild(XML::LibXML::Element->new("Name"));
	$name_child->addChild(XML::LibXML::Text->new( $prop ));
	my $val_child = $procProp->addChild(XML::LibXML::Element->new("Value"));
	$val_child->addChild(XML::LibXML::Text->new( $val ));
	return $procProp;
}

#################################
# setup the rest client
#################################
sub setup_rest_client {
    my $rest_client = REST::Client->new(timeout => 10);
    $rest_client->getUseragent->ssl_opts(verify_hostname => 0,SSL_verify_mode => SSL_VERIFY_NONE);
    $rest_client->getUseragent->show_progress(2);
    $gbl_get_headers = {Accept => 'application/xml', Authorization => 'Basic ' . MIME::Base64::encode_base64($gbl_boomi_user . ':' . $gbl_boomi_pass)};
    $gbl_post_headers = {'Content-Type' => 'application/xml', Authorization => 'Basic ' .MIME::Base64::encode_base64($gbl_boomi_user . ':' . $gbl_boomi_pass)};
    #sayLog(Data::Dumper->Dumper($post_headers));
    return $rest_client;
}

#################################
# looks for API errors; return 0 if one is detected
# otherwise 1
#################################
sub check_for_api_errors() {
	my ($rest_client) =@_;
    my $rcode = $rest_client->responseCode();
    my $rcontent = $rest_client->responseContent();
    if (($rcode != 200) && ($rcode != 201)) {
		sayLog("failed: $rcode",SEVERE);
		return 0;
    } elsif (grep /<error>/, $rcontent) {
		my $errxml = $rcontent;
		sayLog("error: $errxml",SEVERE);
		return 0;
    } 
    return 1;
} 
