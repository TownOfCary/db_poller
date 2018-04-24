# common.pm
# This is a library of commonly used functions
# Developed by Wilson Farrell for use within the Town of Cary.
#
#    Copyright (C) 2013 Town of Cary, NC (Wilson Farrell, Developer)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.


use DBI;
use Date::Format;
use Time::Local;
use Text::CSV_XS;
use File::Basename qw(dirname);
use Cwd  qw(abs_path);

 use constant {
        SEVERE   => 0,
        WARNING   => 1,
        NOTE  => 2,
        INFO  => 3,
        DEBUG   => 4,
        VERBOSE => 5,
    };
####### Standard functions #######
sub posixTimeToDB {
    my ($ptime) = @_;
    my ($lsec,$lmin,$lhour,$lday,$lmon,$lyear) = localtime($ptime);
    return sprintf "%d-%02d-%02d %02d:%02d:%02d", $lyear + 1900, $lmon + 1,$lday,$lhour,$lmin,$lsec;
    
}

sub posixTimeToReg {
    my ($ptime) = @_;
    my ($lsec,$lmin,$lhour,$lday,$lmon,$lyear) = localtime($ptime);
    return sprintf "%02d/%02d/%d %02d:%02d:%02d", $lmon + 1,$lday,$lyear + 1900, $lhour,$lmin,$lsec;
    
}

sub DBTimetoPosix {
    my ($dbtime) = @_;
    if ($dbtime =~ /(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)/) { 
	my $year = $1 - 1900;
	my $month = $2 - 1;
	my $day = $3;
	my $hour = $4;
	my $min = $5;
	my $sec = $6;
	return timelocal($sec,$min,$hour,$day,$month,$year);
    } 
    return 0;
}

sub waitFor {
    my ($waitForSecs) = @_;
    return if ($waitForSecs == 0);
	
    my $startTime = 0;
    my $time = time();
    waitUntil($time + $waitForSecs);
}

sub waitUntil {
    my ($startTime) = @_;
    my $notTime = 1;
    
    sayLog("Will Run again at " . time2str('%c',$startTime),INFO);
    $waiting = 1;
    while ($notTime == 1) {
        $time = time();
        if ($time < $startTime) {
            if (($startTime - $time) <= 60) {
                sayLog ("Waiting for " . ($startTime - $time) . " secs",DEBUG);
                &pauseFor($startTime - $time);
            } else {
                sayLog ("Waiting for " . (($startTime - $time) / 2) . " secs",DEBUG);
                &pauseFor(($startTime - $time) / 2);
            }
        } else {
            $notTime = 0;
        }
    }
    $waiting = 0;
}

sub pauseFor {
    my ($secs) =@_;
    select(undef, undef, undef, $secs);
} 


sub trim($) {
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}

sub flatten {
  map { ref $_ ? flatten(@{$_}) : $_ } @_;
}

sub setupStdout {
    ($filename_prefix,$eol,$tocc_log_level) = @_;

    
    $tocc_log_level = INFO unless (defined $tocc_log_level);

}

sub sayOut {
    my ($msg) = @_; 
    my $myeol = (defined $eol && $eol ne "") ? $eol : "\r\n";
    print time2str("%c",time()) . ": " . $msg . $myeol;
    $|++;
}



sub sayLog {
    my ($msg,$lev_in) = @_;
    $lev_in = INFO unless (defined $lev_in);
    $tocc_log_level = INFO unless (defined $tocc_log_level);
    if ($lev_in <= $tocc_log_level || $lev_in <= SEVERE) {
        if (defined $filename_prefix && $filename_prefix ne "") {
            $logdir = File::Basename::dirname(File::Basename::dirname(Cwd::abs_path($0))) . '/logs';
            if(!-d $logdir){
                mkdir($logdir);
            }
            my $myeol = (defined $eol && $eol ne "") ? $eol : "\r\n";
            my $outFile = $logdir . '/' .$filename_prefix . time2str("%Y%m",time()) .".txt";
            open (LOGFILE, ">> $outFile") or die "Log File not available";
            print LOGFILE time2str("%c",time()) . ": " . $msg . $myeol;
            close(LOGFILE); 
        } else {
            sayOut($msg);
        }
    }
   
}

sub sayDie {
    my ($msg) = @_;
    sayLog($msg,SEVERE);
    exit;
}

sub print_CSV_of_a_of_h {
    my ($file, $a_ref) = @_;
    my @array = @$a_ref;
    my @fields = keys %{$array[0]};
    my $csv = Text::CSV_XS->new({always_quote => 1,binary => 1, eol => "\r\n"});
    $csv->combine(@fields);
    print $file $csv->string();
    for (my $i = 0; $i < scalar(@array); $i++) {    
        my @tmparray = ();
        foreach my $field (@fields) {
            push @tmparray, $array[$i]{$field};
        }
        $csv->combine(@tmparray);
        print $file $csv->string();
    }
}

sub read_csv {
    my ($filename) = @_;
    my $csv = Text::CSV_XS->new ({ binary => 1, eol => "\n" });
    open my $io, "<", $filename or die "$filename: $!";
    $csv->column_names($csv->getline($io));
    my @returnme = ();
    while (my $hr = $csv->getline_hr($io)) {
        push @returnme, {%$hr};
#	print $hr;
    }
    close $io;
    return @returnme;
}

############ SQL Helper Functions ################
sub sql_exec_ns {
    my ($st,$dbh) = @_;
    if ($tocc_updatedb == 1) {
	
	sayLog("Sending SQL: $st",DEBUG);
	my $sql_st = $dbh->prepare($st);
	$sql_st->execute()
	    or sayDie("Cannot execute SQL statement $DBI::errstr\n");
    } else {
	sayLog("NOT Sending SQL: $st",DEBUG);
    }
}

sub sql_exec_s {
    my ($st,$dbh) = @_;
    sayLog("Sending SQL: $st",DEBUG);
    my $sql_st = $dbh->prepare($st);
    $sql_st->execute()
        or sayDie("Cannot execute SQL statement $DBI::errstr\n");
    my @result;
    while ( my $hash_ref = $sql_st->fetchrow_hashref() )
    {
        push @result, {%$hash_ref};
    }

    sayDie("Data fetch terminated by error $DBI::errstr\n") if $DBI::errstr;
    return @result;
}

sub db_connect {
    my ($dsn,$login,$password,$updatedb) = @_;
    $tocc_updatedb = $updatedb;
    sayLog("Attempting to Connect to $dsn",DEBUG);
    if (my $dbh = DBI->connect ( "dbi:ODBC:$dsn", $login, $password, {PrintError => 1})) {
	sayLog("$dsn Connected",INFO);
	return $dbh;
    } 
    sayLog("connection failed to database $dsn with $login $DBI::errstr\n",SEVERE); 
    return;
    
}

sub db_disconnect {
    my ($dbh,$dsn) = @_;
    sayLog("Attempting to disconnect from $dsn",DEBUG);
    # Disconnect
    $dbh->disconnect or warn "disconnection failed $DBI::errstr\n";
    sayLog("$dsn Disconnected",INFO);
}

sub qp {
    my ($string,$len,$dbh) = @_;
    if ($len < 1) {
	return $dbh->quote($string);
    } 
    return $dbh->quote(sprintf substr($string,0,$len), "\%-".$len."s");
}


1;
