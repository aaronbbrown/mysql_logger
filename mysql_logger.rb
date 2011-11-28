#!/usr/bin/ruby 
# mysql_logger.rb
# Author: Aaron Brown <abrown@ideeli.com>
# Log full processlist every second to syslog

require 'optparse'
require 'rubygems'        
#require 'daemons'
require 'dbi'
require 'syslog'

# This fixes a bug in the library where an unknown type will cause
# exceptions to be raised, rather than using a reasonable default: String
# coercion.
# http://rubyforge.org/tracker/index.php?func=detail&aid=16741&group_id=4550&atid=17564
#require 'DBD/Mysql/Mysql'
#class DBI::DBD::Mysql::Database
#  TYPE_MAP.default = TYPE_MAP[nil] if TYPE_MAP.default.nil?
#end


QUERY = "SHOW FULL PROCESSLIST"
MYSQLCF = "/etc/mysql/my.cnf"

class IniFile
  def initialize(filename)
    @sections = {}
    file = File.open(filename, 'r')
    key = ""
    file.each do |line|
      if line =~ /^\[(.*)\]/
        key = $1
        @sections[key] = Hash.new
      elsif line =~ /^(.*?)\s*\=\s*(.*?)\s*$/
        if @sections.has_key?(key)
          @sections[key].store($1, $2)
        end
      end
    end
    file.close
  end

  def [] (key)
    return @sections[key]
  end

  def each
    @sections.each do |x,y|
      yield x,y
    end
  end

  def regex_filter! ( section_filter, key_filter  )
    if section_filter
      @sections = @sections.select { |k,v| k =~ section_filter }
    end
    if key_filter
      newsections = {}
      @sections.each do |section,entry|
        newsections[section]  = entry.select { |k,v| k =~ key_filter }
      end
      @sections = newsections
    end
    return self
  end

  def to_h
    return @sections
  end
end

class MySQLProcessLogger 
  attr_accessor :user, :pass, :syslog, :query

  def initialize( mycnf = MYSQLCF)
    ini = IniFile.new(mycnf)
    @mycf = ini.to_h
    @syslog = false
    @user = ""
    @pass = ""
    @query = "SHOW FULL PROCESSLIST"
  end

  def run
    loop do
      begin
        str = ""
        @mycf.each do |name,instance|
          instance['port']   or next 
          instance['socket'] or next 
          next if name =~ /^client/
          File.socket?(instance['socket']) or next
          str = query_processlist(instance, str)
        end

        #output
        output str
      rescue Exception => e
        output e
      end
      sleep 1
    end
  end

private
  def output ( str )
    if @syslog
      begin
        str.split("\n").each do |msg|
          # syslog doesn't like %
          Syslog.open("mysql_logger", Syslog::LOG_PID | Syslog::LOG_CONS) { |s| s.info msg.gsub('%', '\%') }
        end
      rescue Exception => e
      end
    else
      print str
      STDOUT.flush
    end
   end

  def query_processlist ( instance, str )
    dsn = "DBI:Mysql:host=localhost;socket=#{instance['socket']};port=#{instance['port']}"
    begin
      dbh = DBI.connect(dsn, @user, @pass) 
      sth = dbh.execute(@query) if dbh
      str = output_query(sth,instance, str) if sth
    rescue DBI::DatabaseError => e
      puts "An error occurred"
      puts "Error code: #{e.err}"
      puts "Error message: #{e.errstr}"
      puts "Error SQLSTATE: #{e.state}"
    rescue Exception => e
      puts e
    ensure
      sth.finish if sth
      dbh.disconnect if dbh
    end
    return str
  end

  def output_query ( sth, instance, str )
    while row = sth.fetch_hash do
      next if row["Command"] =~/^(Sleep|Connect|Binlog Dump|Daemon)$/
      next if row["Info"] == @query
      # output prefix data for sockett
      str += "#{Time.now} - " unless @syslog
      str += sprintf("{%s, %s} ",instance['socket'],instance['port'])
      # output all the processlist data
      row.each do |k,v| 
        # join newlines in the rows if they are strings.
        str += sprintf("%s=\"%s\" ",k, (v.class == String ? v.split("\n").join(' ') : v) )
      end
      str += "\n"
    end
    return str
  end
end

class ProcessCtl
  attr_accessor :pidfile, :daemonize

  def initialize 
    @pidfile = ""
    @daemonize = false
  end

  def start
    size = get_running_pids.size
    if size > 0
      puts "Daemon is already running"
      return 1
    end

#    Daemonize.daemonize if @daemonize
    if @daemonize
      #http://stackoverflow.com/questions/1740308/create-a-daemon-with-double-fork-in-ruby
      raise 'First fork failed' if (pid = fork) == -1
      exit unless pid.nil?

      Process.setsid
      raise 'Second fork failed' if (pid = fork) == -1
      exit unless pid.nil?

      Dir.chdir '/'
      File.umask 0000
      STDIN.reopen '/dev/null'
      STDOUT.reopen '/dev/null', 'a'
      STDERR.reopen STDOUT
    end
    write_pid unless pidfile == ""
    yield
    return 0
  end
  
  def stop
    get_running_pids.each do |pid|
      puts "Killing pid #{pid}"
      Process.kill :SIGTERM, pid
    end
    File.delete(@pidfile) if File.file?(@pidfile)
    return 0
  end

  # returns the exit status (1 if not running, 0 if running)
  def status
    size = get_running_pids.size
    puts "#{File.basename $0} is #{"not " if size < 1}running."
    return (size > 0) ? 0 : 1 
  end

protected
  def write_pid
    File.open(@pidfile, "w") do |f|
#      f.write($$)
      f.write(Process.pid)
    end
  end

  def get_running_pids
    result = []
    if File.file? @pidfile
      pid = File.read @pidfile
      # big long line I stole to kill a pid
      result =  `ps -p #{pid} -o pid h`.to_a.map!{|s| s.to_i}
    end
    return result
  end
end

$options = {}
$options[:dbuser] = ""
$options[:dbpass] = ""
$options[:syslog] = false
$options[:daemonize] = false
$options[:pidfile] = "/tmp/mysql_logger.pid"
$options[:command] = "start"

opts = OptionParser.new
opts.banner = "Usage $0 [OPTIONS]"
opts.on("-u", "--user USER", String, "MySQL User" )  { |v|  $options[:dbuser] = v }
opts.on("-p", "--pass PASSWORD", String, "MySQL Password" )  { |v|  $options[:dbpass] = v }
opts.on("-P", "--pidfile PIDFILE", String, "PID File" )  { |v|  $options[:pidfile] = v }
opts.on("-s", "--syslog", "Log to syslog" )  { |v|  $options[:syslog] = true }
opts.on("-d", "--daemonize", "daemonize process" )  { |v|  $options[:daemonize] = true }
opts.on("-k", "--command (start|stop|status)", String, "command to pass daemon") {|v| $options[:command] = v }
opts.on("-h", "--help",  "this message") { puts opts; exit 1}
opts.parse!

if $options[:command] !~ /(start|stop|status)/
  puts "Invalid command #{$options[:command]}"
  puts opts
  exit 1
end

mpl = MySQLProcessLogger.new
mpl.user   = $options[:dbuser]
mpl.pass   = $options[:dbpass]
mpl.syslog = $options[:syslog]
mpl.query  = QUERY

pc = ProcessCtl.new
pc.daemonize = $options[:daemonize]
pc.pidfile   = $options[:pidfile]

case $options[:command]
  when "stop"
    pc.stop
  when "status"
    exit pc.status
  else
    exit pc.start { mpl.run  }
end
