package transactionlogger

import (
	"flag"
	"fmt"
	"log/syslog"
	"os"
	"regexp"
	"strconv"
)

type Publisher interface {
	Push(s string)
}

type LoggerParameters struct {
	Protocol string
	Port     int
	Host     string
}

func ParseLoggerUrl(url string) (logger LoggerParameters, e error) {
	switch url {
	case "debug", "stdout", "stderr", "dummy", "sink":
		return LoggerParameters{Protocol: url,
			Port: 0,
			Host: "localhost",
		}, nil
	}

	// Parse  'rsync://127.0.0.1:3306'
	// Try the regex https://regex101.com/ Tip: there is a code generator
	var reLoggerURL = regexp.MustCompile(`(?m)(\S+)://(\S+):([0-9]+)`)

	if match := reLoggerURL.FindStringSubmatch(url); match != nil {
		port, _ := strconv.Atoi(match[3])
		logger = LoggerParameters{
			Port:     port,
			Host:     match[2],
			Protocol: match[1],
		}
		return logger, nil
	}

	// Parse 'file://var/log/logfile'
	var reLoggerProtocol = regexp.MustCompile(`(?m)(\S+)://(\S+)`)
	if match := reLoggerProtocol.FindStringSubmatch(url); match != nil {
		protocol := match[1]
		port := 0
		switch protocol {
		case "rsyslog":
			port = 514
		default:
			port = 0
		}
		logger = LoggerParameters{
			Port:     port,
			Host:     match[2],
			Protocol: protocol,
		}
		return logger, nil
	}
	return logger, fmt.Errorf("Failed to parse %s", url)
}

// Get environment variable TRANSACTION_LOGGER
// An empty string means "dummy" log - drop the activity log
// "debug" - use debug log
// "rsyslog://127.0.0.1" - dial the rsyslog
// "file:///var/log/pdns-recursor/dns_activity.log" - write to file
// also "stdout", "stderr" are supported
func New() (transactionLogger Publisher, msg string) {
	envTransactionLogger := os.Getenv("TRANSACTION_LOGGER")
	if len(envTransactionLogger) == 0 {
		flag.StringVar(&envTransactionLogger, "logger", "dummy", "Transaction logger. Examples rsyslog://127.0.0.1\nfile:///var/log/pdns-recursor/dns_activity.log\nstdout")
	}
	transactionLoggerParams, err := ParseLoggerUrl(envTransactionLogger)
	if err != nil {
		msg = fmt.Sprintf("Failed to parse activity log URL '%s'. Using default: sink", envTransactionLogger)
		transactionLoggerParams.Protocol = "dummy"
	}

	logChSize := 32 * 1024 // burst tolerance in the async logs
	if v := os.Getenv("TRANSACTION_LOGGER_CH_SIZE"); v != "" {
		logChSize, _ = strconv.Atoi(v)
	}

	switch transactionLoggerParams.Protocol {
	case "rsyslog":
		transactionLogger, err = NewRsyslog(logChSize, transactionLoggerParams.Host, transactionLoggerParams.Port, "")
		if err != nil {
			msg = fmt.Sprintf("Failed to dial rsyslog %s:%d %v", transactionLoggerParams.Host, transactionLoggerParams.Port, err)
		} else {
			msg = fmt.Sprintf("Transaction log goes to rsyslog %s:%d", transactionLoggerParams.Host, transactionLoggerParams.Port)
		}
	case "dummy", "sink":
		transactionLogger = NewDummy()
		msg = "Transaction log goes to sink"
	case "debug":
		transactionLogger = NewDebug()
		msg = "Transaction log goes to the service logger"
	case "stdout":
		transactionLogger = NewStdout(logChSize, os.Stdout)
		msg = "Transaction log goes to the stdout"
	case "stderr":
		transactionLogger = NewStdout(logChSize, os.Stderr)
		msg = "Transaction log goes to the stderr"
	case "file":
		filename := transactionLoggerParams.Host
		err := os.Chmod(filename, os.ModePerm)
		if err != nil {
			msg = fmt.Sprintf("Failed to chmode '%s'", filename)
			transactionLogger = NewDummy()
		}
		f, err := os.Create(filename)
		if err != nil {
			msg = fmt.Sprintf("Failed to open transaction log file '%s' for writing", filename)
			transactionLogger = NewDummy()
		} else {
			transactionLogger = NewStdout(logChSize, f)
			msg = fmt.Sprintf("Transaction log goes to the file '%s'", filename)
		}
	}

	return transactionLogger, msg
}

func NewStdout(logChSize int, outputIo *os.File) Publisher {
	publisher := &PublisherStdout{ch: make(chan string, logChSize), outputIo: outputIo}
	publisher.start()
	return publisher
}

// Returns a publisher which drops the activity log
func NewDebug() Publisher {
	publisher := &PublisherDebug{}
	return publisher
}

// Returns a publisher which drops the activity log
func NewDummy() Publisher {
	publisher := &PublisherDummy{}
	return publisher
}

func NewRsyslog(logChSize int, host string, port int, tag string) (Publisher, error) {
	raddr := fmt.Sprintf("%s:%d", host, port)
	isUdp := false
	flag.BoolVar(&isUdp, "--udp", false, "use UDP when connecting to rsyslog")
	protocol := "tcp"
	if isUdp {
		protocol = "udp"
	}
	logwriter, err := syslog.Dial(protocol, raddr, syslog.LOG_DEBUG, tag)
	if err != nil {
		return nil, err
	}

	publisher := &PublisherRsyslog{
		ch:     make(chan string, logChSize),
		raddr:  raddr,
		writer: logwriter,
		tag:    tag,
	}

	publisher.start()
	return publisher, nil
}

type Shippable interface {
	Log() (string, error)
}

type PublisherDummy struct {
}

func (this *PublisherDummy) Push(s string) {
}

type PublisherDebug struct {
}

func (this *PublisherDebug) Push(s string) {
	fmt.Printf("Transaction %s", s)
}

type PublisherRsyslog struct {
	ch     chan string
	raddr  string
	tag    string
	writer *syslog.Writer
}

func (this *PublisherRsyslog) Push(s string) {
	this.ch <- s
}

func (this *PublisherRsyslog) start() {
	go func() {
		for {
			s := <-this.ch
			this.writer.Debug(s + "\n")
		}
	}()
}

type PublisherStdout struct {
	outputIo *os.File
	ch       chan string
}

func (this *PublisherStdout) Push(s string) {
	this.ch <- s
}

func (this *PublisherStdout) start() {
	go func() {
		for {
			s := <-this.ch
			this.outputIo.WriteString(s + "\n")
		}
	}()
}
