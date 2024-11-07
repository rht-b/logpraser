// This file contains main method to run the program.

package main

import (
	"flag"
	"fmt"
	"logparser/internal/logparsercore"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var t1, t2 string

func init() {
	flag.StringVar(&t1, "t1", "2020-08-09 18:59:25,200", "t1 input of time-range"+
		" in seconds (t1, t2) to give back information of how many threads were"+
		" active in this time-range")
	flag.StringVar(&t2, "t2", "2020-08-09 18:59:25,300", "t2 input of time-range"+
		" in seconds (t1, t2) to give back information of how many threads were"+
		" active in this time-range")
}

func main() {
	// Parse gFlags.
	flag.Parse()

	// Init logger.
	logger, _ := zap.NewProduction()
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	// Init sync log writer.
	slw, err := logparsercore.NewLogWriter()
	if err != nil {
		zap.S().Error(err)
		return
	}
	defer slw.Close()

	// Process raw log files.
	if err := logparsercore.ProcessRawLogFiles(slw); err != nil {
		zap.S().Error(err)
		return
	}

	startTs, err := time.Parse("2006-01-02 15:04:05,000", t1)
	if err != nil {
		err = errors.Wrap(err, "invalid format of t1 input")
		zap.S().Error(err)
		return
	}
	endTs, err := time.Parse("2006-01-02 15:04:05,000", t2)
	if err != nil {
		err = errors.Wrap(err, "invalid format of t2 input")
		zap.S().Error(err)
		return
	}

	// Query - 1.
	filteredThList := slw.GetActiveThreadCountBetweenInterval(startTs, endTs)
	for idx, thInfo := range filteredThList {
		if idx == 0 {
			fmt.Println("Thread ID\t\tProcess ID\t\tLogs Filepath")
		}
		fmt.Printf("%s\t\t%s\t\t%s\n", thInfo.ThreadId, thInfo.ProcessID,
			thInfo.ThreadFileName)
	}

	// Query - 2.
	maxCon, epoch := slw.GetMaxConcurrencyAndEpoch()
	fmt.Printf("\nThe highest count of concurrent threads running in any "+
		"second was [%d]. The epoch at which the maximum concurrent threads were "+
		"alive was: [%s]\n", maxCon, epoch.String())

	// Query - 3.
	avg, stdev := slw.GetAvgStdThreadRuntime()
	fmt.Printf("\n\nThe average and stdev of the all threads lifetime are: "+
		"avg=[%f ms] stdev=[%f ms]", avg, stdev)
}

// -----------------------------------------------------------------------------
