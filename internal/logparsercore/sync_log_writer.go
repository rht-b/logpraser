// This file provides utils to write logs by a thread to its respective log
// file. It also maintains state about threads to service queries on them.

package logparsercore

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	mergeLogFilePath      = "./mergedlogs"
	mergeLogFileExtension = "log"
	threadLogsStartMarker = "**START**"
	threadLogsEndMarker   = "**END**"
)

type threadFileMetadata struct {
	// Process ID of the thread.
	processId string

	// Timestamp of Start log marker.
	logsStartTimestamp time.Time

	// Timestamp of End log marker.
	logsEndTimestamp time.Time

	// Reference to the log file used for merging logs.
	logFd *os.File

	// File name of the log file for the thread.
	logFilepath string
}

type SyncLogWriter struct {
	// Map of threadID and the offsets of its section in merged file.
	threadsMetadata map[string]*threadFileMetadata
}

// -----------------------------------------------------------------------------

// NewLogWriter initializes state for the writer and creates an output directory
// for log files.
func NewLogWriter() (*SyncLogWriter, error) {
	// Create a new directory for log files for all threads.
	err := os.MkdirAll(mergeLogFilePath, os.ModePerm)
	if err != nil {
		zap.S().Error(err.Error())
		return nil, err
	}

	return &SyncLogWriter{
		threadsMetadata: make(map[string]*threadFileMetadata),
	}, nil
}

// -----------------------------------------------------------------------------

// Write writes log line (strongly typed object) to it's respective log file for
// a thread.
func (slw *SyncLogWriter) Write(ll *LogLine) (err error) {
	thState, ok := slw.threadsMetadata[ll.ThreadId()]
	if !ok {
		thState, err = slw.initialize(ll)
		if err != nil {
			return errors.Wrap(err, "error occurred while initializing log section")
		}
	}

	return slw.writeLogLine(ll, thState)
}

// -----------------------------------------------------------------------------

// initialize creates log file for a new thread and initializes state associated
// with it in the writer.
func (slw *SyncLogWriter) initialize(ll *LogLine) (
	*threadFileMetadata, error) {
	if ll.Message() != threadLogsStartMarker {
		return nil, errors.New("invalid start of thread logs")
	}

	// Create a new log file.
	filePath := filepath.Join(mergeLogFilePath, fmt.Sprintf("%s-%s.%s",
		ll.ProcessID(), ll.ThreadId(), mergeLogFileExtension))
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		zap.S().Error(err.Error())
		return nil, err
	}

	// Init state.
	thStatePtr := &threadFileMetadata{
		processId:          ll.ProcessID(),
		logsStartTimestamp: ll.Timestamp(),
		logFd:              file,
		logFilepath:        filePath,
	}
	slw.threadsMetadata[ll.ThreadId()] = thStatePtr

	return thStatePtr, nil
}

// -----------------------------------------------------------------------------

// writeLogLine is a helper util to write a log line (strongly typed object) to
// it's respective log file for the thread.
func (slw *SyncLogWriter) writeLogLine(ll *LogLine,
	thState *threadFileMetadata) error {
	msgWithLineBreak := append([]byte(ll.String()), '\n')
	_, err := thState.logFd.Write(msgWithLineBreak)
	if err != nil {
		return errors.Wrapf(err, "error occurred while writing to [%s]",
			thState.logFilepath)
	}

	// If logs end marker is found, update logs end TS.
	if ll.Message() == threadLogsEndMarker {
		thState.logsEndTimestamp = ll.Timestamp()
		// Flush the file system's in-memory copy of recently written data to disk.
		thState.logFd.Sync()
		// Close I/Os on the file.
		thState.logFd.Close()
		thState.logFd = nil
	}

	return nil
}

// -----------------------------------------------------------------------------

func (slw *SyncLogWriter) Close() {
	// All file descriptors should be closed by now. Nonetheless, flush the file
	// system's in-memory copy to disk, and close I/Os.
	for _, thMetadata := range slw.threadsMetadata {
		if thMetadata.logFd != nil {
			thMetadata.logFd.Sync()
			thMetadata.logFd.Close()
		}
	}
}

// -----------------------------------------------------------------------------

// GetActiveThreadCountBetweenInterval is the util to respond to the query
// on parsed log dumps.
func (slw *SyncLogWriter) GetActiveThreadCountBetweenInterval(
	startTime time.Time, endTime time.Time) []*ThreadInfo {
	thList := make([]*ThreadInfo, 0)
	for thID, thMetadata := range slw.threadsMetadata {
		if endTime.Before(thMetadata.logsStartTimestamp) ||
			startTime.After(thMetadata.logsEndTimestamp) {
			continue
		}
		thList = append(thList, &ThreadInfo{
			ProcessID:      thMetadata.processId,
			ThreadId:       thID,
			ThreadFileName: thMetadata.logFilepath,
		})
	}
	return thList
}

// -----------------------------------------------------------------------------

// GetMaxConcurrencyAndEpoch is the util to respond to the query to find
// the highest count of concurrent threads running in any second and the epoch.
func (slw *SyncLogWriter) GetMaxConcurrencyAndEpoch() (int, time.Time) {
	// Map to hold concurrency count for threadIds, at the time when they start
	// running.
	memo := make(map[string]int)
	for thId, thMetadata := range slw.threadsMetadata {
		memo[thId] = 0
		for _, thMetadataInner := range slw.threadsMetadata {
			if thMetadataInner.logsStartTimestamp.Equal(thMetadata.logsStartTimestamp) ||
				(thMetadataInner.logsStartTimestamp.After(thMetadata.logsStartTimestamp) &&
					thMetadataInner.logsStartTimestamp.Before(thMetadata.logsEndTimestamp)) {
				memo[thId] = memo[thId] + 1
			}
		}
	}
	var maxConcurrency int
	var epoch time.Time
	for thId, thCount := range memo {
		if thCount > maxConcurrency {
			maxConcurrency = thCount
			epoch = slw.threadsMetadata[thId].logsStartTimestamp
		}
	}

	return maxConcurrency, epoch
}

// -----------------------------------------------------------------------------

// GetAvgStdThreadRuntime is the util to respond to the query
// on parsed log dumps to get average runtime and standard deviation of
// thread runtimes.
func (slw *SyncLogWriter) GetAvgStdThreadRuntime() (float64, float64) {
	// Calculate average.
	var aggRuntime int64 = 0
	for _, thMetadata := range slw.threadsMetadata {
		runtimeMs := thMetadata.logsEndTimestamp.Sub(
			thMetadata.logsStartTimestamp).Milliseconds()
		aggRuntime = aggRuntime + runtimeMs
	}
	avgRuntime := float64(aggRuntime) / float64(len(slw.threadsMetadata))

	// Calculating std deviation.
	var stdSum float64
	for _, thMetadata := range slw.threadsMetadata {
		runtimeMs := thMetadata.logsEndTimestamp.Sub(
			thMetadata.logsStartTimestamp).Milliseconds()
		stdSum = stdSum + math.Pow(avgRuntime-float64(runtimeMs), 2)
	}

	std := math.Sqrt(stdSum / float64(len(slw.threadsMetadata)))

	return avgRuntime, std
}

// -----------------------------------------------------------------------------
