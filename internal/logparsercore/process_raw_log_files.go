// This file contains methods to read raw files, converts and clusters log files
// according to threads/user-requests.

package logparsercore

import (
	"bufio"
	"os"
	"path"

	"go.uber.org/zap"
)

func ProcessRawLogFiles(slw *SyncLogWriter) error {
	// Read file names of processes logs.
	processesLogDirEntries, err := os.ReadDir(InputLogsDirPath)
	if err != nil {
		zap.S().Error(err)
		return err
	}

	for _, dirEntry := range processesLogDirEntries {
		convertProcessLogFile(path.Join(InputLogsDirPath, dirEntry.Name()), slw)
	}

	return nil
}

// -----------------------------------------------------------------------------

// convertProcessLogFile convert processes log files to thread log files.
func convertProcessLogFile(logfileName string,
	slw *SyncLogWriter) error {
	fd, err := os.OpenFile(logfileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		zap.S().Errorf("error while opening file: %v", err)
		return err
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		lineText := scanner.Text()

		// Parse the log line.
		llObj, err := ParseLogline(lineText)
		if err != nil {
			zap.S().Error(err.Error())
			return err
		}

		// Write to clustered log files.
		err = slw.Write(llObj)
		if err != nil {
			zap.S().Error(err.Error())
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		zap.S().Errorf("error occurred while scanning file [%s]: %v",
			logfileName, err)
		return err
	}

	return nil
}

// -----------------------------------------------------------------------------
