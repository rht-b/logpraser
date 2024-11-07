// This file contains struct type for a line in log file, and provides
// parser util.

package logparsercore

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var LogLineParseErr = errors.New("log line parse error")

type LogLine struct {
	processID  *string
	threadID   *string
	threadName *string
	timestamp  time.Time
	message    *string
}

// -----------------------------------------------------------------------------

// Parses log line and forms a strongly typed object.
func ParseLogline(lineText string) (*LogLine, error) {
	// Splits the log line text into two parts.
	textOnceHalvedTokens := strings.SplitN(lineText, "::", 2)
	if len(textOnceHalvedTokens) != 2 {
		return nil, LogLineParseErr
	}

	// Again split the first half of the log text to ProcessID and ThreadID.
	pidThidTokens := strings.Split(textOnceHalvedTokens[0], ":")
	if len(pidThidTokens) != 2 {
		return nil, LogLineParseErr
	}

	// Split the second half of the log text to [ThName Timestamp] and Message.
	textSecondHalfTokens := strings.SplitN(textOnceHalvedTokens[1], " - ", 2)
	if len(textSecondHalfTokens) != 2 {
		return nil, LogLineParseErr
	}

	// Split the text to ThreadName and Timestamp.
	thNameTsTokens := strings.SplitN(textSecondHalfTokens[0], " ", 2)
	if len(thNameTsTokens) != 2 {
		return nil, LogLineParseErr
	}

	// Parse timestamp with format string.
	ts, err := time.Parse("2006-01-02 15:04:05,000", thNameTsTokens[1])
	if err != nil {
		return nil, errors.Wrap(LogLineParseErr, err.Error())
	}

	return &LogLine{
		processID:  &pidThidTokens[0],
		threadID:   &pidThidTokens[1],
		threadName: &thNameTsTokens[0],
		timestamp:  ts,
		message:    &textSecondHalfTokens[1],
	}, nil
}

// -----------------------------------------------------------------------------

// Gets process ID on the log line.
func (ll *LogLine) ProcessID() string {
	return *ll.processID
}

// -----------------------------------------------------------------------------

// Gets thread ID on the log line.
func (ll *LogLine) ThreadId() string {
	return *ll.threadID
}

// -----------------------------------------------------------------------------

// Gets thread name on the log line.
func (ll *LogLine) ThreadName() string {
	return *ll.threadName
}

// -----------------------------------------------------------------------------

// Gets timestamp on the log line.
func (ll *LogLine) Timestamp() time.Time {
	return ll.timestamp
}

// -----------------------------------------------------------------------------

// Gets the log message.
func (ll *LogLine) Message() string {
	return *ll.message
}

// -----------------------------------------------------------------------------

// Formats a LogLine pointer object to text.
func (ll *LogLine) String() string {
	return fmt.Sprintf("%s:%s::%s %v - %s", *ll.processID, *ll.threadID,
		*ll.threadName, ll.timestamp, *ll.message)
}

// -----------------------------------------------------------------------------
