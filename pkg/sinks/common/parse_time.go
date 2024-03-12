package common

import (
	"github.com/sirupsen/logrus"
	"time"
)

// parseTime from time string to unix time (with milliseconds)
// time with milliseconds, time without milliseconds
// "2024-01-20T16:00:43.516Z", "2024-01-20T16:00:43Z"
func parseTime(tStr string) (int64, bool) {
	t, err := time.Parse(time.RFC3339Nano, tStr)
	if err != nil {
		// If there's an error in parsing, try parsing with RFC3339 instead
		t, err = time.Parse(time.RFC3339, tStr)
		if err != nil {
			logrus.Debugf("Error parsing time: %v\n", err)
			return 0, false
		}
	}

	unixTime := t.UnixMilli()
	return unixTime, true
}
