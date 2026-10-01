// Package testutil provides small helpers shared by unit tests.
package testutil

import (
	"fmt"
	"testing"
)

// Log is a scheduler/controller Logger that funnels into t.Logf, so every
// scheduling input and verdict is printed in the unit-test logs
// (go test -v).
type Log struct{ T *testing.T }

func (l Log) Printf(format string, args ...any) {
	l.T.Helper()
	l.T.Logf(format, args...)
}

// Q converts an int literal into a milli resource Quantity in tests.
func Q(v int64) int64 { return v }

// Describe returns a compact, stable textual summary used in assertions/logs.
func Describe(prefix string, kvs ...any) string {
	s := prefix
	for i := 0; i+1 < len(kvs); i += 2 {
		s += fmt.Sprintf(" %s=%v", kvs[i], kvs[i+1])
	}
	return s
}
