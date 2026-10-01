package controller

import (
	"fmt"
	"strconv"
	"strings"
)

// replicaUID returns the stable, restart-surviving identity of a deployment
// replica: "namespace-deploymentName-ordinal". Stability of identity is what
// makes repeated/concurrent reconciliation idempotent: the same ordinal always
// names the same pod slot, so it can never be created twice.
func replicaUID(namespace, deployment string, ordinal int) string {
	return fmt.Sprintf("%s-%s-%d", namespace, deployment, ordinal)
}

// ordinalOf extracts the ordinal from a replica UID. The second result is
// false if the UID is not a replica UID produced by replicaUID.
func ordinalOf(uid string) (int, bool) {
	idx := strings.LastIndex(uid, "-")
	if idx < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(uid[idx+1:])
	if err != nil {
		return 0, false
	}
	return n, true
}
