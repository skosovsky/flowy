// Package rediskeys defines injective standalone Redis adapter key schema v2.
// It has no Redis dependency and grants no clustered-deployment capability.
package rediskeys

import "encoding/base64"

// Key independently encodes namespace and execution identity, without delimiters
// or host-supplied braces being interpreted as a storage path or hash tag.
func Key(namespace, execution, kind string) string {
	return "flowy:v2:" + base64.RawURLEncoding.EncodeToString(
		[]byte(namespace),
	) + ":" + base64.RawURLEncoding.EncodeToString(
		[]byte(execution),
	) + ":" + kind
}
