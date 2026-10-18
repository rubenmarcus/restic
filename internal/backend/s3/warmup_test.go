package s3_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/s3"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/options"
	rtest "github.com/restic/restic/internal/test"
)

const deepArchivePack = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// deepArchiveS3 serves a single DEEP_ARCHIVE pack without restore information
// and rejects restore requests with InvalidObjectState, which is what S3
// answers while a restore request for the object is already running.
func deepArchiveS3(t testing.TB) (endpoint string, restores *atomic.Int64, cleanup func()) {
	t.Helper()

	var restoreRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("restore") {
			restoreRequests.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InvalidObjectState</Code><Message>Object restore is already in progress.</Message><RequestId>1</RequestId><HostId>2</HostId></Error>`)
			return
		}

		if r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if !strings.HasSuffix(r.URL.Path, "/"+deepArchivePack[:2]+"/"+deepArchivePack) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>no such key</Message><RequestId>1</RequestId><HostId>2</HostId></Error>`)
			return
		}

		w.Header().Set("Content-Length", "42")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.Header().Set("X-Amz-Storage-Class", "DEEP_ARCHIVE")
		w.WriteHeader(http.StatusOK)
	}))

	return strings.TrimPrefix(srv.URL, "http://"), &restoreRequests, srv.Close
}

// TestS3WarmupDeepArchive verifies that a DEEP_ARCHIVE pack whose restore
// request is rejected with InvalidObjectState is still reported as warming up.
// Otherwise the restore silently reads the archived pack and retries forever.
func TestS3WarmupDeepArchive(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.S3Restore, true)()

	endpoint, restores, cleanup := deepArchiveS3(t)
	defer cleanup()

	cfg := s3.NewConfig()
	cfg.Endpoint = endpoint
	cfg.Bucket = "restictestbucket"
	cfg.UseHTTP = true
	cfg.Region = "us-east-1"
	cfg.BucketLookup = "path"
	cfg.KeyID = "keyid"
	cfg.Secret = options.NewSecretString("secret")
	cfg.EnableRestore = true
	cfg.RestoreTimeout = time.Second

	be, err := s3.Open(context.Background(), cfg, nil, func(_ string, _ ...any) {})
	rtest.OK(t, err)

	pack := backend.Handle{Type: backend.PackFile, Name: deepArchivePack}
	handles, err := be.Warmup(context.Background(), []backend.Handle{pack})
	rtest.OK(t, err)
	rtest.Equals(t, int64(1), restores.Load(), "expected exactly one restore request for the DEEP_ARCHIVE pack")
	rtest.Equals(t, []backend.Handle{pack}, handles)

	// The pack never becomes readable, so waiting must fail with a clear error
	// instead of the restore reading the archived pack in an endless retry loop.
	err = be.WarmupWait(context.Background(), handles)
	rtest.Assert(t, err != nil, "expected WarmupWait to fail, got nil")
	rtest.Assert(t, strings.Contains(err.Error(), "not warming up"), "unexpected WarmupWait error: %v", err)
}
