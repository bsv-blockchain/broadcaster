package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	chaintrackslib "github.com/bsv-blockchain/go-chaintracks/chaintracks"
	chaintracksclient "github.com/bsv-blockchain/go-chaintracks/client"
)

// TestChaintracksHeaderReader_RemoteNotFoundIsAbsence: in microservice mode
// bump-builder reads headers from the chaintracks pod over HTTP, and the
// go-chaintracks client reports a height above the tip as
// "server request failed: status 404" — not ErrHeaderNotFound, the absence
// sentinel ChainHeaderReader promises. On mainnet 973522 (2026-10-31) that
// made every reconciler tick fail its neighborhood walk at the tip ("look up
// the active block at height 973524: server request failed: status 404") for
// 27 minutes, until the tip had grown past the walk's depth. A 404 must read
// as absence; any other failed request stays a failure.
func TestChaintracksHeaderReader_RemoteNotFoundIsAbsence(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v2/header/height/973524":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	reader := chaintracksHeaderReader{ct: chaintracksclient.New(srv.URL)}

	_, err := reader.GetHeaderByHeight(context.Background(), 973524)
	if !errors.Is(err, chaintrackslib.ErrHeaderNotFound) {
		t.Fatalf("a remote 404 must be reported as ErrHeaderNotFound (absence), got %v", err)
	}

	_, err = reader.GetHeaderByHeight(context.Background(), 973525)
	if err == nil || errors.Is(err, chaintrackslib.ErrHeaderNotFound) {
		t.Fatalf("a remote 500 is a failed lookup, not absence, got %v", err)
	}
}
