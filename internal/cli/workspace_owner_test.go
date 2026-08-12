package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeWorkspaceOwnerRemote struct {
	mu               sync.Mutex
	token            string
	expires          time.Time
	childAlive       bool
	failRenew        bool
	ambiguousRelease bool
	blockBusyAcquire bool
	changed          chan struct{}
}

type blockedRenewWorkspaceOwnerRemote struct {
	inner        *fakeWorkspaceOwnerRemote
	renewStarted chan struct{}
	releaseRenew chan struct{}
	err          error
	once         sync.Once
}

type reachableReleaseTestBackend struct {
	testSSHBackend
	released atomic.Bool
}

type destructiveGraceReleaseTestBackend struct {
	testSSHBackend
}

type retainedStopReleaseTestBackend struct {
	testSSHBackend
	released atomic.Bool
}

func (b *reachableReleaseTestBackend) ReleaseLease(context.Context, ReleaseLeaseRequest) error {
	b.released.Store(true)
	return nil
}

func (b *reachableReleaseTestBackend) ReleaseLeaseOwnerCleanupMode(LeaseTarget) ReleaseLeaseOwnerCleanupMode {
	return ReleaseLeaseOwnerCleanupAfterProviderRelease
}

func (b destructiveGraceReleaseTestBackend) ReleaseLeaseNeedsOwnerGraceFence(LeaseTarget) bool {
	return true
}

func (b *retainedStopReleaseTestBackend) ReleaseLeaseOwnerCleanupMode(LeaseTarget) ReleaseLeaseOwnerCleanupMode {
	return ReleaseLeaseOwnerCleanupBeforeProviderRelease
}

func (b *retainedStopReleaseTestBackend) ReleaseLease(context.Context, ReleaseLeaseRequest) error {
	b.released.Store(true)
	return nil
}

func (r *blockedRenewWorkspaceOwnerRemote) Do(ctx context.Context, req workspaceOwnerRemoteRequest) (string, error) {
	if req.Action != workspaceOwnerRenew {
		return r.inner.Do(ctx, req)
	}
	r.once.Do(func() { close(r.renewStarted) })
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.releaseRenew:
		return "", r.err
	}
}

func newFakeWorkspaceOwnerRemote() *fakeWorkspaceOwnerRemote {
	return &fakeWorkspaceOwnerRemote{changed: make(chan struct{})}
}

func (f *fakeWorkspaceOwnerRemote) signalLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeWorkspaceOwnerRemote) Do(ctx context.Context, req workspaceOwnerRemoteRequest) (string, error) {
	for {
		f.mu.Lock()
		switch req.Action {
		case workspaceOwnerAcquire:
			if f.token == "" {
				f.token = req.Token
				f.expires = time.Now().Add(req.TTL)
				f.signalLocked()
				f.mu.Unlock()
				return "ACQUIRED", nil
			}
			if time.Now().After(f.expires) {
				if f.childAlive {
					f.mu.Unlock()
					return "CHILD", nil
				}
				f.token = req.Token
				f.expires = time.Now().Add(req.TTL)
				f.signalLocked()
				f.mu.Unlock()
				return "RECOVERED", nil
			}
			if !f.blockBusyAcquire {
				f.mu.Unlock()
				return "BUSY", nil
			}
			changed := f.changed
			f.mu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-changed:
				continue
			}
		case workspaceOwnerRenew:
			if f.failRenew {
				f.mu.Unlock()
				return "", errors.New("renew transport lost")
			}
			if f.token != req.Token {
				f.mu.Unlock()
				return "MISMATCH", nil
			}
			f.expires = time.Now().Add(req.TTL)
			f.mu.Unlock()
			return "RENEWED", nil
		case workspaceOwnerInspect:
			if f.token != req.Token {
				f.mu.Unlock()
				return "MISMATCH", nil
			}
			if f.childAlive {
				f.mu.Unlock()
				return "CHILD", nil
			}
			f.mu.Unlock()
			return "OWNED", nil
		case workspaceOwnerRelease:
			if f.ambiguousRelease {
				f.mu.Unlock()
				return "", errors.New("release response lost")
			}
			if f.token != req.Token {
				f.mu.Unlock()
				return "MISMATCH", nil
			}
			if f.childAlive {
				f.mu.Unlock()
				return "CHILD", nil
			}
			f.token = ""
			f.signalLocked()
			f.mu.Unlock()
			return "RELEASED", nil
		default:
			f.mu.Unlock()
			return "AMBIGUOUS", nil
		}
	}
}

func acquireFakeWorkspaceOwner(t *testing.T, ctx context.Context, remote *fakeWorkspaceOwnerRemote, leaseID string) *workspaceOwner {
	t.Helper()
	owner, err := acquireWorkspaceOwnerWithTransport(ctx, SSHTarget{}, leaseID, &bytes.Buffer{}, remote, 250*time.Millisecond, 80*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire workspace owner: %v", err)
	}
	return owner
}

func crashFakeWorkspaceOwner(t *testing.T, owner *workspaceOwner) {
	t.Helper()
	owner.closeOnce.Do(func() {
		close(owner.stop)
		<-owner.done
	})
}

func TestWorkspaceOwnerSerializesIndependentClientsAndRevisions(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	remote.blockBusyAcquire = true
	ownerA := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_shared")

	type fakeWorkspace struct {
		sync.Mutex
		revision string
	}
	var workspace fakeWorkspace
	var active atomic.Int32
	var overlap atomic.Bool
	startedA := make(chan struct{})
	releaseA := make(chan struct{})
	done := make(chan error, 2)
	run := func(owner *workspaceOwner, revision string, started chan<- struct{}, release <-chan struct{}) {
		if active.Add(1) != 1 {
			overlap.Store(true)
		}
		defer active.Add(-1)
		workspace.Lock()
		workspace.revision = revision
		workspace.Unlock()
		if started != nil {
			close(started)
		}
		if release != nil {
			<-release
		}
		workspace.Lock()
		executed := workspace.revision
		workspace.Unlock()
		if executed != revision {
			done <- fmt.Errorf("executed revision %s, want %s", executed, revision)
			return
		}
		done <- nil
	}
	go run(ownerA, "revision-a", startedA, releaseA)
	<-startedA

	ownerBCh := make(chan *workspaceOwner, 1)
	errBCh := make(chan error, 1)
	go func() {
		ownerB, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_shared", &bytes.Buffer{}, remote, time.Second, 80*time.Millisecond, 20*time.Millisecond)
		if err != nil {
			errBCh <- err
			return
		}
		ownerBCh <- ownerB
	}()
	select {
	case ownerB := <-ownerBCh:
		_ = ownerB.Close(context.Background())
		t.Fatal("second client acquired while the first lifecycle was active")
	case err := <-errBCh:
		t.Fatalf("second client failed instead of waiting: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseA)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := ownerA.Close(context.Background()); err != nil {
		t.Fatalf("release first owner: %v", err)
	}
	var ownerB *workspaceOwner
	select {
	case ownerB = <-ownerBCh:
	case err := <-errBCh:
		t.Fatalf("second client acquire: %v", err)
	case <-time.After(time.Second):
		t.Fatal("second client did not acquire after release")
	}
	go run(ownerB, "revision-b", nil, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if overlap.Load() {
		t.Fatal("independent clients overlapped workspace lifecycle operations")
	}
	if err := ownerB.Close(context.Background()); err != nil {
		t.Fatalf("release second owner: %v", err)
	}
}

func TestWorkspaceOwnerCrashRecoveryHonorsWitnessedChild(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	owner := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_child")
	crashFakeWorkspaceOwner(t, owner)
	remote.mu.Lock()
	remote.expires = time.Now().Add(-time.Second)
	remote.childAlive = true
	remote.mu.Unlock()

	_, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_child", &bytes.Buffer{}, remote, 40*time.Millisecond, time.Second, 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("live witnessed child takeover err=%v, want bounded refusal", err)
	}
	remote.mu.Lock()
	remote.childAlive = false
	remote.mu.Unlock()
	recovered := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_child")
	if err := recovered.Close(context.Background()); err != nil {
		t.Fatalf("release recovered owner: %v", err)
	}
}

func TestWorkspaceOwnerTokenAndTransportFailuresFailClosed(t *testing.T) {
	t.Run("token mismatch", func(t *testing.T) {
		remote := newFakeWorkspaceOwnerRemote()
		owner := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_token")
		remote.mu.Lock()
		remote.token = strings.Repeat("f", 64)
		remote.mu.Unlock()
		if err := owner.Close(context.Background()); err == nil || !strings.Contains(err.Error(), "mismatch") {
			t.Fatalf("close err=%v, want token mismatch", err)
		}
	})

	t.Run("failed renew", func(t *testing.T) {
		remote := newFakeWorkspaceOwnerRemote()
		owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_renew", &bytes.Buffer{}, remote, time.Second, 50*time.Millisecond, 5*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		remote.mu.Lock()
		remote.failRenew = true
		remote.mu.Unlock()
		select {
		case <-owner.Context().Done():
		case <-time.After(time.Second):
			t.Fatal("renewal failure did not cancel lifecycle context")
		}
		if err := owner.Close(context.Background()); err == nil || !strings.Contains(err.Error(), "renewal failed closed") {
			t.Fatalf("close err=%v, want renewal failure", err)
		}
	})

	t.Run("release preparation preserves default pre-release renewal failures", func(t *testing.T) {
		remote := newFakeWorkspaceOwnerRemote()
		owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_default_renew", &bytes.Buffer{}, remote, time.Second, 50*time.Millisecond, 5*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		remote.mu.Lock()
		remote.failRenew = true
		remote.mu.Unlock()
		select {
		case <-owner.Context().Done():
		case <-time.After(time.Second):
			t.Fatal("renewal failure did not cancel lifecycle context")
		}
		_, err = prepareWorkspaceOwnerForLeaseRelease(context.Background(), owner, testSSHBackend{}, LeaseTarget{LeaseID: "cbx_default_renew"})
		if err == nil || !strings.Contains(err.Error(), "renewal failed closed") {
			t.Fatalf("prepare default err=%v, want renewal failure", err)
		}
	})

	t.Run("release preparation preserves pre-release renewal failures", func(t *testing.T) {
		remote := &blockedRenewWorkspaceOwnerRemote{
			inner:        newFakeWorkspaceOwnerRemote(),
			renewStarted: make(chan struct{}),
			releaseRenew: make(chan struct{}),
			err:          errors.New("signal: killed"),
		}
		owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_released_renew", &bytes.Buffer{}, remote, time.Second, 500*time.Millisecond, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-remote.renewStarted:
		case <-time.After(time.Second):
			t.Fatal("renewal call did not start")
		}
		done := make(chan error, 1)
		go func() {
			done <- owner.PrepareLeaseRelease(context.Background(), time.Second)
		}()
		select {
		case err := <-done:
			t.Fatalf("release preparation returned before in-flight renewal: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		close(remote.releaseRenew)
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "renewal failed closed") {
				t.Fatalf("release preparation err=%v, want preserved renewal failure", err)
			}
		case <-time.After(time.Second):
			t.Fatal("release preparation waited after renewal returned")
		}
		if err := owner.CloseAfterLeaseRelease(); err == nil || !strings.Contains(err.Error(), "renewal failed closed") {
			t.Fatalf("post-release close err=%v, want preserved pre-release renewal failure", err)
		}
	})

	t.Run("release-grace renewal blocks competing recovery past original ttl", func(t *testing.T) {
		remote := newFakeWorkspaceOwnerRemote()
		owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_release_retry", &bytes.Buffer{}, remote, time.Second, 60*time.Millisecond, 10*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.PrepareLeaseRelease(context.Background(), 300*time.Millisecond); err != nil {
			t.Fatalf("prepare release: %v", err)
		}
		time.Sleep(90 * time.Millisecond)
		competing := make(chan error, 1)
		go func() {
			competitor, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_release_retry", &bytes.Buffer{}, remote, 120*time.Millisecond, 60*time.Millisecond, 10*time.Millisecond)
			if err == nil {
				_ = competitor.Close(context.Background())
			}
			competing <- err
		}()
		select {
		case err := <-competing:
			if err == nil {
				t.Fatal("competing owner recovered workspace during release")
			}
			if !strings.Contains(err.Error(), "timed out") {
				t.Fatalf("competing acquire err=%v, want timeout while release-grace owner is valid", err)
			}
		case <-time.After(time.Second):
			t.Fatal("competing acquire did not finish")
		}
		if err := owner.CloseAfterLeaseRelease(); err != nil {
			t.Fatalf("close release owner: %v", err)
		}
	})

	t.Run("ambiguous release", func(t *testing.T) {
		remote := newFakeWorkspaceOwnerRemote()
		owner := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_release")
		remote.mu.Lock()
		remote.ambiguousRelease = true
		remote.mu.Unlock()
		if err := owner.Close(context.Background()); err == nil || !strings.Contains(err.Error(), "ambiguous remote state") {
			t.Fatalf("close err=%v, want ambiguous release", err)
		}
	})
}

func TestWorkspaceOwnerAcquisitionBoundary(t *testing.T) {
	nonExclusive := newWatchTestBackend()
	if !shouldAcquireWorkspaceOwner(true, nonExclusive) {
		t.Fatal("a successful non-exclusive acquisition must acquire the workspace owner")
	}
	exclusive := runEnvProfileTestBackend{}
	if shouldAcquireWorkspaceOwner(true, exclusive) {
		t.Fatal("newly acquired exclusive one-shot lease must bypass the workspace owner")
	}
	if !shouldAcquireWorkspaceOwner(false, exclusive) {
		t.Fatal("existing, pooled, and watch-reused leases must acquire the reuse owner")
	}
}

func TestWorkspaceOwnerContextWrapsEverySSHChild(t *testing.T) {
	owner := &workspaceOwner{target: SSHTarget{TargetOS: targetLinux}, key: strings.Repeat("a", 64), token: strings.Repeat("b", 64)}
	ctx := contextWithWorkspaceOwner(context.Background(), owner)
	if got := wrapWorkspaceOwnerRemote(ctx, "printf ok", false); !strings.Contains(got, "child_identity=$(ps -o lstart=") {
		t.Fatalf("ordinary SSH child was not witnessed:\n%s", got)
	}
	if got := wrapWorkspaceOwnerRemote(ctx, "cat", true); !strings.Contains(got, `cat >"$run_dir/input"`) || !strings.Contains(got, `<"$run_dir/input"`) {
		t.Fatalf("input SSH child did not preserve stdin:\n%s", got)
	}
	if got := wrapWorkspaceOwnerRemote(contextWithoutWorkspaceOwner(ctx), "printf raw", false); got != "printf raw" {
		t.Fatalf("owner bypass wrapped protocol-internal command: %q", got)
	}
}

func TestWorkspaceOwnerLifecycleBoundaryMatrix(t *testing.T) {
	for _, lifecycle := range []string{
		"fingerprint skip",
		"normal sync",
		"full resync",
		"no sync",
		"sync only",
		"fresh pr",
		"actions hydration",
		"command",
		"results and artifacts",
		"failure bundle",
		"pool scrub and return",
		"watch iteration",
	} {
		t.Run(lifecycle, func(t *testing.T) {
			if !shouldAcquireWorkspaceOwner(false, nil) {
				t.Fatalf("reused %s path bypassed workspace ownership", lifecycle)
			}
		})
	}
}

func TestWorkspaceOwnerSerializesStaticRunAndStandaloneActionsHydration(t *testing.T) {
	if !shouldAcquireWorkspaceOwner(true, testStaticSSHBackend{}) {
		t.Fatal("static SSH acquisition bypassed workspace ownership")
	}
	if releaseLeaseNeedsOwnerGraceFence(testStaticSSHBackend{}, LeaseTarget{LeaseID: "cbx_static"}) {
		t.Fatal("static SSH lease backend unexpectedly requested release grace fence")
	}
	if !releaseLeaseNeedsOwnerGraceFence(destructiveGraceReleaseTestBackend{}, LeaseTarget{LeaseID: "cbx_destructive"}) {
		t.Fatal("destructive release backend did not request release grace fence")
	}
	remote := newFakeWorkspaceOwnerRemote()
	remote.blockBusyAcquire = true
	runOwner := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_run_hydrate")
	hydrateOwner := make(chan *workspaceOwner, 1)
	hydrateErr := make(chan error, 1)
	go func() {
		owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_run_hydrate", &bytes.Buffer{}, remote, time.Second, time.Second, 100*time.Millisecond)
		if err != nil {
			hydrateErr <- err
			return
		}
		hydrateOwner <- owner
	}()
	select {
	case owner := <-hydrateOwner:
		_ = owner.Close(context.Background())
		t.Fatal("standalone Actions hydration overlapped the normal run owner")
	case err := <-hydrateErr:
		t.Fatalf("standalone Actions hydration failed instead of contending: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := runOwner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case owner := <-hydrateOwner:
		if err := owner.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	case err := <-hydrateErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("standalone Actions hydration did not acquire after the run released")
	}
}

func TestWorkspaceOwnerPersistentReleaseAllowsImmediateReuse(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	owner := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_static_reuse")
	if err := owner.Close(context.Background()); err != nil {
		t.Fatalf("release persistent owner: %v", err)
	}
	next, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_static_reuse", &bytes.Buffer{}, remote, 100*time.Millisecond, time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("immediate second static acquisition failed: %v", err)
	}
	if err := next.Close(context.Background()); err != nil {
		t.Fatalf("release second owner: %v", err)
	}
}

func TestWorkspaceOwnerReachableRunCleanupReleasesRemoteOwner(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	owner := acquireFakeWorkspaceOwner(t, context.Background(), remote, "cbx_static_cleanup")
	backend := &reachableReleaseTestBackend{}

	mode, err := prepareWorkspaceOwnerForLeaseRelease(context.Background(), owner, backend, LeaseTarget{LeaseID: "cbx_static_cleanup"})
	if err != nil {
		t.Fatalf("prepare reachable cleanup: %v", err)
	}
	if mode != ReleaseLeaseOwnerCleanupAfterProviderRelease {
		t.Fatalf("reachable release mode=%s, want after provider release", mode)
	}
	if err := backend.ReleaseLease(context.Background(), ReleaseLeaseRequest{Lease: LeaseTarget{LeaseID: "cbx_static_cleanup"}}); err != nil {
		t.Fatalf("provider release: %v", err)
	}
	if !backend.released.Load() {
		t.Fatal("provider release was not exercised")
	}
	if err := closeWorkspaceOwnerAfterLeaseRelease(context.Background(), owner, mode); err != nil {
		t.Fatalf("close reachable owner after release: %v", err)
	}
	next, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_static_cleanup", &bytes.Buffer{}, remote, 100*time.Millisecond, time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("immediate reacquire after reachable cleanup failed: %v", err)
	}
	if err := next.Close(context.Background()); err != nil {
		t.Fatalf("release reacquired owner: %v", err)
	}
}

func TestWorkspaceOwnerUnclassifiedRunCleanupDefaultsToGraceFence(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_unclassified_cleanup", &bytes.Buffer{}, remote, time.Second, 80*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	mode, err := prepareWorkspaceOwnerForLeaseRelease(context.Background(), owner, testSSHBackend{}, LeaseTarget{LeaseID: "cbx_unclassified_cleanup"})
	if err != nil {
		t.Fatalf("prepare unclassified cleanup: %v", err)
	}
	if mode != ReleaseLeaseOwnerCleanupGraceFence {
		t.Fatalf("unclassified release mode=%s, want grace fence", mode)
	}
	if err := (testSSHBackend{}).ReleaseLease(context.Background(), ReleaseLeaseRequest{Lease: LeaseTarget{LeaseID: "cbx_unclassified_cleanup"}}); err != nil {
		t.Fatalf("provider release: %v", err)
	}
	if err := closeWorkspaceOwnerAfterLeaseRelease(context.Background(), owner, mode); err != nil {
		t.Fatalf("close unclassified owner after release: %v", err)
	}
	_, err = acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_unclassified_cleanup", &bytes.Buffer{}, remote, 120*time.Millisecond, 80*time.Millisecond, 10*time.Millisecond)
	if err == nil {
		t.Fatal("unclassified cleanup released remote owner instead of retaining grace fence")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("unclassified reacquire err=%v, want timeout while grace fence remains", err)
	}
}

func TestWorkspaceOwnerRetainedStopCleanupReleasesRemoteOwnerBeforeProviderRelease(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	leaseID := "cbx_retained_stop_cleanup"
	owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, leaseID, &bytes.Buffer{}, remote, time.Second, 80*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	backend := &retainedStopReleaseTestBackend{}

	mode, err := prepareWorkspaceOwnerForLeaseRelease(context.Background(), owner, backend, LeaseTarget{LeaseID: leaseID})
	if err != nil {
		t.Fatalf("prepare retained stop cleanup: %v", err)
	}
	if mode != ReleaseLeaseOwnerCleanupBeforeProviderRelease {
		t.Fatalf("retained stop release mode=%s, want before provider release", mode)
	}
	next, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, leaseID, &bytes.Buffer{}, remote, 100*time.Millisecond, 80*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("retained stop did not release owner before provider release: %v", err)
	}
	if err := backend.ReleaseLease(context.Background(), ReleaseLeaseRequest{Lease: LeaseTarget{LeaseID: leaseID}}); err != nil {
		t.Fatalf("provider release: %v", err)
	}
	if !backend.released.Load() {
		t.Fatal("provider release was not exercised")
	}
	if err := closeWorkspaceOwnerAfterLeaseRelease(context.Background(), owner, mode); err != nil {
		t.Fatalf("post retained-stop close should be a no-op: %v", err)
	}
	if err := next.Close(context.Background()); err != nil {
		t.Fatalf("release reacquired owner: %v", err)
	}
}

func TestWorkspaceOwnerCoordinatorReleaseDeletePathUsesGraceFence(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	leaseID := "cbx_coord_delete_cleanup"
	owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, leaseID, &bytes.Buffer{}, remote, time.Second, 60*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	releaseCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/leases/"+leaseID+"/release" {
			http.NotFound(w, r)
			return
		}
		releaseCalled = true
		var body struct {
			Delete bool `json:"delete"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode coordinator release body: %v", err)
		}
		if !body.Delete {
			t.Fatal("coordinator release did not request destructive delete=true")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"lease": CoordinatorLease{ID: leaseID, Provider: "aws", State: "released"}})
	}))
	defer server.Close()

	cfg := Config{Coordinator: server.URL, CoordToken: "user-token"}
	coord, _, err := newCoordinatorClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	backend := &coordinatorLeaseBackend{cfg: cfg, coord: coord, direct: testSSHBackend{}}
	mode, err := prepareWorkspaceOwnerForLeaseRelease(context.Background(), owner, backend, LeaseTarget{LeaseID: leaseID})
	if err != nil {
		t.Fatalf("prepare coordinator cleanup: %v", err)
	}
	if mode != ReleaseLeaseOwnerCleanupGraceFence {
		t.Fatalf("coordinator cleanup mode=%s, want grace fence", mode)
	}
	if err := backend.ReleaseLease(context.Background(), ReleaseLeaseRequest{Lease: LeaseTarget{LeaseID: leaseID}}); err != nil {
		t.Fatalf("coordinator release: %v", err)
	}
	if !releaseCalled {
		t.Fatal("coordinator release endpoint was not called")
	}
	if err := closeWorkspaceOwnerAfterLeaseRelease(context.Background(), owner, mode); err != nil {
		t.Fatalf("close coordinator owner after release: %v", err)
	}
	_, err = acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, leaseID, &bytes.Buffer{}, remote, 120*time.Millisecond, 60*time.Millisecond, 10*time.Millisecond)
	if err == nil {
		t.Fatal("coordinator destructive cleanup released remote owner instead of retaining grace fence")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("coordinator destructive reacquire err=%v, want timeout while grace fence remains", err)
	}
}

func TestWorkspaceOwnerCoordinatorGraceFenceProviderSpecBoundary(t *testing.T) {
	for _, spec := range []ProviderSpec{
		(testStaticSSHProvider{}).Spec(),
		(testNamespaceProvider{}).Spec(),
	} {
		if spec.Coordinator != CoordinatorNever {
			t.Fatalf("%s coordinator mode=%s, want never", spec.Name, spec.Coordinator)
		}
	}
	for _, spec := range []ProviderSpec{
		(testAWSProvider{}).Spec(),
		(testAzureProvider{}).Spec(),
		(testGCPProvider{}).Spec(),
		(testHetznerProvider{}).Spec(),
		(testDaytonaProvider{}).Spec(),
	} {
		if spec.Coordinator != CoordinatorSupported {
			t.Fatalf("%s coordinator mode=%s, want supported", spec.Name, spec.Coordinator)
		}
	}
	if !releaseLeaseNeedsOwnerGraceFence(&coordinatorLeaseBackend{direct: testSSHBackend{}}, LeaseTarget{LeaseID: "cbx_coord_boundary"}) {
		t.Fatal("coordinator wrapper must fence releases independent of direct provider policy")
	}
}

func TestProviderReleaseOwnerCleanupModes(t *testing.T) {
	modeFor := func(t *testing.T, provider Provider, cfg Config) ReleaseLeaseOwnerCleanupMode {
		t.Helper()
		backend, err := provider.Configure(cfg, Runtime{})
		if err != nil {
			t.Fatalf("configure %s: %v", provider.Name(), err)
		}
		ssh, ok := backend.(SSHLeaseBackend)
		if !ok {
			t.Fatalf("backend %T for %s is not SSHLeaseBackend", backend, provider.Name())
		}
		return releaseLeaseOwnerCleanupMode(ssh, LeaseTarget{LeaseID: "cbx_mode"})
	}

	for _, tc := range []struct {
		name     string
		provider Provider
		cfg      Config
		want     ReleaseLeaseOwnerCleanupMode
	}{
		{name: "aws direct delete", provider: testAWSProvider{}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "azure direct delete", provider: testAzureProvider{}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "gcp direct delete", provider: testGCPProvider{}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "hetzner direct delete", provider: testHetznerProvider{}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "daytona direct delete", provider: testDaytonaProvider{}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "apple vm direct delete", provider: testAppleVMProvider{}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "static ssh reachable", provider: testStaticSSHProvider{}, want: ReleaseLeaseOwnerCleanupAfterProviderRelease},
		{name: "external protocol reachable", provider: testExternalProvider{}, cfg: Config{External: ExternalConfig{Command: "external"}}, want: ReleaseLeaseOwnerCleanupAfterProviderRelease},
		{name: "namespace retained stop", provider: testNamespaceProvider{}, cfg: Config{Namespace: NamespaceConfig{DeleteOnRelease: false}}, want: ReleaseLeaseOwnerCleanupBeforeProviderRelease},
		{name: "namespace delete", provider: testNamespaceProvider{}, cfg: Config{Namespace: NamespaceConfig{DeleteOnRelease: true}}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "incus retained stop", provider: testIncusProvider{}, cfg: Config{Incus: IncusConfig{DeleteOnRelease: false}}, want: ReleaseLeaseOwnerCleanupBeforeProviderRelease},
		{name: "firecracker retained stop", provider: testFirecrackerProvider{}, cfg: Config{Firecracker: FirecrackerConfig{DeleteOnRelease: false}}, want: ReleaseLeaseOwnerCleanupBeforeProviderRelease},
		{name: "morph retained stop", provider: testMorphProvider{}, cfg: Config{Morph: MorphConfig{DeleteOnRelease: false}}, want: ReleaseLeaseOwnerCleanupBeforeProviderRelease},
		{name: "vast stop", provider: testVastProvider{}, cfg: Config{Vast: VastConfig{ReleaseAction: "stop"}}, want: ReleaseLeaseOwnerCleanupBeforeProviderRelease},
		{name: "vast keep", provider: testVastProvider{}, cfg: Config{Vast: VastConfig{ReleaseAction: "keep"}}, want: ReleaseLeaseOwnerCleanupBeforeProviderRelease},
		{name: "vast destroy", provider: testVastProvider{}, cfg: Config{Vast: VastConfig{ReleaseAction: "destroy"}}, want: ReleaseLeaseOwnerCleanupGraceFence},
		{name: "nvidia brev stop", provider: testNvidiaBrevProvider{}, cfg: Config{NvidiaBrev: NvidiaBrevConfig{ReleaseAction: "stop"}}, want: ReleaseLeaseOwnerCleanupBeforeProviderRelease},
		{name: "nvidia brev delete", provider: testNvidiaBrevProvider{}, cfg: Config{NvidiaBrev: NvidiaBrevConfig{ReleaseAction: "delete"}}, want: ReleaseLeaseOwnerCleanupGraceFence},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := modeFor(t, tc.provider, tc.cfg); got != tc.want {
				t.Fatalf("release owner cleanup mode=%s, want %s", got, tc.want)
			}
		})
	}
}

func TestWorkspaceOwnerDestructiveRunCleanupKeepsGraceFence(t *testing.T) {
	remote := newFakeWorkspaceOwnerRemote()
	owner, err := acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_destructive_cleanup", &bytes.Buffer{}, remote, time.Second, 60*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	mode, err := prepareWorkspaceOwnerForLeaseRelease(context.Background(), owner, destructiveGraceReleaseTestBackend{}, LeaseTarget{LeaseID: "cbx_destructive_cleanup"})
	if err != nil {
		t.Fatalf("prepare destructive cleanup: %v", err)
	}
	if mode != ReleaseLeaseOwnerCleanupGraceFence {
		t.Fatalf("destructive release mode=%s, want grace fence", mode)
	}
	if err := (destructiveGraceReleaseTestBackend{}).ReleaseLease(context.Background(), ReleaseLeaseRequest{Lease: LeaseTarget{LeaseID: "cbx_destructive_cleanup"}}); err != nil {
		t.Fatalf("provider release: %v", err)
	}
	if err := closeWorkspaceOwnerAfterLeaseRelease(context.Background(), owner, mode); err != nil {
		t.Fatalf("close destructive owner after release: %v", err)
	}
	_, err = acquireWorkspaceOwnerWithTransport(context.Background(), SSHTarget{}, "cbx_destructive_cleanup", &bytes.Buffer{}, remote, 120*time.Millisecond, 60*time.Millisecond, 10*time.Millisecond)
	if err == nil {
		t.Fatal("destructive cleanup released remote owner instead of retaining grace fence")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("destructive reacquire err=%v, want timeout while grace fence remains", err)
	}
}

func TestWorkspaceOwnerProtocolGeneration(t *testing.T) {
	leaseID := "cbx_raw-lease-must-not-appear"
	key := workspaceOwnerKey(leaseID)
	token := strings.Repeat("a", 64)
	req := workspaceOwnerRemoteRequest{Action: workspaceOwnerAcquire, Key: key, Token: token, TTL: time.Minute}

	posix := remoteWorkspaceOwnerCommand(SSHTarget{TargetOS: targetLinux}, req)
	wsl := remoteWorkspaceOwnerCommand(SSHTarget{TargetOS: targetWindows, WindowsMode: windowsModeWSL2}, req)
	if posix != wsl {
		t.Fatal("POSIX and WSL2 must use the same owner protocol")
	}
	for _, want := range []string{".crabbox/workspace-owners", key + ".gate", "$key.owner", "$key.child", "flock -x -w 0", "lockf -t 0", "ps -o lstart=", "RECOVERED", "MISMATCH", "EXPIRED", "AMBIGUOUS", `[ "$state_expiry" -gt "$(date +%s)" ]`} {
		if !strings.Contains(posix, want) {
			t.Fatalf("POSIX protocol missing %q:\n%s", want, posix)
		}
	}
	if strings.Contains(posix, leaseID) {
		t.Fatalf("POSIX protocol exposed raw lease ID: %s", posix)
	}

	windows := decodePowerShellCommand(t, remoteWorkspaceOwnerCommand(SSHTarget{TargetOS: targetWindows, WindowsMode: windowsModeNormal}, req))
	for _, want := range []string{".crabbox\\workspace-owners", "$key = '" + key + "'", "$key + \".owner\"", "$key + \".child\"", "[Diagnostics.Process]::GetProcessById", "return \"ambiguous\"", "StartTime.ToUniversalTime().Ticks", "RECOVERED", "MISMATCH", "EXPIRED", "AMBIGUOUS", "$current.Expiry -le [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()"} {
		if !strings.Contains(windows, want) {
			t.Fatalf("Windows protocol missing %q:\n%s", want, windows)
		}
	}
	if strings.Contains(windows, leaseID) {
		t.Fatalf("Windows protocol exposed raw lease ID: %s", windows)
	}
	posixWitness := remoteWorkspaceOwnerPOSIXWitness(key, token, "printf ok")
	for _, want := range []string{"child_identity=$(ps -o lstart=", "owner_expiry=$(sed -n", "owner_expiry", "date +%s", "mv \"$child_tmp\" \"$child\"", "touch \"$start\"", "wait \"$child_pid\"", "rm -f \"$child\""} {
		if !strings.Contains(posixWitness, want) {
			t.Fatalf("POSIX child witness missing %q:\n%s", want, posixWitness)
		}
	}
	windowsWitness := decodePowerShellCommand(t, remoteWorkspaceOwnerWindowsWitness(key, token, "Write-Output ok"))
	for _, want := range []string{"Start-Process", "StartTime.ToUniversalTime().Ticks", "Read-Expiry", "(Read-Expiry) -le [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()", "Move-Item -LiteralPath $tmp -Destination $child", "$process.WaitForExit()", "Remove-Item -LiteralPath $child"} {
		if !strings.Contains(windowsWitness, want) {
			t.Fatalf("Windows child witness missing %q:\n%s", want, windowsWitness)
		}
	}
	windowsInputWitness := decodePowerShellCommand(t, remoteWorkspaceOwnerWindowsWitness(key, token, "Write-Output ok", true))
	for _, want := range []string{"[Console]::OpenStandardInput().CopyTo($inputFile)", "-RedirectStandardInput $inputPath", "[IO.FileShare]::None"} {
		if !strings.Contains(windowsInputWitness, want) {
			t.Fatalf("Windows input witness missing %q:\n%s", want, windowsInputWitness)
		}
	}
}

func runPOSIXWorkspaceOwnerScript(t *testing.T, home, script string) (string, error) {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func TestWorkspaceOwnerPOSIXProtocolBehavior(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX protocol execution requires sh")
	}
	home := t.TempDir()
	key := workspaceOwnerKey("cbx_protocol")
	tokenA := strings.Repeat("a", 64)
	tokenB := strings.Repeat("b", 64)
	request := func(action workspaceOwnerAction, token string) workspaceOwnerRemoteRequest {
		return workspaceOwnerRemoteRequest{Action: action, Key: key, Token: token, TTL: 30 * time.Second}
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerAcquire, tokenA))); err != nil || out != "ACQUIRED" {
		t.Fatalf("acquire out=%q err=%v", out, err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerRenew, tokenB))); err == nil || out != "MISMATCH" {
		t.Fatalf("mismatched renew out=%q err=%v", out, err)
	}
	statePath := filepath.Join(home, ".crabbox", "workspace-owners", key+".owner")
	childPath := filepath.Join(home, ".crabbox", "workspace-owners", key+".child")
	expiredState := "v1\n" + tokenA + "\n1\n"
	if err := os.WriteFile(statePath, []byte(expiredState), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerRenew, tokenA))); err == nil || out != "EXPIRED" {
		t.Fatalf("late same-token renew out=%q err=%v", out, err)
	}
	if data, err := os.ReadFile(statePath); err != nil || string(data) != expiredState {
		t.Fatalf("late renew changed expired state: data=%q err=%v", data, err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerInspect, tokenA))); err == nil || out != "EXPIRED" {
		t.Fatalf("late same-token inspect out=%q err=%v", out, err)
	}
	lateResultPath := filepath.Join(home, "late-child.txt")
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIXWitness(key, tokenA, "touch "+shellQuote(lateResultPath))); err == nil {
		t.Fatalf("late same-token witness succeeded: out=%q", out)
	}
	if _, err := os.Stat(childPath); !os.IsNotExist(err) {
		t.Fatalf("late witness published child state: %v", err)
	}
	if _, err := os.Stat(lateResultPath); !os.IsNotExist(err) {
		t.Fatalf("late witness executed child command: %v", err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerAcquire, tokenA))); err != nil || out != "RECOVERED" {
		t.Fatalf("recover expired generation out=%q err=%v", out, err)
	}

	resultPath := filepath.Join(home, "revision.txt")
	witness := remoteWorkspaceOwnerPOSIXWitness(key, tokenA, "printf revision-a > "+shellQuote(resultPath))
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, witness); err != nil {
		t.Fatalf("witnessed command out=%q err=%v", out, err)
	}
	if data, err := os.ReadFile(resultPath); err != nil || string(data) != "revision-a" {
		t.Fatalf("witnessed result=%q err=%v", data, err)
	}
	inputPath := filepath.Join(home, "input.txt")
	inputWitness := remoteWorkspaceOwnerPOSIXWitness(key, tokenA, "cat > "+shellQuote(inputPath), true)
	cmd := exec.Command("sh", "-c", inputWitness)
	cmd.Env = append(os.Environ(), "HOME="+home)
	cmd.Stdin = strings.NewReader("registration-input")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("input witness out=%q err=%v", out, err)
	}
	if data, err := os.ReadFile(inputPath); err != nil || string(data) != "registration-input" {
		t.Fatalf("witnessed input=%q err=%v", data, err)
	}
	identityOut, err := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	identity := strings.TrimSpace(strings.Join(strings.Fields(string(identityOut)), " "))
	existingWitness := strconv.Itoa(os.Getpid()) + "\n" + identity + "\n"
	if err := os.WriteFile(childPath, []byte(existingWitness), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIXWitness(key, tokenA, "true")); err == nil {
		t.Fatalf("overlapping witness replaced live child: out=%q", out)
	}
	if data, err := os.ReadFile(childPath); err != nil || string(data) != existingWitness {
		t.Fatalf("live witness changed: data=%q err=%v", data, err)
	}
	if err := os.Remove(childPath); err != nil {
		t.Fatal(err)
	}
	guardOwner := &workspaceOwner{target: SSHTarget{TargetOS: targetLinux}, key: key, token: tokenA}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, guardOwner.WrapBackgroundCommand(guardOwner.rsyncGuardPayload(filepath.Join(home, "guard-destination")))); err != nil || strings.TrimSpace(out) == "" {
		t.Fatalf("start rsync guard out=%q err=%v", out, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(childPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rsync guard did not publish its child witness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, guardOwner.rsyncStopCommand()); err != nil {
		t.Fatalf("stop rsync guard out=%q err=%v", out, err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(childPath); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rsync guard did not clear its child witness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(childPath); !os.IsNotExist(err) {
		t.Fatalf("child witness remained after exit: %v", err)
	}

	if err := os.WriteFile(statePath, []byte("v1\n"+tokenA+"\n1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(childPath, []byte(strconv.Itoa(os.Getpid())+"\n"+identity+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerAcquire, tokenB))); err != nil || out != "CHILD" {
		t.Fatalf("live child acquire out=%q err=%v", out, err)
	}
	if err := os.WriteFile(childPath, []byte("999999999\nold identity\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerAcquire, tokenB))); err != nil || out != "RECOVERED" {
		t.Fatalf("stale recovery out=%q err=%v", out, err)
	}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(request(workspaceOwnerRelease, tokenB))); err != nil || out != "RELEASED" {
		t.Fatalf("release out=%q err=%v", out, err)
	}
}

func TestWorkspaceOwnerPOSIXRecoversAbandonedGate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX protocol execution requires sh")
	}
	home := t.TempDir()
	key := workspaceOwnerKey("cbx_stale_gate")
	token := strings.Repeat("d", 64)
	root := filepath.Join(home, ".crabbox", "workspace-owners")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, key+".gate"), []byte("abandoned"), 0o600); err != nil {
		t.Fatal(err)
	}
	req := workspaceOwnerRemoteRequest{Action: workspaceOwnerAcquire, Key: key, Token: token, TTL: 30 * time.Second}
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(req)); err != nil || out != "ACQUIRED" {
		t.Fatalf("recover abandoned gate out=%q err=%v", out, err)
	}
	req.Action = workspaceOwnerRelease
	if out, err := runPOSIXWorkspaceOwnerScript(t, home, remoteWorkspaceOwnerPOSIX(req)); err != nil || out != "RELEASED" {
		t.Fatalf("release after gate recovery out=%q err=%v", out, err)
	}
}

func TestWorkspaceOwnerWindowsProtocolBehavior(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("native Windows protocol execution runs in Windows CI")
	}
	key := workspaceOwnerKey("cbx_windows_protocol_" + strconv.FormatInt(time.Now().UnixNano(), 10))
	token := strings.Repeat("c", 64)
	prepare := `$root = Join-Path $HOME ".crabbox\workspace-owners"
New-Item -ItemType Directory -Force -Path $root | Out-Null
Set-Content -LiteralPath (Join-Path $root (` + psQuote(key) + ` + ".gate")) -Value "abandoned" -Encoding ASCII
`
	if out, err := runWindowsPowerShellScript(t, prepare); err != nil {
		t.Fatalf("prepare abandoned Windows gate out=%q err=%v", out, err)
	}
	req := workspaceOwnerRemoteRequest{Action: workspaceOwnerAcquire, Key: key, Token: token, TTL: 30 * time.Second}
	if out, err := runWindowsPowerShellScript(t, decodePowerShellCommand(t, remoteWorkspaceOwnerWindows(req))); err != nil || !strings.Contains(string(out), "ACQUIRED") {
		t.Fatalf("Windows acquire out=%q err=%v", out, err)
	}
	expire := `$root = Join-Path $HOME ".crabbox\workspace-owners"
$state = Join-Path $root (` + psQuote(key) + ` + ".owner")
Set-Content -LiteralPath $state -Value @("v1", ` + psQuote(token) + `, "1") -Encoding ASCII
`
	if out, err := runWindowsPowerShellScript(t, expire); err != nil {
		t.Fatalf("expire Windows owner state out=%q err=%v", out, err)
	}
	req.Action = workspaceOwnerRenew
	if out, err := runWindowsPowerShellScript(t, decodePowerShellCommand(t, remoteWorkspaceOwnerWindows(req))); err == nil || !strings.Contains(string(out), "EXPIRED") {
		t.Fatalf("late Windows renew out=%q err=%v", out, err)
	}
	req.Action = workspaceOwnerInspect
	if out, err := runWindowsPowerShellScript(t, decodePowerShellCommand(t, remoteWorkspaceOwnerWindows(req))); err == nil || !strings.Contains(string(out), "EXPIRED") {
		t.Fatalf("late Windows inspect out=%q err=%v", out, err)
	}
	if out, err := runWindowsPowerShellScript(t, decodePowerShellCommand(t, remoteWorkspaceOwnerWindowsWitness(key, token, "Write-Output late-child"))); err == nil || strings.Contains(string(out), "late-child") {
		t.Fatalf("late Windows witness executed: out=%q err=%v", out, err)
	}
	verifyExpired := `$root = Join-Path $HOME ".crabbox\workspace-owners"
$state = Join-Path $root (` + psQuote(key) + ` + ".owner")
$child = Join-Path $root (` + psQuote(key) + ` + ".child")
$lines = @(Get-Content -LiteralPath $state -ErrorAction Stop)
if ($lines.Count -ne 3 -or $lines[2] -ne "1") { throw "expired state changed" }
if (Test-Path -LiteralPath $child) { throw "late witness published child" }
`
	if out, err := runWindowsPowerShellScript(t, verifyExpired); err != nil {
		t.Fatalf("verify expired Windows generation out=%q err=%v", out, err)
	}
	req.Action = workspaceOwnerAcquire
	if out, err := runWindowsPowerShellScript(t, decodePowerShellCommand(t, remoteWorkspaceOwnerWindows(req))); err != nil || !strings.Contains(string(out), "RECOVERED") {
		t.Fatalf("recover expired Windows generation out=%q err=%v", out, err)
	}
	if out, err := runWindowsPowerShellScript(t, decodePowerShellCommand(t, remoteWorkspaceOwnerWindowsWitness(key, token, "Write-Output child-ok"))); err != nil || !strings.Contains(string(out), "child-ok") {
		t.Fatalf("Windows witnessed child out=%q err=%v", out, err)
	}
	req.Action = workspaceOwnerRelease
	if out, err := runWindowsPowerShellScript(t, decodePowerShellCommand(t, remoteWorkspaceOwnerWindows(req))); err != nil || !strings.Contains(string(out), "RELEASED") {
		t.Fatalf("Windows release out=%q err=%v", out, err)
	}
}
