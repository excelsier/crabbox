package shared

import (
	"context"
	"fmt"
	"strings"
	"time"

	core "github.com/openclaw/crabbox/internal/cli"
)

type DirectSSHBackend struct {
	SpecValue       core.ProviderSpec
	Cfg             core.Config
	RT              core.Runtime
	Delete          func(context.Context, core.Config, core.Server) error
	CleanupEligible func(context.Context, core.Server) (bool, error)
	StoredLeaseKeys bool
}

func (b *DirectSSHBackend) Spec() core.ProviderSpec { return b.SpecValue }

func (b *DirectSSHBackend) ReleaseLeaseOwnerCleanupMode(lease core.LeaseTarget) core.ReleaseLeaseOwnerCleanupMode {
	if b.SpecValue.Name == "ssh" {
		return core.ReleaseLeaseOwnerCleanupAfterProviderRelease
	}
	return core.ReleaseLeaseOwnerCleanupGraceFence
}

func DeleteOnReleaseOwnerCleanupMode(deleteOnRelease bool) core.ReleaseLeaseOwnerCleanupMode {
	if deleteOnRelease {
		return core.ReleaseLeaseOwnerCleanupGraceFence
	}
	return core.ReleaseLeaseOwnerCleanupShortFence
}

func DeleteOnReleaseFromLease(configured bool, explicit bool, lease core.LeaseTarget) bool {
	if explicit {
		return configured
	}
	switch ReleaseActionLabel(lease) {
	case "stop", "keep", "pause", "false":
		return false
	case "delete", "destroy", "true":
		return true
	default:
		return configured
	}
}

func ReleaseActionFromLease(configured string, explicit bool, lease core.LeaseTarget) string {
	if explicit {
		return strings.ToLower(strings.TrimSpace(configured))
	}
	return strings.ToLower(strings.TrimSpace(firstNonBlank(ReleaseActionLabel(lease), configured)))
}

func ReleaseActionLabel(lease core.LeaseTarget) string {
	if lease.Server.Labels == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(firstNonBlank(
		lease.Server.Labels["release"],
		lease.Server.Labels["release_action"],
		lease.Server.Labels["coder_release_action"],
	)))
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (b *DirectSSHBackend) RebindResolvedLeaseTarget(target *core.LeaseTarget, leaseID string) error {
	if b.StoredLeaseKeys {
		core.UseStoredTestboxKey(&target.SSH, leaseID)
	}
	return nil
}

func (b *DirectSSHBackend) CleanupServers(ctx context.Context, req core.CleanupRequest, servers []core.Server) error {
	now := time.Now().UTC()
	if b.RT.Clock != nil {
		now = b.RT.Clock.Now().UTC()
	}
	for _, s := range servers {
		shouldDelete, reason := core.ShouldCleanupServer(s, now)
		if !shouldDelete {
			fmt.Fprintf(b.RT.Stderr, "skip server id=%s name=%s reason=%s\n", s.DisplayID(), s.Name, reason)
			continue
		}
		if b.CleanupEligible != nil {
			eligible, err := b.CleanupEligible(ctx, s)
			if err != nil {
				return err
			}
			if !eligible {
				fmt.Fprintf(b.RT.Stderr, "skip server id=%s name=%s reason=no-exact-local-claim\n", s.DisplayID(), s.Name)
				continue
			}
		}
		fmt.Fprintf(b.RT.Stderr, "delete server id=%s name=%s\n", s.DisplayID(), s.Name)
		if !req.DryRun {
			if b.Delete == nil {
				return core.Exit(2, "provider=%s cleanup backend has no delete capability", b.SpecValue.Name)
			}
			if err := b.Delete(ctx, b.Cfg, s); err != nil {
				return err
			}
		}
	}
	return nil
}

func ServerWithDefaultLabel(server core.Server, key, value string) core.Server {
	labels := make(map[string]string, len(server.Labels)+1)
	for label, current := range server.Labels {
		labels[label] = current
	}
	if labels[key] == "" {
		labels[key] = value
	}
	server.Labels = labels
	return server
}

func CleanupClaimEligible(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	var exitErr core.ExitError
	if core.AsExitError(err, &exitErr) {
		return false, nil
	}
	return false, err
}

func (b *DirectSSHBackend) Touch(ctx context.Context, server core.Server, state string) core.Server {
	return core.TouchDirectLeaseBestEffort(ctx, b.Cfg, server, state, b.RT.Stderr)
}

func AcquireAttemptsRetry(rt core.Runtime, keep bool, acquire func() (core.LeaseTarget, error)) (core.LeaseTarget, error) {
	var lastErr error
	attempts := core.AcquireAttempts(keep)
	for attempt := 1; attempt <= attempts; attempt++ {
		lease, err := acquire()
		if err == nil {
			return lease, nil
		}
		lastErr = err
		if attempt == attempts || !core.IsBootstrapWaitError(err) {
			return core.LeaseTarget{}, err
		}
		fmt.Fprintf(rt.Stderr, "warning: bootstrap failed; retrying with fresh lease: %v\n", err)
	}
	return core.LeaseTarget{}, lastErr
}
