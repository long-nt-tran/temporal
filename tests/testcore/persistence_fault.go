package testcore

import (
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/faultinjection"
)

// PersistenceFault returns the error to inject. Return nil to run the operation.
type PersistenceFault func(target faultinjection.Target) error

// PersistenceFaultOption limits which operations receive a fault.
type PersistenceFaultOption func(*persistenceFaultOptions)

type persistenceFaultOptions struct {
	store  config.DataStoreName
	method string
}

// WithStore limits a fault to one data store.
func WithStore(store config.DataStoreName) PersistenceFaultOption {
	return func(o *persistenceFaultOptions) {
		o.store = store
	}
}

// WithMethod limits a fault to one method.
func WithMethod(method string) PersistenceFaultOption {
	return func(o *persistenceFaultOptions) {
		o.method = method
	}
}

func (o persistenceFaultOptions) matches(target faultinjection.Target) bool {
	if o.store != "" && target.Store != o.store {
		return false
	}
	if o.method != "" && target.Method != o.method {
		return false
	}
	return true
}

func applyPersistenceFaultOptions(opts []PersistenceFaultOption) persistenceFaultOptions {
	var options persistenceFaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// InjectPersistenceFault adds a fault and enables it before cluster startup.
// The test fails if the fault does not fire.
func InjectPersistenceFault(t testing.TB, reg *faultinjection.FaultRegistry, fault PersistenceFault, opts ...PersistenceFaultOption) TestOption {
	t.Helper()

	options := applyPersistenceFaultOptions(opts)
	tracker := newFaultTracker(t)
	unregister := reg.RegisterCallback(func(target faultinjection.Target) error {
		if !options.matches(target) {
			return nil
		}
		injectedErr := fault(target)
		if injectedErr == nil {
			return nil
		}
		tracker.markFired(target)
		return injectedErr
	})
	tracker.attach(unregister)

	return WithPersistenceFaultInjection(&config.FaultInjection{Injector: reg.Inject})
}
