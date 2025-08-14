package main

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
)

const fieldCoordinates = "currentLocationCoordinates" // TODO(elffjs): Move this to the vss package.

var (
	futureAllowance    = 5 * time.Minute
	allowedLocationGap = 500 * time.Millisecond
)

func fmtTime(t time.Time) string {
	return t.Format(time.RFC3339)
}

var zeroTime time.Time

// ProcessSignals transforms a slice of input signals in ways that
// simplify downstream processing. Currently this means:
//
//   - If there are multiple exact copies of a given signal, then
//     remove all but one.
//   - Remove location values with latitude and longitude both equal
//     to zero.
//   - Roughly, for each triple of the input signals named
//     currentLocationLatitude, currentLocationLongitude, and
//     dimoAftermarketHDOP with sufficiently
//     close timestamps, we will also emit a location-values signal
//     named currentLocationCoordinates which combines all three.
//   - Remove unpaired latitudes and longitudes.
//   - Remove values that are far into the future.
//   - Remove coordinates at the origin (0, 0).
//
// The returned slice of signals is always meaningful, even if an error
// is also returned.
//
// Note that this function does reorder the input slice.
func ProcessSignals(signals []vss.Signal) ([]vss.Signal, error) {
	if len(signals) == 0 {
		return signals, nil
	}

	// Used to populate the payload-wide stuff, like token id or
	// CloudEvent id, in newly created rows. We assume that these do
	// not change within a single payload.
	template := signals[0]

	var (
		errs    error
		drop    = make([]bool, len(signals))
		created []vss.Signal

		// The
		lastLat, lastLon = -1, -1
		lastHDOP         = -1
		lastTime         time.Time
	)

	lastReference := func(signalName string) *int {
		switch signalName {
		case vss.FieldCurrentLocationLatitude:
			return &lastLat
		case vss.FieldCurrentLocationLongitude:
			return &lastLon
		case vss.FieldDIMOAftermarketHDOP:
			return &lastHDOP
		default:
			return nil
		}
	}

	tryCreateLocationSignal := func() {
		var (
			loc    vss.Location
			create bool
		)

		if lastLat != -1 && lastLon != -1 {
			lat, lon := signals[lastLat].ValueNumber, signals[lastLon].ValueNumber
			if lat == 0 && lon == 0 {
				drop[lastLat], drop[lastLon] = true, true
				errs = errors.Join(errs, fmt.Errorf("latitude and longitude at origin at time %s", fmtTime(lastTime)))
			} else {
				loc.Latitude, loc.Longitude = lat, lon
				create = true
			}
		} else if lastLat != -1 {
			drop[lastLat] = true
			errs = errors.Join(errs, fmt.Errorf("unpaired latitude at time %s", fmtTime(lastTime)))
		} else if lastLon != -1 {
			drop[lastLon] = true
			errs = errors.Join(errs, fmt.Errorf("unpaired longitude at time %s", fmtTime(lastTime)))
		}

		if lastHDOP != -1 {
			loc.HDOP = signals[lastHDOP].ValueNumber
			create = true
		}

		if create {
			created = append(created, vss.Signal{
				TokenID:       template.TokenID,
				Timestamp:     lastTime,
				Name:          fieldCoordinates,
				ValueLocation: loc,
				Source:        template.Source,
				Producer:      template.Producer,
				CloudEventID:  template.CloudEventID,
			})
		}

		lastLat, lastLon = -1, -1
		lastHDOP = -1
		lastTime = zeroTime
	}

	// Sorting this way makes it easier to handle time gaps, and to
	// remove duplicates.
	slices.SortFunc(signals, func(a, b vss.Signal) int {
		return cmp.Or(a.Timestamp.Compare(b.Timestamp), cmp.Compare(a.Name, b.Name))
	})

	now := time.Now()

	for i, sig := range signals {
		if !lastTime.IsZero() && sig.Timestamp.Sub(lastTime) >= allowedLocationGap {
			tryCreateLocationSignal()
		}

		// If this triggers then it will continue to trigger for the
		// rest of the loop, because we started out sorting by
		// timestamp.
		if sig.Timestamp.After(now.Add(futureAllowance)) {
			errs = errors.Join(errs, fmt.Errorf("signal %s has future timestamp %s", sig.Name, fmtTime(sig.Timestamp)))
			drop[i] = true
			continue
		}

		// Check for exact duplicates. These will be next to each other
		// after the sort.
		if i > 0 {
			lastSig := signals[i-1]
			if sig.Name == lastSig.Name && sig.Timestamp.Equal(lastSig.Timestamp) {
				// Historically, we have not emitted errors for this.
				drop[i] = true
				continue
			}
		}

		lastRef := lastReference(sig.Name)
		if lastRef == nil {
			// Not a location signal.
			continue
		}

		// We got the same signal again. Try to create before starting
		// a new location.
		if *lastRef != -1 {
			tryCreateLocationSignal()
		}

		*lastRef = i
		if lastTime.IsZero() {
			lastTime = sig.Timestamp
		}
	}

	// One last attempt, in case we're in the process of constructing
	// a location.
	tryCreateLocationSignal()

	var out []vss.Signal
	for i, sig := range signals {
		if !drop[i] {
			out = append(out, sig)
		}
	}

	out = append(out, created...)

	return out, errs
}
