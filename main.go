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

const (
	allowedLocationGap = 500 * time.Millisecond
	dropSignalName     = "__drop"
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

	var errs error
	var created []vss.Signal
	var lastTime time.Time
	lastLat := -1
	lastLon := -1
	lastHDOP := -1

	tryCreateLocationSignal := func() {
		var loc vss.Location
		var create bool

		if lastLat != -1 && lastLon != -1 {
			lat := signals[lastLat].ValueNumber
			lon := signals[lastLon].ValueNumber

			if lat == 0 && lon == 0 {
				signals[lastLat].Name = dropSignalName
				signals[lastLon].Name = dropSignalName
				errs = errors.Join(errs, fmt.Errorf("latitude and longitude at origin at time %s", fmtTime(lastTime)))
			} else {
				loc.Latitude = lat
				loc.Longitude = lon
				create = true
			}
		} else if lastLat != -1 {
			signals[lastLat].Name = dropSignalName
			errs = errors.Join(errs, fmt.Errorf("unpaired latitude at time %s", fmtTime(lastTime)))
		} else if lastLon != -1 {
			signals[lastLon].Name = dropSignalName
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

		lastLat = -1
		lastLon = -1
		lastHDOP = -1
		lastTime = zeroTime
	}

	// Sorting this way makes it easier to handle time gaps. Sorting
	// thereafter by name is not strictly necessary.
	slices.SortFunc(signals, func(a, b vss.Signal) int {
		return cmp.Or(a.Timestamp.Compare(b.Timestamp), cmp.Compare(a.Name, b.Name))
	})

	for i, sig := range signals {
		if !lastTime.IsZero() && sig.Timestamp.Sub(lastTime) >= allowedLocationGap {
			tryCreateLocationSignal()
		}

		// This logic could be made shorter and less repetitive by
		// playing around with *int.
		switch sig.Name {
		case vss.FieldCurrentLocationLatitude:
			if lastLat != -1 {
				// Start a new triple, but see if what's already being
				// tracked is enough to yield a row.
				tryCreateLocationSignal()
			}
			lastLat = i
		case vss.FieldCurrentLocationLongitude:
			if lastLon != -1 {
				tryCreateLocationSignal()
			}
			lastLon = i
		case vss.FieldDIMOAftermarketHDOP:
			if lastHDOP != -1 {
				tryCreateLocationSignal()
			}
			lastHDOP = i
		default:
			continue
		}

		if lastTime.IsZero() {
			lastTime = sig.Timestamp
		}
	}

	// One last attempt, in case we're in the process of constructing
	// a location.
	tryCreateLocationSignal()

	var out []vss.Signal
	for _, sig := range signals {
		if sig.Name != dropSignalName {
			out = append(out, sig)
		}
	}

	out = append(out, created...)

	return out, errs
}
