package main

import (
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
)

const (
	// maxLocationTimestampGap is the maximum size of the time gap
	// between location signals (latitude, longitude, HDOP) such that
	// we still consider the signals to be related.
	maxLocationTimestampGap = 500 * time.Millisecond

	fieldLatitude  = vss.FieldCurrentLocationLatitude
	fieldLongitude = vss.FieldCurrentLocationLongitude
	fieldHDOP      = vss.FieldDIMOAftermarketHDOP

	fieldLocation = "currentLocation" // TODO(elffjs): Move this to the vss package.
)

type Store struct {
	Signals        []vss.Signal
	AddedSignals   []vss.Signal
	TemplateSignal *vss.Signal
	ActiveLat      Cell
	ActiveLon      Cell
	ActiveHDOP     Cell
	//
	//
	// Previously, we've set the Name fields of signals to magic
	// strings in order to flag for deletion. This worked but may
	// cause problems when there is more than one duplicate of a
	// signal.
	SignalsToDelete []bool
	// InFlightTimestamp is the earliest timestamp among the latitude,
	// longitude, and HDOP; if, indeed, any are populated.
	InFlightTimestamp time.Time
}

var zeroTime time.Time

type Cell struct {
	filled bool
	index  int
}

func (c *Cell) Filled() bool {
	return c.filled
}

// Get returns the index stored for this cell, if any. If the cell is
// empty then the sentinel value -1 is returned.
func (c *Cell) Get() int {
	if c.filled {
		return c.index
	}
	return -1
}

func (c *Cell) Set(index int) {
	c.filled = true
	c.index = index
}

func (c *Cell) Clear() {
	c.filled = false
}

func (s *Store) GetValue(c Cell) float64 {
	return s.Signals[c.Get()].ValueNumber
}

func (s *Store) DropValue(signalIndex int) {
	if signalIndex != -1 {
		s.SignalsToDelete[signalIndex] = true
	}
}

// TryFlush attempts to create a new location row with any active
// signals. If this is not possible then any active signals will be
// marked for deletion.
func (s *Store) TryFlush() {
	var (
		flushable bool
		loc       vss.Location
	)

	if s.ActiveLat.Filled() && s.ActiveLon.Filled() {
		flushable = true
		loc.Latitude = s.GetValue(s.ActiveLat)
		loc.Longitude = s.GetValue(s.ActiveLon)
	} else {
		s.DropValue(s.ActiveLat.Get())
		s.DropValue(s.ActiveLon.Get())
	}

	if s.ActiveHDOP.Filled() {
		flushable = true
		loc.HDOP = s.GetValue(s.ActiveHDOP)
	}

	if flushable {
		s.AddedSignals = append(s.AddedSignals,
			vss.Signal{
				TokenID:       s.TemplateSignal.TokenID,
				Timestamp:     s.InFlightTimestamp,
				Name:          fieldLocation,
				ValueLocation: loc,
				Source:        s.TemplateSignal.Source,
				Producer:      s.TemplateSignal.Producer,
				CloudEventID:  s.TemplateSignal.CloudEventID,
			})
	}

	// Reset everything.
	s.ActiveLat.Clear()
	s.ActiveLon.Clear()
	s.ActiveHDOP.Clear()
	s.InFlightTimestamp = zeroTime
}

func (s *Store) HasAnyActive() bool {
	return !s.InFlightTimestamp.IsZero()
}

// EnsureTimestamp sets the timestamp for the vss.Location currently
// being built, if this timestamp has not already been set.
func (s *Store) EnsureTimestamp(t time.Time) {
	if s.InFlightTimestamp.IsZero() {
		s.InFlightTimestamp = t
	}
}

func (s *Store) Process(i int) {
	sig := &s.Signals[i]

	// Remove exact duplicates, from the perspective of the dimo.signal
	// table index.
	if i > 0 && sig.Timestamp.Equal(s.Signals[i-1].Timestamp) && sig.Name == s.Signals[i-1].Name {
		s.DropValue(i)
	}

	if s.HasAnyActive() && sig.Timestamp.After(s.InFlightTimestamp.Add(maxLocationTimestampGap)) {
		s.TryFlush()
	}

	cell := s.GetActiveIndex(sig.Name)
	if cell != nil {
		if cell.Filled() {
			// We got another value for this signal too quickly.
			s.DropValue(i)
		} else {
			// We could attempt to Flush if all three signals happen
			// to be active, but that would be more code, s we don't.
			cell.Set(i)
		}
		s.EnsureTimestamp(sig.Timestamp)
	}
}

func (s *Store) GetActiveIndex(signalName string) *Cell {
	switch signalName {
	case fieldLatitude:
		return &s.ActiveLat
	case fieldLongitude:
		return &s.ActiveLon
	case fieldHDOP:
		return &s.ActiveHDOP
	default:
		return nil
	}
}

func (s *Store) ProcessAll() []vss.Signal {
	if len(s.Signals) == 0 {
		return s.Signals
	}

	s.TemplateSignal = &s.Signals[0]
	s.SignalsToDelete = make([]bool, len(s.Signals))

	for i := range s.Signals {
		s.Process(i)
	}

	// See if we can make something out of the last signal.
	s.TryFlush()

	var out []vss.Signal
	for i := range s.Signals {
		if !s.SignalsToDelete[i] {
			out = append(out, s.Signals[i])
		}
	}

	out = append(out, s.AddedSignals...)

	return out
}

func New(signals []vss.Signal) *Store {
	return &Store{
		Signals: signals,
	}
}
