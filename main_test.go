package main

import (
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
)

func TestEmptyInput(t *testing.T) {
	input := []vss.Signal{}

	actual, err := ProcessSignals(input)

	assert.NoError(t, err)
	assert.Empty(t, actual)
}

func TestNonLocationSignalUntouched(t *testing.T) {
	now := time.Now()

	input := []vss.Signal{
		{TokenID: 3, Timestamp: now, Name: vss.FieldSpeed, ValueNumber: 55},
	}

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: now, Name: vss.FieldSpeed, ValueNumber: 55},
	}

	actual, err := ProcessSignals(input)

	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func TestCreateLocationSignal(t *testing.T) {
	now := time.Now()

	input := []vss.Signal{
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLatitude, ValueNumber: 42.33432565967395},
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLongitude, ValueNumber: -83.06028627110183},
	}

	actual, err := ProcessSignals(input)

	expected := append(input, vss.Signal{TokenID: 3, Timestamp: now, Name: fieldCoordinates, ValueLocation: vss.Location{Latitude: 42.33432565967395, Longitude: -83.06028627110183}})

	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func TestCreateLocationSignalOtherOrder(t *testing.T) {
	now := time.Now()

	input := []vss.Signal{
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLongitude, ValueNumber: -83.06028627110183},
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLatitude, ValueNumber: 42.33432565967395},
	}

	actual, err := ProcessSignals(input)

	expected := append(input, vss.Signal{TokenID: 3, Timestamp: now, Name: fieldCoordinates, ValueLocation: vss.Location{Latitude: 42.33432565967395, Longitude: -83.06028627110183}})

	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func TestCreateLocationSignalMultiple(t *testing.T) {
	now := time.Now()

	input := []vss.Signal{
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLatitude, ValueNumber: 42.33432565967395},
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLongitude, ValueNumber: -83.06028627110183},
		{TokenID: 3, Timestamp: now.Add(time.Minute), Name: vss.FieldCurrentLocationLongitude, ValueNumber: -83.07573579459459},
		{TokenID: 3, Timestamp: now.Add(time.Minute), Name: vss.FieldCurrentLocationLatitude, ValueNumber: 42.335848403478145},
	}

	actual, err := ProcessSignals(input)

	expected := append(input,
		vss.Signal{TokenID: 3, Timestamp: now, Name: "currentLocationCoordinates", ValueLocation: vss.Location{Latitude: 42.33432565967395, Longitude: -83.06028627110183}},
		vss.Signal{TokenID: 3, Timestamp: now.Add(time.Minute), Name: "currentLocationCoordinates", ValueLocation: vss.Location{Latitude: 42.335848403478145, Longitude: -83.07573579459459}},
	)

	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func TestDropUnpairedCoordinate(t *testing.T) {
	now := time.Now()

	input := []vss.Signal{
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLatitude, ValueNumber: 42.33432565967395},
		{TokenID: 3, Timestamp: now.Add(time.Minute), Name: vss.FieldCurrentLocationLongitude, ValueNumber: -83.07573579459459},
		{TokenID: 3, Timestamp: now.Add(time.Minute + time.Millisecond), Name: vss.FieldCurrentLocationLatitude, ValueNumber: 42.33432565967395},
	}

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: now.Add(time.Minute), Name: vss.FieldCurrentLocationLongitude, ValueNumber: -83.07573579459459},
		{TokenID: 3, Timestamp: now.Add(time.Minute + time.Millisecond), Name: vss.FieldCurrentLocationLatitude, ValueNumber: 42.33432565967395},
		{TokenID: 3, Timestamp: now.Add(time.Minute), Name: fieldCoordinates, ValueLocation: vss.Location{Latitude: 42.33432565967395, Longitude: -83.07573579459459}},
	}

	actual, err := ProcessSignals(input)

	assert.Error(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func TestDropOriginLocation(t *testing.T) {
	now := time.Now()

	input := []vss.Signal{
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLatitude, ValueNumber: 0},
		{TokenID: 3, Timestamp: now, Name: vss.FieldCurrentLocationLongitude, ValueNumber: 0},
		{TokenID: 3, Timestamp: now.Add(time.Minute), Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 10034.2},
	}

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: now.Add(time.Minute), Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 10034.2},
	}

	actual, err := ProcessSignals(input)

	assert.Error(t, err)
	assert.ElementsMatch(t, expected, actual)
}
