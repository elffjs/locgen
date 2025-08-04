package main

import (
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
)

func TestAssembleWithSkew(t *testing.T) {
	firstTs := time.Date(2025, time.April, 19, 9, 0, 0, 0, time.UTC)
	signals := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(5 * time.Millisecond), Name: fieldLongitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(7 * time.Millisecond), Name: fieldHDOP, ValueNumber: 4.2},
	}

	store := New(signals)
	actual := store.ProcessAll()

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(5 * time.Millisecond), Name: fieldLongitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(7 * time.Millisecond), Name: fieldHDOP, ValueNumber: 4.2},
		{TokenID: 3, Timestamp: firstTs, Name: fieldLocation, ValueLocation: vss.Location{Latitude: 39.997257245426994, Longitude: -78.03109355400886, HDOP: 4.2}},
	}

	assert.ElementsMatch(t, expected, actual)
}

func TestWithMultipleLocations(t *testing.T) {
	firstTs := time.Date(2025, time.April, 19, 9, 0, 0, 0, time.UTC)
	signals := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(5 * time.Millisecond), Name: fieldLongitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(7 * time.Millisecond), Name: fieldHDOP, ValueNumber: 4.2},
		{TokenID: 3, Timestamp: firstTs.Add(10 * time.Millisecond), Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1002},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLatitude, ValueNumber: 39.12488084657269},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldLongitude, ValueNumber: -80.9534567391833},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldHDOP, ValueNumber: 3.8},
	}

	store := New(signals)
	actual := store.ProcessAll()

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(5 * time.Millisecond), Name: fieldLongitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(7 * time.Millisecond), Name: fieldHDOP, ValueNumber: 4.2},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLatitude, ValueNumber: 39.12488084657269},
		{TokenID: 3, Timestamp: firstTs.Add(10 * time.Millisecond), Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1002},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldLongitude, ValueNumber: -80.9534567391833},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldHDOP, ValueNumber: 3.8},

		{TokenID: 3, Timestamp: firstTs, Name: fieldLocation, ValueLocation: vss.Location{Latitude: 39.997257245426994, Longitude: -78.03109355400886, HDOP: 4.2}},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLocation, ValueLocation: vss.Location{Latitude: 39.12488084657269, Longitude: -80.9534567391833, HDOP: 3.8}},
	}

	assert.ElementsMatch(t, expected, actual)
}

func TestDuplicateLocations(t *testing.T) {
	firstTs := time.Date(2025, time.April, 19, 9, 0, 0, 0, time.UTC)
	signals := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(5 * time.Millisecond), Name: fieldLongitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(6 * time.Millisecond), Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(7 * time.Millisecond), Name: fieldHDOP, ValueNumber: 4.2},
		{TokenID: 3, Timestamp: firstTs.Add(9 * time.Millisecond), Name: fieldLongitude, ValueNumber: -80.9534567391833},
		{TokenID: 3, Timestamp: firstTs.Add(10 * time.Millisecond), Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1002},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLatitude, ValueNumber: 39.12488084657269},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldLongitude, ValueNumber: -80.9534567391833},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldHDOP, ValueNumber: 3.8},
	}

	store := New(signals)
	actual := store.ProcessAll()

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(5 * time.Millisecond), Name: fieldLongitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(6 * time.Millisecond), Name: fieldLatitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(7 * time.Millisecond), Name: fieldHDOP, ValueNumber: 4.2},
		{TokenID: 3, Timestamp: firstTs.Add(9 * time.Millisecond), Name: fieldLongitude, ValueNumber: -80.9534567391833},
		{TokenID: 3, Timestamp: firstTs.Add(10 * time.Millisecond), Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1002},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLatitude, ValueNumber: 39.12488084657269},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldLongitude, ValueNumber: -80.9534567391833},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 2*time.Millisecond), Name: fieldHDOP, ValueNumber: 3.8},

		{TokenID: 3, Timestamp: firstTs, Name: fieldLocation, ValueLocation: vss.Location{Latitude: 39.997257245426994, Longitude: -78.03109355400886}},
		{TokenID: 3, Timestamp: firstTs.Add(6 * time.Millisecond), Name: fieldLocation, ValueLocation: vss.Location{Latitude: 39.997257245426994, Longitude: -80.9534567391833, HDOP: 4.2}},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLocation, ValueLocation: vss.Location{Latitude: 39.12488084657269, Longitude: -80.9534567391833, HDOP: 3.8}},
	}

	assert.ElementsMatch(t, expected, actual)
}

func TestDropExactDuplicates(t *testing.T) {
	firstTs := time.Date(2025, time.April, 19, 9, 0, 0, 0, time.UTC)
	signals := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1001},
		{TokenID: 3, Timestamp: firstTs, Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1002},
		{TokenID: 3, Timestamp: firstTs, Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1003},
	}

	store := New(signals)
	actual := store.ProcessAll()

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 1001},
	}

	assert.ElementsMatch(t, expected, actual)
}

func TestDropUnpairedCoordinates(t *testing.T) {
	firstTs := time.Date(2025, time.April, 19, 9, 0, 0, 0, time.UTC)
	signals := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs, Name: fieldLongitude, ValueNumber: 39.997257245426992},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLatitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 6*time.Millisecond), Name: fieldLongitude, ValueNumber: 39.997257245426994},
	}

	store := New(signals)
	actual := store.ProcessAll()

	expected := []vss.Signal{
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLatitude, ValueNumber: -78.03109355400886},
		{TokenID: 3, Timestamp: firstTs.Add(2*time.Second + 6*time.Millisecond), Name: fieldLongitude, ValueNumber: 39.997257245426994},
		{TokenID: 3, Timestamp: firstTs.Add(2 * time.Second), Name: fieldLocation, ValueLocation: vss.Location{Latitude: -78.03109355400886, Longitude: 39.997257245426994}},
	}

	assert.ElementsMatch(t, expected, actual)
}
