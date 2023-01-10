package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Taxi Trips dataset retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// trip_id	"c354c843908537bbf90997917b714f1c63723785"
// trip_start_timestamp	"2021-11-13T22:45:00.000"
// trip_end_timestamp	"2021-11-13T23:00:00.000"
// trip_seconds	"703"
// trip_miles	"6.83"
// pickup_census_tract	"17031840300"
// dropoff_census_tract	"17031081800"
// pickup_community_area	"59"
// dropoff_community_area	"8"
// fare	"27.5"
// tip	"0"
// additional_charges	"1.02"
// trip_total	"28.52"
// shared_trip_authorized	false
// trips_pooled	"1"
// pickup_centroid_latitude	"41.8335178865"
// pickup_centroid_longitude	"-87.6813558293"
// pickup_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6813558293
// 		1	41.8335178865
// dropoff_centroid_latitude	"41.8932163595"
// dropoff_centroid_longitude	"-87.6378442095"
// dropoff_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6378442095
// 		1	41.8932163595
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentJsonRecords []struct {
	Community_Area             string `json:"community_area"`
	Community_Area_Name        string `json:"community_area_name"`
	Per_Capita_Income          string `json:"per_capita_income"`
	Unemployment_Rate          string `json:"unemployment_rate"`
}

type BuildingPermitJsonRecords []struct {
	Permit_id string `json:"permit_id"` 
	Permit_Number string `json:"permit_number"` 
	Permit_Type string `json:"permit_type"` 
	Community_Area string `json:"community_area"` 
	Longitude string `json:"centroid_longitude"` 
	Latitude string `json:"centroid_latitude"`  
}


func main() {

	// Establish connection to Postgres Database
	db_connection := "user=postgres dbname=chicago_business_intelligence password=atharva host=localhost sslmode=disable"

	// Docker image for the microservice - uncomment when deploy
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable"

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		panic(err)
	}

	// Test the database connection
	err = db.Ping()
	if err != nil {
		fmt.Println("Couldn't Connect to database")
		panic(err)
	}

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary
	for {
		// build and fine-tune functions to pull data from different data sources
		// This is a code snippet to show you how to pull data from different data sources.
		GetTaxiTrips(db)
		GetUnemploymentRates(db)
		GetBuildingPermits(db)

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

func GetTaxiTrips(db *sql.DB) {


	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p



	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "ADD-YOUR-API-KEY-HERE"


	drop_table := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list TaxiTripsJsonRecords
	json.Unmarshal(body, &taxi_trips_list)

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($taxi_trips_list[i].Trip_id, $taxi_trips_list[i].Trip_start_timestamp, $taxi_trips_list[i].Trip_end_timestamp, $taxi_trips_list[i].Pickup_centroid_latitude, $taxi_trips_list[i].Pickup_centroid_longitude, $taxi_trips_list[i].Dropoff_centroid_latitude, $taxi_trips_list[i].Dropoff_centroid_longitude, $taxi_trips_list[i].Pickup_zip_code, $taxi_trips_list[i].Dropoff_zip_code)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

	}

}

func GetUnemploymentRates(db *sql.DB) {
	fmt.Println("GetUnemploymentRates: Collecting Unemployment Rates Data")

	geocoder.ApiKey = "ADD-YOUR-API-KEY-HERE"


	//creating building_permit table 
	drop_table := `drop table if exists unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment" (
						"id"   SERIAL ,  
						"community_area" VARCHAR(255),
						"community_area_name" VARCHAR(255),
						"per_capita_income" VARCHAR(255), 
						"unemployment_rate" VARCHAR(255)   
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_list)

	for i := 0; i < len(unemployment_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		// get community_area
		community_area := unemployment_list[i].Community_Area
		if community_area == "" {
			continue
		}

		// get community_area_name
		community_area_name := unemployment_list[i].Community_Area_Name
		if community_area_name == "" {
			continue
		}

		// get per_capital_income
		per_capital_income := unemployment_list[i].Per_Capita_Income
		if per_capital_income == "" {
			continue
		}

		//get unemployment_rate
		unemployment_rate := unemployment_list[i].Unemployment_Rate
		if unemployment_rate == "" {
			continue
		}

		sql := `INSERT INTO unemployment ("community_area", "community_area_name", "per_capital_income", "unemployment_rate") 
			values($unemploment_list[i].Community_Area, $unemploment_list[i].Community_Area_Name, $unemploment_list[i].Per_Capita_Income, $unemploment_list[i].Unemployment_Rate)`

		_, err = db.Exec(
			sql,
			community_area,
			community_area_name,
			per_capita_income,
			unemployment_rate)

		if err != nil {
			panic(err)
		}

	fmt.Println("GetUnemploymentRates: Implement Unemployment")

	
}

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	geocoder.ApiKey = "ADD-YOUR-API-KEY-HERE"


	//creating building_permit table 
	drop_table := `drop table if exists building_permit`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permit" (
						"id"   SERIAL , 
						"permit_id" VARCHAR(255) UNIQUE, 
						"permit_number" VARCHAR(255) UNIQUE, 
						"permit_type" VARCHAR(255), 
						"community_area" VARCHAR(255),
						"ZIP_code" VARCHAR(255), 
						"centroid_longitude" DOUBLE PRECISION, 
						"centroid_latitude" DOUBLE PRECISION,  
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	//change url
	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var building_permit_list BuildingPermitJsonRecords
	json.Unmarshal(body, &building_permit_list)

	for i := 0; i < len(building_permit_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		// get permit_id
		permit_id := building_permit_list[i].Permit_id
		if permit_id == "" {
			continue
		}

		// get permit_number
		permit_number := building_permit_list[i].Permit_Number
		if permit_number == "" {
			continue
		}

		// get permit_number
		community_area := building_permit_list[i].Community_Area
		if community_area == "" {
			continue
		}

		//get latitude
		centroid_latitude := building_permit_list[i].Latitude
		if centroid_latitude == "" {
			continue
		}

		//get longitude
		centroid_longitude := building_permit_list[i].Longitude
		if centroid_longitude == "" {
			continue
		}

		// Using centroid_latitude and centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		centroid_latitude_float, _ := strconv.ParseFloat(centroid_latitude, 64)
		centroid_longitude_float, _ := strconv.ParseFloat(centroid_longitude, 64)
		permit_location := geocoder.Location{
			Latitude:  centroid_latitude_float,
			Longitude: centroid_longitude_float,
		}

		permit_address_list, _ := geocoder.GeocodingReverse(permit_location)
		permit_address := permit_address_list[0]
		permit_zip_code := permit_address.PostalCode

		sql := `INSERT INTO building_permit ("permit_id", "permit_name", "community_area", "ZIP_code", "centroid_longitude", "centroid_latitude" )
		 values($building_permit_list[i].Permit_id, $building_permit_list[i].Permit_Name, $building_permit_list[i].Community_Name, $permit_zip_code, $building_permit_list[i].Longitude, $building_permit_list[i].Latitude)`

		_, err = db.Exec(
			sql,
			permit_id,
			permit_name,
			community_area,
			ZIP_code,
			centroid_longitude,
			centroid_latitude)

		if err != nil {
			panic(err)
		}

	}



	fmt.Println("GetBuildingPermits: Implement Building Permits")
}
