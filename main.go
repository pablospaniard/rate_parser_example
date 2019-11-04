package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	radix "github.com/armon/go-radix"
)

// Leg is representing each row in xls file
type LegBTS struct {
	Destination string
	Prefix      int
	Calls       int
	Minutes     float64
	Rate        float64
	Income      float64
	Currency    string
}

type Leg struct {
	Source      string
	Destination string
	Minutes     float64
	Rate        float64
	Income      float64
	Currency    string
}

type Tree struct {
	Destination string
	Prefix      int
}

func main() {
	btsFile, err := os.Open("./mexico.csv")
	if err != nil {
		fmt.Println(err)
	}
	defer btsFile.Close()

	btsReader := csv.NewReader(btsFile)
	btsReader.FieldsPerRecord = -1
	btsReader.LazyQuotes = true

	btsData, err := btsReader.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	r := radix.New()

	for i, each := range btsData {
		if i != 0 {
			r.Insert(each[1], r)
		}
	}

	csvFile, err := os.Open("./logs.csv")
	if err != nil {
		fmt.Println(err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true

	csvData, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var leg Leg
	var legs []Leg

	for i, each := range csvData {
		if i != 0 {
			secs, _ := strconv.ParseFloat(each[2], 64)
			rate, _ := strconv.ParseFloat(each[3], 64)
			leg.Source = each[0]
			leg.Destination = each[1]
			leg.Minutes = secs / 60
			leg.Rate = rate / 1000000
			leg.Income = leg.Minutes * leg.Rate
			leg.Currency = each[4]
			legs = append(legs, leg)
		}
	}
}
