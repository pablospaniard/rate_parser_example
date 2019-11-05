package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	radix "github.com/armon/go-radix"
)

// LegBTS is representing each row in bts file
type LegBTS struct {
	Prefix  string
	Calls   int
	Minutes float64
	Rate    float64
	Income  float64
}

// Leg is representing each row in mb file
type Leg struct {
	Source      string
	Destination string
	Minutes     float64
	Rate        float64
	Income      float64
	Currency    string
	Prefix      string
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
	var legBTS LegBTS
	var legsBTS []LegBTS

	for i, each := range btsData {
		if i != 0 {
			r.Insert(each[1], r)
			legBTS.Prefix = each[1]
			legBTS.Calls, _ = strconv.Atoi(each[2])
			legBTS.Minutes, _ = strconv.ParseFloat(each[3], 64)
			legBTS.Rate, _ = strconv.ParseFloat(each[4], 64)
			legBTS.Income, _ = strconv.ParseFloat(each[5], 64)
			legsBTS = append(legsBTS, legBTS)
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

	mbData, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	var leg Leg
	var legs []Leg
	var aggLegs map[string]LegBTS

	for i, each := range mbData {
		if i != 0 {
			secs, _ := strconv.ParseFloat(each[2], 64)
			rate, _ := strconv.ParseFloat(each[3], 64)
			leg.Source = each[0]
			leg.Destination = each[1]
			leg.Minutes = secs / 60
			leg.Rate = rate / 1000000
			leg.Income = leg.Minutes * leg.Rate
			leg.Currency = each[4]
			// Find the longest prefix match
			leg.Prefix, _, _ = r.LongestPrefix(each[1])

			if each[4] != "EUR" {
				fmt.Printf("call to %s is billed in %s", each[1], each[4])
				os.Exit(3)
			}

			legs = append(legs, leg)
		}

		for _, sl := range legs {
			if val, ok := aggLegs[sl.Prefix]; ok {
				if val.Rate != aggLegs[sl.Prefix].Rate {
					fmt.Printf("rates are different for %s, should be: %f, got: %f", val.Prefix, aggLegs[sl.Prefix].Rate, val.Rate)
					os.Exit(4)
				}
				counter := aggLegs[sl.Prefix].Calls + 1
				min := aggLegs[sl.Prefix].Minutes + val.Minutes
				income := aggLegs[sl.Prefix].Income + val.Income

				result := LegBTS{
					Prefix:  val.Prefix,
					Calls:   counter,
					Minutes: min,
					Rate:    val.Rate,
					Income:  income,
				}

				aggLegs[sl.Prefix] = result
			}
		}
	}

	// Convert to JSON
	jsonData, err := json.Marshal(aggLegs)
	if err != nil {
		fmt.Println(err)
		os.Exit(5)
	}

	fmt.Println(string(jsonData))

	jsonFile, err := os.Create("./data.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	jsonFile.Write(jsonData)
	jsonFile.Close()

}
