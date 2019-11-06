package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	radix "github.com/armon/go-radix"
)

// LegBTS is representing each row in bts file
type LegBTS struct {
	Prefix  string
	Calls   int
	Seconds int
	Rate    float64
	Income  float64
}

// Leg is representing each row in mb file
type Leg struct {
	Source      string
	Destination string
	Seconds     int
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
	var legsBTS []LegBTS

	for i, each := range btsData {
		if i != 0 {
			r.Insert(each[1], r)
			var legBTS LegBTS
			legBTS.Prefix = each[1]
			legBTS.Calls, _ = strconv.Atoi(each[2])
			legBTS.Seconds, _ = strconv.Atoi(each[3])
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

	var legs []Leg
	for i, each := range mbData {
		if i != 0 {
			rate, _ := strconv.ParseFloat(each[3], 64)
			var leg Leg
			leg.Source = each[0]
			leg.Destination = each[1]
			leg.Seconds, _ = strconv.Atoi(each[2])
			leg.Rate = rate / 1000000
			leg.Income = float64(leg.Seconds) * leg.Rate
			leg.Currency = each[4]
			// Find the longest prefix match
			leg.Prefix, _, _ = r.LongestPrefix(each[1])

			if each[4] != "EUR" {
				fmt.Printf("call to %s is billed in %s", each[1], each[4])
				os.Exit(3)
			}

			legs = append(legs, leg)
		}

	}

	aggLegs := map[string]LegBTS{}
	for i, sl := range legs {
		result := LegBTS{}
		if val, ok := aggLegs[sl.Prefix]; ok {
			if val.Rate != sl.Rate {
				aggLegs[sl.Prefix+"_"+string(i)] = LegBTS{
					Prefix:  sl.Prefix,
					Calls:   1,
					Seconds: sl.Seconds,
					Rate:    sl.Rate,
					Income:  sl.Income,
				}
			}
			result = LegBTS{
				Prefix:  val.Prefix,
				Calls:   val.Calls + 1,
				Seconds: val.Seconds + sl.Seconds,
				Rate:    val.Rate,
				Income:  val.Income + sl.Income,
			}
		} else {
			result = LegBTS{
				Prefix:  sl.Prefix,
				Calls:   1,
				Seconds: sl.Seconds,
				Rate:    sl.Rate,
				Income:  sl.Income,
			}
		}
		aggLegs[sl.Prefix] = result
	}

	file, err := os.OpenFile("agg_data.csv", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Print(err)
		os.Exit(5)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	headers := []string{
		"Prefix",
		"Calls",
		"Minutes",
		"Rate",
		"Outcome",
	}
	if err := writer.Write(headers); err != nil {
		fmt.Printf("couldn't write headers: %v", err)
		os.Exit(6)
	}

	others := [][]string{}
	count := 0

	for _, aggLeg := range aggLegs {

		line := make([]string, len(headers))

		line[0] = aggLeg.Prefix
		line[1] = fmt.Sprintf("%d", aggLeg.Calls)
		line[2] = fmt.Sprintf("%d", (aggLeg.Seconds / 60))
		line[3] = fmt.Sprintf("%.4f", aggLeg.Rate)
		line[4] = fmt.Sprintf("%.4f", (aggLeg.Income / 60))

		others = append(others, line)
		count++
	}

	// SAVE

	if err := writer.WriteAll(others); err != nil {
		fmt.Printf("couldn't save %v: %s", others, err)
	}
	writer.Flush()

	fmt.Print("enjoy!\n")

}
