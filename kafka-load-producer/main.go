package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

type Client struct {
	Last_name        string
	First_name       string
	Middle_name      string
	Birth_date       string
	MainDocType      string
	MainDocNbr       string
	AddDocType       string
	AddDocNbr        string
	MainDocIssueDate string
	MainDocIssueAuth string
	AddDocIssueDate  string
	AddDocIssueAuth  string
}

func ParseFile(file string) []string {
	filePath, err := os.Open(file)
	if err != nil {
		log.Fatal("Unable to read input file"+file, err)
	}
	defer filePath.Close()

	csvReader := csv.NewReader(filePath)
	//record, err := csvReader.ReadAll()
	record, err := csvReader.Read()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+file, err)
	}

	return record
}

//func SendToTopic(client Client) error {
//	dsad
//}

func main() {

	fmt.Println(ParseFile("test.csv"))
}
