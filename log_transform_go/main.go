package main

// Referenced https://docs.redpanda.com/current/develop/data-transforms/build/?tab=tabs-1-go

import (
	"bytes"
	"encoding/json"
	"io"
	"log"

	"github.com/valyala/bytebufferpool"
)

// The Record structure represents a Kafka record.
type Record struct {
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

func transform(input []byte) []byte {
	var record Record
	if err := json.Unmarshal(input, &record); err != nil {
		log.Printf("Failed to unmarshal input: %v", err)
		return nil
	}

	// Parse the log message from the record value
	var logEntry map[string]interface{}
	if err := json.Unmarshal(record.Value, &logEntry); err != nil {
		log.Printf("Failed to unmarshal log entry: %v", err)
		return nil
	}

	// Filtering Logic
	if level, ok := logEntry["level"].(string); ok {
		if level == "DEBUG" || level == "INFO" {
			// Discard the log
			return nil
		}
	}

	// Remove unnecessary fields
	delete(logEntry, "unnecessaryField")

	// Serialize the modified log entry back to JSON
	modifiedLogEntry, err := json.Marshal(logEntry)
	if err != nil {
		log.Printf("Failed to marshal modified log entry: %v", err)
		return nil
	}

	// Compress the log using GZIP
	var compressedLog bytes.Buffer
	gzipWriter := bytebufferpool.Get()
	defer bytebufferpool.Put(gzipWriter)
	gzipWriter.Reset()
	gzipWriter.Write(modifiedLogEntry)
	compressedData := gzipWriter.Bytes()

	// Update the record value with compressed data
	record.Value = compressedData

	// Add a header to indicate compression
	if record.Headers == nil {
		record.Headers = make(map[string][]byte)
	}
	record.Headers["Content-Encoding"] = []byte("gzip")

	// Marshal the modified record back to JSON
	output, err := json.Marshal(record)
	if err != nil {
		log.Printf("Failed to marshal output record: %v", err)
		return nil
	}

	return output
}

// Required by TinyGo to export the transform function
func main() {
	// Read input from stdin
	input, err := io.ReadAll(io.Reader(os.Stdin))
	if err != nil {
		log.Fatalf("Failed to read input: %v", err)
	}

	// Call the transform function
	output := transform(input)

	// Write output to stdout
	if output != nil {
		_, err = os.Stdout.Write(output)
		if err != nil {
			log.Fatalf("Failed to write output: %v", err)
		}
	}
}
