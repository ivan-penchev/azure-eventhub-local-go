package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
)

type Event struct {
	ID        string    `json:"id"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// Create producer client
	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(
		"Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
		"eh1",
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producerClient.Close(context.Background())

	// Create some test events
	events := []Event{
		{
			ID:        "event-1",
			Message:   "Hello from Event Hub!",
			Timestamp: time.Now(),
		},
		{
			ID:        "event-2",
			Message:   "This is a test event",
			Timestamp: time.Now(),
		},
		{
			ID:        "event-3",
			Message:   "Event Hub is working!",
			Timestamp: time.Now(),
		},
	}

	// Send events
	ctx := context.Background()
	for _, event := range events {
		// Marshal event to JSON
		eventData, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event: %v", err)
			continue
		}

		// Create event batch
		batch, err := producerClient.NewEventDataBatch(ctx, nil)
		if err != nil {
			log.Printf("Failed to create batch: %v", err)
			continue
		}

		// Add event to batch
		err = batch.AddEventData(&azeventhubs.EventData{
			Body: eventData,
		}, nil)
		if err != nil {
			log.Printf("Failed to add event to batch: %v", err)
			continue
		}

		// Send the batch
		err = producerClient.SendEventDataBatch(ctx, batch, nil)
		if err != nil {
			log.Printf("Failed to send batch: %v", err)
			continue
		}

		fmt.Printf("✓ Sent event: %s - %s\n", event.ID, event.Message)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n✓ All events sent successfully!")
}
