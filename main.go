package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// Environment holds the configuration for the application
type Environment struct {
	EventHubConnString    string
	EventHubConsumerGroup string
	StorageAccountURL     string
	StorageContainerName  string
	StorageAccountName    string
	StorageAccountKey     string
}

type Event struct {
	ID        string    `json:"id"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

type Processor struct {
	blobContainer *container.Client
	processor     *azeventhubs.Processor
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	config := loadConfig()

	checkpointStore, consumerClient, blobContainerClient, err := setupClients(config)
	if err != nil {
		log.Fatalf("Failed to setup clients: %v", err)
	}
	defer consumerClient.Close(context.Background())

	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	proc := &Processor{
		blobContainer: blobContainerClient,
		processor:     processor,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		slog.Info("Shutdown signal received, stopping processor...")
		cancel()
	}()

	slog.Info("Starting Event Hub processor...")
	if err := proc.Run(ctx); err != nil {
		log.Fatalf("Processor failed: %v", err)
	}

	slog.Info("Application terminated successfully")
}

func loadConfig() Environment {
	getEnv := func(key, defaultVal string) string {
		if val := os.Getenv(key); val != "" {
			return val
		}
		return defaultVal
	}

	return Environment{
		EventHubConnString:    getEnv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"),
		EventHubConsumerGroup: getEnv("EVENTHUB_CONSUMER_GROUP", "$Default"),
		StorageAccountURL:     getEnv("STORAGE_ACCOUNT_URL", "http://localhost:10000/devstoreaccount1"),
		StorageContainerName:  getEnv("STORAGE_CONTAINER_NAME", "checkpointsv2"),
		StorageAccountName:    getEnv("STORAGE_ACCOUNT_NAME", "devstoreaccount1"),
		StorageAccountKey:     getEnv("STORAGE_ACCOUNT_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="),
	}
}

func setupClients(config Environment) (*checkpoints.BlobStore, *azeventhubs.ConsumerClient, *container.Client, error) {
	var client *azblob.Client
	var err error

	isRunningLocally := strings.Contains(config.StorageAccountURL, "localhost") || strings.Contains(config.StorageAccountURL, "127.0.0.1")

	if isRunningLocally {
		// Use shared key authentication (default for Azurite)
		cred, err := azblob.NewSharedKeyCredential(config.StorageAccountName, config.StorageAccountKey)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create credential: %w", err)
		}

		client, err = azblob.NewClientWithSharedKeyCredential(config.StorageAccountURL, cred, nil)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create blob client: %w", err)
		}
		slog.Info("Using Shared Key authentication for Azure Storage")

	} else {
		// Use DefaultAzureCredential for production
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create default credential: %w", err)
		}
		slog.Info("Using DefaultAzureCredential for Azure Storage")

		client, err = azblob.NewClient(config.StorageAccountURL, cred, nil)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create blob client with OAuth: %w", err)
		}
	}

	containerClient := client.ServiceClient().NewContainerClient(config.StorageContainerName)

	_, err = containerClient.Create(context.Background(), nil)
	if err != nil {
		slog.Info("Container might already exist", slog.Any("error", err))
	}

	checkpointStore, err := checkpoints.NewBlobStore(containerClient, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create checkpoint store: %w", err)
	}

	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(
		config.EventHubConnString,
		"eh1",
		config.EventHubConsumerGroup,
		nil,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create consumer client: %w", err)
	}

	return checkpointStore, consumerClient, containerClient, nil
}

func (p *Processor) Run(ctx context.Context) error {
	dispatchPartitionClients := func() {
		defer slog.Info("Processor terminating")
		for {
			slog.Info("Waiting for the next partition client")
			ppc := p.processor.NextPartitionClient(ctx)
			if ppc == nil {
				break
			}

			slog.Info("Found partition client", slog.String("partition_id", ppc.PartitionID()))

			go func() {
				if err := p.listen(ctx, ppc); err != nil {
					if err.Error() == "client has been closed by user" || errors.Is(err, context.Canceled) {
						slog.Info(err.Error())
						return
					}
					slog.Error("Listener error", slog.Any("error", err))
				}
			}()
		}
	}

	go dispatchPartitionClients()

	slog.Info("Starting consumer")
	return p.processor.Run(ctx)
}

func (p *Processor) listen(ctx context.Context, ppc *azeventhubs.ProcessorPartitionClient) error {
	defer func() {
		slog.InfoContext(ctx, "Processor listener terminating")
		if err := ppc.Close(ctx); err != nil {
			slog.WarnContext(ctx, "Cannot close partition client", slog.Any("error", err))
		}
	}()

	const maxEventsCount = 100
	const receiveEventsTimeout = 10 * time.Second

	for {
		slog.InfoContext(ctx, "Waiting for events")

		events, err := func() ([]*azeventhubs.ReceivedEventData, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			rctx, cancel := context.WithTimeout(context.TODO(), receiveEventsTimeout)
			events, err := ppc.ReceiveEvents(rctx, maxEventsCount, nil)
			cancel()

			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}

			if len(events) != 0 {
				if err := ppc.UpdateCheckpoint(ctx, events[len(events)-1], nil); err != nil {
					return nil, err
				}
			}

			return events, nil
		}()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.ErrorContext(ctx, "Failed to receive events", slog.Any("error", err))
			}
			return err
		}

		if len(events) == 0 {
			continue
		}

		p.process(ctx, ppc, events)
	}
}

func (p *Processor) process(ctx context.Context, ppc *azeventhubs.ProcessorPartitionClient, events []*azeventhubs.ReceivedEventData) {
	slog.InfoContext(ctx, "Processing event(s)", slog.Int("count", len(events)))

	for _, e := range events {
		logger := slog.With(slog.Int64("sequence_number", e.SequenceNumber))

		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := p.ProcessEvent(ctx, e); err != nil {
			logger.InfoContext(ctx, "can't process event", slog.Any("error", err))
		}
	}
}

func (p *Processor) ProcessEvent(ctx context.Context, event *azeventhubs.ReceivedEventData) error {
	var evt Event
	if err := json.Unmarshal(event.Body, &evt); err != nil {
		return fmt.Errorf("failed to unmarshal event data: %w", err)
	}

	slog.Info("Processing event",
		slog.String("id", evt.ID),
		slog.String("message", evt.Message),
		slog.Time("timestamp", evt.Timestamp),
		slog.Int64("sequence_number", event.SequenceNumber))
	return nil
}
