// package main

// import (
// 	"context"
// 	"fmt"
// 	"log"

// 	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
// )

// const (
// 	azuriteAccountName = "devstoreaccount1"
// 	azuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
// 	testServiceAddress = "http://127.0.0.1:10000/devstoreaccount1"
// 	containerName      = "checkpoints"
// )

// func main() {
// 	cred, err := azblob.NewSharedKeyCredential(azuriteAccountName, azuriteAccountKey)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	ctx := context.Background()

// 	fmt.Printf("Listing blobs in container '%s':\n\n", containerName)

// 	pager := client.NewListBlobsFlatPager(containerName, nil)

// 	count := 0
// 	for pager.More() {
// 		page, err := pager.NextPage(ctx)
// 		if err != nil {
// 			log.Fatalf("Failed to list blobs: %v", err)
// 		}

// 		for _, blob := range page.Segment.BlobItems {
// 			count++
// 			fmt.Printf("  %d. %s (size: %d bytes)\n", count, *blob.Name, *blob.Properties.ContentLength)
// 		}
// 	}

// 	if count == 0 {
// 		fmt.Println("  (no blobs found)")
// 	} else {
// 		fmt.Printf("\nTotal: %d blob(s)\n", count)
// 	}
// }
