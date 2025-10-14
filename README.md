# Event Hub Emulator with Go Demo

This project demonstrates how to use Microsoft Azure Event Hub emulator with Docker Compose and interact with it using Go.

## Prerequisites

- Docker and Docker Compose
- Go 1.21+

## Quick Start

1. **Setup the environment:**
   ```bash
  docker compose up -d
   ```

2. **Run the demo:**
   ```bash
   ## In terminal one run:
   go run main.go

   ## In terminal two run:
   go run send-events.go
   ```

3. **Stop the containers:**
   ```bash
   docker compose down
   ```