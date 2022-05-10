package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/axiomhq/axiom-cloudfront-lambda/parser"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/pkg/cmd"
	"go.uber.org/zap"
)

func main() {
	cmd.Run("axiom-cloudwatch-lambda", run,
		cmd.WithRequiredEnvVars("AXIOM_DATASET"),
		cmd.WithValidateAxiomCredentials(),
	)
}

func run(ctx context.Context, _ *zap.Logger, client *axiom.Client) error {
	// Export `AXIOM_TOKEN`, `AXIOM_ORG_ID` and `AXIOM_DATASET` for Axiom Cloud.
	// Export `AXIOM_URL`, `AXIOM_TOKEN` and `AXIOM_DATASET` for Axiom Selfhost.

	hf := handler(client, os.Getenv("AXIOM_DATASET"))
	lambda.StartWithContext(ctx, hf)

	return nil
}

func handler(client *axiom.Client, dataset string) func(context.Context, events.S3Event) error {
	return func(ctx context.Context, logsEvent events.S3Event) error {
		// Parse logs from S3Event
		for _, record := range logsEvent.Records {
			if record.EventName != "ObjectCreated:Put" {
				continue
			}

			s3 := record.S3

			// fetch logs from S3
			file, err := os.Open(s3.Object.Key)
			if err != nil {
				return err
			}

			// parse logs
			events, err := parser.ParseCloudfrontLogs(file)
			if err != nil {
				return err
			}

			// send logs to Axiom
			if status, err := client.Datasets.IngestEvents(ctx, dataset, axiom.IngestOptions{TimestampField: "time"}, events...); err != nil {
				return err
			} else {
				log.Printf("Ingestion status: %d ingested, %d failed.\n", status.Ingested, status.Failed)
			}
		}

		return nil
	}
}
