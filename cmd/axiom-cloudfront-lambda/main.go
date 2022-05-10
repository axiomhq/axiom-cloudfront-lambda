package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/pkg/cmd"
	"go.uber.org/zap"

	"github.com/axiomhq/axiom-cloudfront-lambda/parser"
)

func main() {
	cmd.Run("axiom-cloudfront-lambda", run,
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

func downloadS3Object(entity events.S3Entity) (io.ReadCloser, error) {
	// Download S3 object
	sess := session.Must(session.NewSession())
	s3Client := s3.New(sess)
	input := &s3.GetObjectInput{
		Bucket: aws.String(entity.Bucket.Name),
		Key:    aws.String(entity.Object.Key),
	}
	if output, err := s3Client.GetObject(input); err != nil {
		return nil, err
	} else {
		return output.Body, nil
	}
}

func handler(client *axiom.Client, dataset string) func(context.Context, events.S3Event) error {
	return func(ctx context.Context, logsEvent events.S3Event) error {
		// Parse logs from S3Event
		for _, record := range logsEvent.Records {
			if record.EventName != "ObjectCreated:Put" {
				continue
			}

			// fetch logs from S3
			objectReader, err := downloadS3Object(record.S3)
			if err != nil {
				return err
			}

			// gunzip
			gzipReader, err := gzip.NewReader(objectReader)
			if err != nil {
				return fmt.Errorf("failed to gunzip file: %w", err)
			}

			// parse logs
			events, err := parser.ParseCloudfrontLogs(gzipReader)
			if err != nil {
				return err
			}

			// close s3Reader
			if err := objectReader.Close(); err != nil {
				return err
			}

			// close gzip reader
			if err := gzipReader.Close(); err != nil {
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
