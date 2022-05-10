package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/axiomhq/axiom-cloudfront-lambda/parser"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/pkg/cmd"
	"go.uber.org/zap"
)

var (
	downloader     *s3manager.Downloader
	downloaderOnce sync.Once
)

func main() {
	downloaderOnce.Do(func() {
		// TODO: Do we need credentials
		sess := session.Must(session.NewSession())
		downloader = s3manager.NewDownloader(sess)
	})

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

			// Download file
			buf := aws.NewWriteAtBuffer([]byte{})
			_, err := downloader.DownloadWithContext(ctx, buf, &s3.GetObjectInput{
				Bucket: aws.String(record.S3.Bucket.Name),
				Key:    aws.String(record.S3.Object.Key),
			})
			if err != nil {
				return fmt.Errorf("failed to download file: %w", err)
			}
			contents := bytes.NewBuffer(buf.Bytes()) // TODO: Can we stream directly into the reader?

			// Gunzip
			gzipReader, err := gzip.NewReader(contents)
			if err != nil {
				return fmt.Errorf("failed to gunzip file: %w", err)
			}

			// Parse
			events, err := parser.ParseCloudfrontLogs(gzipReader)
			if err != nil {
				return err
			}

			// Send logs to Axiom
			if status, err := client.Datasets.IngestEvents(ctx, dataset, axiom.IngestOptions{TimestampField: "time"}, events...); err != nil {
				return err
			} else {
				log.Printf("Ingestion status: %d ingested, %d failed.\n", status.Ingested, status.Failed)
			}
		}

		return nil
	}
}
