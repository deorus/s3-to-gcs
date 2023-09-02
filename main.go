package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var printer = message.NewPrinter(language.English)

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if days > 0 {
		return printer.Sprintf("%dd %2dh %2dm %2ds", days, hours, minutes, seconds)
	} else if hours > 0 {
		return printer.Sprintf("%2dh %2dm %2ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return printer.Sprintf("%2dm %2ds", minutes, seconds)
	} else {
		return printer.Sprintf("%ds", seconds)
	}
}

func deleteAllVersions(ctx context.Context, bucket *storage.BucketHandle, objectKey string) error {
	it := bucket.Objects(ctx, &storage.Query{
		Prefix:    objectKey,
		Versions:  true,
		Delimiter: "/",
	})
	for {
		attrs, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			log.Fatalf("Error iterating over versions of object %s: %v", objectKey, err)
			return err
		}

		// Delete the specific version of the object
		object := bucket.Object(attrs.Name).Generation(attrs.Generation)
		if err := object.Delete(ctx); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	forceFlag := flag.Bool("force", false, "Force copying objects, skipping checksum comparison")
	flag.Parse()

	if len(flag.Args()) < 2 || len(flag.Args()) > 3 {
		log.Fatal("Usage: ./s3-to-gcs [-force] <S3 bucket> <GCS bucket> [optional object key prefix]")
	}

	s3Bucket := flag.Arg(0)
	gcsBucket := flag.Arg(1)

	var objectKeyPrefix string
	if len(flag.Args()) == 3 {
		objectKeyPrefix = flag.Arg(2)
	}

	log.Printf("S3 bucket: %s", s3Bucket)
	log.Printf("GCS bucket: %s", gcsBucket)
	if objectKeyPrefix != "" {
		log.Printf("Object key prefix: %s", objectKeyPrefix)
	}
	log.Printf("Force copy: %t", *forceFlag)

	awsRegion := os.Getenv("AWS_REGION")

	if awsRegion == "" {
		log.Fatal("AWS_REGION environment variable must be set")
	}

	sess, err := session.NewSession(&aws.Config{
		Region: &awsRegion,
	})
	if err != nil {
		log.Fatal(err)
	}

	s3Client := s3.New(sess)

	versioningInput := &s3.GetBucketVersioningInput{
		Bucket: aws.String(s3Bucket),
	}
	versioningOutput, err := s3Client.GetBucketVersioning(versioningInput)
	if err != nil {
		log.Fatal(err)
	}

	versionEnabled := false

	if versioningOutput.Status != nil {
		versionEnabled = *versioningOutput.Status == "Enabled"
	}

	log.Printf("S3 bucket – Versioning enabled: %t", versionEnabled)

	gcsRetryer := storage.WithBackoff(gax.Backoff{
		// Set the initial retry delay to a maximum of 2 seconds. The length of
		// pauses between retries is subject to random jitter.
		Initial: 2 * time.Second,
		// Set the maximum retry delay to 60 seconds.
		Max: 60 * time.Second,
		// Set the backoff multiplier to 3.0.
		Multiplier: 3,
	})

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var filesCopied int64
	var totalBytesCopied int64
	var copyStartTime time.Time
	var copyMutex sync.Mutex

	wg := sync.WaitGroup{}

	reportStatsFn := func() {
		copyMutex.Lock()
		defer copyMutex.Unlock()
		copyDuration := time.Since(copyStartTime)
		mbPerSec := float64(totalBytesCopied) / copyDuration.Seconds() / (1024 * 1024)
		formattedBytes := formatBytes(totalBytesCopied)
		formattedFiles := printer.Sprintf("%d", filesCopied)
		formattedDuration := formatDuration(copyDuration)
		log.Printf("Copied %s files, total size: %s, time taken: %s, MB/sec: %.2f", formattedFiles, formattedBytes, formattedDuration, mbPerSec)
	}

	ticker := time.NewTicker(5 * time.Second)
	quit := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				reportStatsFn()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	copyFileVersionFn := func(awsKey string, awsVersion string, gcsObject *storage.ObjectHandle) {
		defer wg.Done()

		s3ObjectOutput, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket:    aws.String(s3Bucket),
			Key:       aws.String(awsKey),
			VersionId: aws.String(awsVersion),
		})

		if err != nil {
			log.Fatal("Error getting object " + awsKey + " from bucket " + s3Bucket + ": " + err.Error())
		}

		gcsObjectWriter := gcsObject.NewWriter(ctx)
		defer gcsObjectWriter.Close()

		// write to gcsObjectWriter
		bytesCopied, err := io.Copy(gcsObjectWriter, s3ObjectOutput.Body)
		if err != nil {
			log.Fatal("Error copying object " + awsKey + " from bucket " + s3Bucket + ": " + err.Error())
		}

		gcsObjectWriter.Close()

		copyMutex.Lock()
		totalBytesCopied += bytesCopied
		filesCopied++
		copyMutex.Unlock()

		// Copy metadata from S3 object to GCS object
		gcsObjectAttrs := &storage.ObjectAttrsToUpdate{
			Metadata: make(map[string]string),
		}

		for key, value := range s3ObjectOutput.Metadata {
			gcsObjectAttrs.Metadata[key] = *value
		}

		// add ETag to metadata
		gcsObjectAttrs.Metadata["ETag"] = *s3ObjectOutput.ETag

		_, err = gcsObject.Update(ctx, *gcsObjectAttrs)
		if err != nil {
			log.Fatal("Error updating object " + awsKey + " in bucket " + gcsBucket + ": " + err.Error())
		}
	}

	copyFileFn := func(s3Object *s3.Object, gcsObject *storage.ObjectHandle) {
		s3VersionsOutput, err := s3Client.ListObjectVersions(&s3.ListObjectVersionsInput{
			Bucket: aws.String(s3Bucket),
			Prefix: s3Object.Key,
		})
		if err != nil {
			log.Fatal(err)
		}

		if len(s3VersionsOutput.Versions) == 1 {
			wg.Add(1)
			go copyFileVersionFn(*s3Object.Key, *s3VersionsOutput.Versions[0].VersionId, gcsObject)
		} else {
			log.Printf("%s – %d versions detected", *s3Object.Key, len(s3VersionsOutput.Versions))
			for _, s3Version := range s3VersionsOutput.Versions {
				wg.Add(1)
				copyFileVersionFn(*s3Object.Key, *s3Version.VersionId, gcsObject)
			}
		}
	}

	gcsBucketHandle := client.Bucket(gcsBucket).Retryer(gcsRetryer)

	copyStartTime = time.Now()

	handleS3ObjectsPageFn := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		if err != nil {
			log.Fatal(err)
		}

		for _, s3Object := range page.Contents {
			if *s3Object.Key == "" || (*s3Object.Key)[len(*s3Object.Key)-1:] == "/" {
				continue
			}

			gcsObject := gcsBucketHandle.Object(*s3Object.Key).Retryer(gcsRetryer, storage.WithPolicy(storage.RetryAlways))

			_, err := gcsObject.Attrs(ctx)

			if err != storage.ErrObjectNotExist && *forceFlag {
				if versionEnabled {
					if err := deleteAllVersions(ctx, gcsBucketHandle, *s3Object.Key); err != nil {
						log.Fatal(err)
					}
				} else {
					err := gcsObject.Delete(ctx)
					if err != nil {
						log.Fatal(err)
					}
				}
			}

			if err == storage.ErrObjectNotExist || *forceFlag {
				log.Printf("Object %s – copying", *s3Object.Key)
				copyFileFn(s3Object, gcsObject)
			} else {
				gcsObjectAttrs, err := gcsObject.Attrs(ctx)

				if err != nil {
					log.Fatal(err)
				}

				// get ETag from metadata
				if gcsMetadataEtag, ok := gcsObjectAttrs.Metadata["ETag"]; ok {
					if *s3Object.ETag != gcsObjectAttrs.Metadata["ETag"] {
						log.Fatalf("Mismatch detected:\n  S3 object: %s\n  GCS object %s\n  S3 ETag: %s\n  GCS Metadata ETag: %s\n",
							*s3Object.Key, gcsObjectAttrs.Name, *s3Object.ETag, gcsMetadataEtag)
					} else {
						log.Printf("Object %s match (ETag: %s)", *s3Object.Key, *s3Object.ETag)
					}
				} else {
					log.Printf("GCS Object: %s\n  ETag not found in GCS object metadata – object may be corrupt, forcing copy.", gcsObjectAttrs.Name)
					copyFileFn(s3Object, gcsObject)
				}
			}
		}

		wg.Wait()

		return true
	}

	s3ObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
	}

	if objectKeyPrefix != "" {
		s3ObjectsInput.Prefix = aws.String(objectKeyPrefix)
	}

	if err := s3Client.ListObjectsV2PagesWithContext(ctx, s3ObjectsInput, handleS3ObjectsPageFn); err != nil {
		log.Fatal(err)
	}

	close(quit)

	reportStatsFn()
}
