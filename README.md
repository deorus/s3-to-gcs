# S3-to-GCS

This is a Go program that copies objects from an Amazon S3 bucket to a Google Cloud Storage (GCS) bucket. It can copy an entire bucket or just a subset of files by prefix. The program compares the checksums of the files to decide whether to copy them or not.

## Features

- Copy an entire S3 bucket or a subset of files by prefix
- Compare checksums to decide when to copy
- Force copying objects, skipping checksum comparison
- Copy multiple versions of objects if versioning is enabled
- Report progress and statistics during the copy process

## Usage

```
./s3-to-gcs [-force] <S3 bucket> <GCS bucket> [optional object key prefix]
```

- `-force`: Force copying objects, skipping checksum comparison
- `<S3 bucket>`: The source Amazon S3 bucket
- `<GCS bucket>`: The destination Google Cloud Storage bucket
- `[optional object key prefix]`: An optional prefix to filter objects in the S3 bucket

## Examples

### Copy an entire bucket

```
./s3-to-gcs my-s3-bucket my-gcs-bucket
```

### Copy a subset of files by prefix

```
./s3-to-gcs my-s3-bucket my-gcs-bucket images/
```

### Force copying objects, skipping checksum comparison

```
./s3-to-gcs -force my-s3-bucket my-gcs-bucket
```

## How it works

1. The program lists objects in the S3 bucket, optionally filtered by a prefix.
2. For each object, it checks if the object exists in the GCS bucket.
3. If the object does not exist in the GCS bucket or the `-force` flag is set, the program copies the object.
4. If the object exists in the GCS bucket and the `-force` flag is not set, the program compares the checksums of the S3 and GCS objects.
5. If the checksums do not match, the program reports a mismatch and exits.
6. If the checksums match, the program skips copying the object.
7. The program reports progress and statistics during the copy process.


## Installation

1. Clone the repository:

```
git clone https://github.com/deorus/s3-to-gcs.git
```

2. Change to the project directory:

```
cd s3-to-gcs
```

3. Build the program:

```
go build
```

4. Set the `AWS_REGION` environment variable:

```
export AWS_REGION=us-west-2
```

5. Authenticate with Google Cloud:

```
gcloud auth application-default login
```

## License

This project is licensed under the MIT License.
