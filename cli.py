#!/usr/bin/env python3

import argparse
import subprocess
import shlex
import boto3
import sys

CHUNK_SIZE = 5 * 1024 * 1024  # 5MB


def log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def upload_stream_to_s3(stream, s3, bucket, key):
    mp = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mp["UploadId"]

    parts = []
    part_number = 1
    buffer = b""
    total_uploaded = 0

    try:
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                break

            buffer += chunk

            if len(buffer) >= CHUNK_SIZE:
                resp = s3.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer,
                )

                parts.append({
                    "PartNumber": part_number,
                    "ETag": resp["ETag"]
                })

                total_uploaded += len(buffer)

                log(f"[UPLOAD] part={part_number} bytes={len(buffer)} total={total_uploaded}")

                part_number += 1
                buffer = b""

        # final part
        if buffer:
            resp = s3.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer,
            )

            parts.append({
                "PartNumber": part_number,
                "ETag": resp["ETag"]
            })

            total_uploaded += len(buffer)

        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        log("[DONE] upload complete")

    except Exception as e:
        log(f"[ERROR] {e}")
        s3.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
        )
        raise


def main():
    parser = argparse.ArgumentParser(description="YouTube → S3 uploader (stateless)")

    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--url", required=True)

    args = parser.parse_args()

    # init S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
    )

    # yt-dlp command (stream to stdout)
    cmd = f'yt-dlp -o - "{args.url}"'

    log("[INFO] starting yt-dlp...")

    process = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0
    )

    try:
        upload_stream_to_s3(process.stdout, s3, args.bucket, args.key)

        process.wait()

        if process.returncode != 0:
            err = process.stderr.read().decode()
            log(f"[ERROR] yt-dlp failed:\n{err}")

    except KeyboardInterrupt:
        log("[ABORT] interrupted")
        process.kill()


if __name__ == "__main__":
    main()
