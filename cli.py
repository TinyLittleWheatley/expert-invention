#!/usr/bin/env python3
import argparse
import os
import sys
import boto3
from pytubefix import YouTube

def log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()

def download_with_pytubefix(url, out_path):
    """
    Download via pytubefix
    """
    log(f"[INFO] Downloading video: {url}")
    yt = YouTube(url)

    # pick highest resolution progressive stream
    stream = yt.streams.filter(progressive=True).order_by("resolution").desc().first()
    if not stream:
        raise Exception("No suitable stream found")

    # ensure directory is ready
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    stream.download(output_path=os.path.dirname(out_path), filename=os.path.basename(out_path))
    log(f"[INFO] Download complete: {out_path}")
    return out_path

def upload_to_s3(file_path, endpoint, access_key, secret_key, bucket, object_key):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    log(f"[INFO] Uploading {file_path} → s3://{bucket}/{object_key}")
    s3.upload_file(file_path, bucket, object_key)
    log("[DONE] upload complete")

def main():
    parser = argparse.ArgumentParser(description="Download YouTube video with pytubefix and upload to S3")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--filename", required=True, help="Local filename and S3 object key")
    parser.add_argument("--tmp-dir", default="./downloads", help="Temporary download directory")
    args = parser.parse_args()

    out_path = os.path.join(args.tmp_dir, args.filename)

    try:
        download_with_pytubefix(args.url, out_path)
        upload_to_s3(out_path, args.endpoint, args.access_key, args.secret_key, args.bucket, args.filename)
    finally:
        # delete local file
        if os.path.exists(out_path):
            os.remove(out_path)
        # remove empty tmp dir
        if os.path.isdir(args.tmp_dir) and not os.listdir(args.tmp_dir):
            os.rmdir(args.tmp_dir)

if __name__ == "__main__":
    main()
