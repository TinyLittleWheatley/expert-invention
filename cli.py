#!/usr/bin/env python3
import argparse
import subprocess
import shlex
import boto3
import os
import sys

def log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()

def download_youtube(url, out_path):
    """
    Download YouTube video to the given out_path using yt-dlp.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    cmd = f'yt-dlp -o "{out_path}" "{url}"'
    log(f"[INFO] Downloading video: {url}")
    subprocess.run(shlex.split(cmd), check=True)
    log(f"[INFO] Download complete: {out_path}")
    return out_path

def upload_file_to_s3(file_path, endpoint, access_key, secret_key, bucket, object_key):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    log(f"[INFO] Uploading {file_path} → s3://{bucket}/{object_key}")
    s3.upload_file(file_path, bucket, object_key)
    log("[DONE] Upload complete")

def main():
    parser = argparse.ArgumentParser(description="Download YouTube video and upload to S3")
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
        # download
        download_youtube(args.url, out_path)

        # upload
        upload_file_to_s3(out_path, args.endpoint, args.access_key, args.secret_key, args.bucket, args.filename)

    finally:
        # cleanup
        if os.path.exists(out_path):
            os.remove(out_path)
        if os.path.isdir(args.tmp_dir) and not os.listdir(args.tmp_dir):
            os.rmdir(args.tmp_dir)

if __name__ == "__main__":
    main()
