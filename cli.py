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

def download_youtube(url, out_dir):
    """
    Download YouTube video using yt-dlp into out_dir.
    Returns the absolute path to the downloaded file.
    """
    os.makedirs(out_dir, exist_ok=True)
    # Save file as "<title>.ext"
    cmd = f'yt-dlp -o "{out_dir}/%(title)s.%(ext)s" "{url}"'
    log(f"[INFO] Running: {cmd}")
    subprocess.run(shlex.split(cmd), check=True)
    
    # find the file in out_dir (assume only 1 new file)
    files = os.listdir(out_dir)
    if not files:
        raise FileNotFoundError("yt-dlp did not produce any file")
    return os.path.join(out_dir, files[0])

def upload_file_to_s3(file_path, endpoint, access_key, secret_key, bucket):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    key = os.path.basename(file_path)
    log(f"[INFO] Uploading {file_path} → s3://{bucket}/{key}")
    s3.upload_file(file_path, bucket, key)
    log("[DONE] Upload complete")

def main():
    parser = argparse.ArgumentParser(description="Download YouTube video and upload to S3")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--tmp-dir", default="downloads", help="Temporary download directory")
    args = parser.parse_args()

    try:
        file_path = download_youtube(args.url, args.tmp_dir)
        upload_file_to_s3(file_path, args.endpoint, args.access_key, args.secret_key, args.bucket)
    finally:
        # Optional cleanup
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.isdir(args.tmp_dir) and not os.listdir(args.tmp_dir):
            os.rmdir(args.tmp_dir)

if __name__ == "__main__":
    main()
