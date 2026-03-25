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

def get_video_filename(url, out_dir):
    """
    Ask yt-dlp for the final filename using template.
    """
    os.makedirs(out_dir, exist_ok=True)
    cmd = f'yt-dlp --get-filename -o "{out_dir}/%(title)s.%(ext)s" "{url}"'
    result = subprocess.run(shlex.split(cmd), capture_output=True, text=True, check=True)
    filename = result.stdout.strip()
    return filename

def download_youtube(url, out_dir):
    """
    Download YouTube video using yt-dlp into out_dir.
    Returns the absolute path to the downloaded file.
    """
    cmd = f'yt-dlp -o "{out_dir}/%(title)s.%(ext)s" "{url}"'
    log(f"[INFO] Downloading video: {url}")
    subprocess.run(shlex.split(cmd), check=True)

    # Get the filename (yt-dlp will have created this)
    return get_video_filename(url, out_dir)

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
    parser.add_argument("--out-dir", default="./videos", help="Directory to download videos into")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # yt-dlp decides the filename
    file_path = download_youtube(args.url, args.out_dir)
    upload_file_to_s3(file_path, args.endpoint, args.access_key, args.secret_key, args.bucket)

    # cleanup optional
    if os.path.exists(file_path):
        os.remove(file_path)

if __name__ == "__main__":
    main()
