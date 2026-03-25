#!/usr/bin/env python3
import argparse
import os
import sys
import boto3
from pytubefix import YouTube

def log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()

def download_video(url, out_path):
    """
    Download YouTube video using pytubefix into out_path
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    yt = YouTube(url)
    # pick highest resolution progressive stream (video+audio)
    stream = yt.streams.filter(progressive=True).order_by("resolution").desc().first()
    if not stream:
        raise Exception("No suitable stream found")
    log(f"[INFO] Downloading {url} → {out_path}")
    stream.download(output_path=os.path.dirname(out_path), filename=os.path.basename(out_path))
    log(f"[INFO] Download complete: {out_path}")
    return out_path

def upload_to_s3(file_path, endpoint, access_key, secret_key, bucket_name, object_key):
    """
    Upload a local file to S3 using boto3 resource + put_object
    """
    s3_resource = boto3.resource(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    bucket = s3_resource.Bucket(bucket_name)
    log(f"[INFO] Uploading {file_path} → s3://{bucket_name}/{object_key}")
    with open(file_path, "rb") as f:
        bucket.put_object(
            Key=object_key,
            Body=f,
            ACL='private'  # change to 'public-read' if needed
        )
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
        # download video
        download_video(args.url, out_path)

        # upload to S3
        upload_to_s3(out_path, args.endpoint, args.access_key, args.secret_key, args.bucket, args.filename)

    finally:
        # cleanup
        if os.path.exists(out_path):
            os.remove(out_path)
        if os.path.isdir(args.tmp_dir) and not os.listdir(args.tmp_dir):
            os.rmdir(args.tmp_dir)

if __name__ == "__main__":
    main()
