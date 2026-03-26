#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
from pytubefix import YouTube


def log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def run(cmd):
    log(f"[CMD] {cmd}")
    subprocess.run(cmd, shell=True, check=True)


def download_video(url, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    yt = YouTube(url)
    stream = yt.streams.filter(progressive=True).order_by("resolution").desc().first()

    if not stream:
        raise Exception("No suitable stream found")

    log(f"[INFO] Downloading → {out_path}")
    stream.download(
        output_path=os.path.dirname(out_path),
        filename=os.path.basename(out_path)
    )
    log("[INFO] Download complete")


def upload_with_s3cmd(file_path, bucket, endpoint, access_key, secret_key):
    """
    Uses s3cmd without requiring global config
    """
    cmd = (
        f"s3cmd put {file_path} s3://{bucket}/{os.path.basename(file_path)} "
        f"--host={endpoint.replace('http://', '').replace('https://', '')} "
        f"--host-bucket=%(bucket)s.{endpoint.replace('http://', '').replace('https://', '')} "
        f"--access_key={access_key} "
        f"--secret_key={secret_key} "
        f"--no-ssl"
    )

    log(f"[INFO] Uploading → s3://{bucket}/{os.path.basename(file_path)}")
    run(cmd)
    log("[DONE] Upload complete")


def main():
    parser = argparse.ArgumentParser(description="Download YouTube video and upload to S3 via s3cmd")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--filename", required=True)
    parser.add_argument("--tmp-dir", default="./downloads")

    args = parser.parse_args()

    out_path = os.path.join(args.tmp_dir, args.filename)

    try:
        download_video(args.url, out_path)
        upload_with_s3cmd(
            out_path,
            args.bucket,
            args.endpoint,
            args.access_key,
            args.secret_key
        )
    finally:
        # cleanup
        if os.path.exists(out_path):
            os.remove(out_path)
        if os.path.isdir(args.tmp_dir) and not os.listdir(args.tmp_dir):
            os.rmdir(args.tmp_dir)


if __name__ == "__main__":
    main()
