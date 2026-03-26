#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
import re
from pytubefix import YouTube


def log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def run(cmd_list):
    log(f"[CMD] {' '.join(cmd_list)}")
    subprocess.run(cmd_list, check=True)



def sanitize_filename(name):
    """
    Remove problematic characters for filesystems
    """
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = name.strip()
    return name


def download_video(url, out_dir):
    yt = YouTube(url)
    title = sanitize_filename(yt.title)

    filename = f"{title}.mp4"
    out_path = os.path.join(out_dir, filename)

    os.makedirs(out_dir, exist_ok=True)

    stream = yt.streams.filter(progressive=True).order_by("resolution").desc().first()
    if not stream:
        raise Exception("No suitable stream found")

    log(f"[INFO] Downloading: {title}")
    stream.download(output_path=out_dir, filename=filename)

    return out_path, filename



def upload_with_s3cmd(file_path, bucket):
    cmd = [
        "s3cmd",
        "put",
        file_path,
        f"s3://{bucket}/{os.path.basename(file_path)}"
    ]

    log(f"[INFO] Uploading → s3://{bucket}/{os.path.basename(file_path)}")
    run(cmd)
    log("[DONE] Upload complete")


def main():
    parser = argparse.ArgumentParser(description="Download YouTube video and upload to S3 (via s3cmd)")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--tmp-dir", default="./downloads")

    args = parser.parse_args()

    try:
        file_path, filename = download_video(args.url, args.tmp_dir)
        upload_with_s3cmd(file_path, args.bucket)

    finally:
        # cleanup
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        if os.path.isdir(args.tmp_dir) and not os.listdir(args.tmp_dir):
            os.rmdir(args.tmp_dir)


if __name__ == "__main__":
    main()
