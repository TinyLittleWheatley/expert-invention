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


def sanitize(name):
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    return name.strip()


def clean_url(url):
    return url.split("&")[0]


def create_yt(url):
    token_path = "./tokens.json"

    if os.path.exists(token_path):
        log(f"[INFO] Using OAuth token: {token_path}")
    else:
        log("[INFO] No token found, will trigger OAuth flow")

    return YouTube(
        url,
        use_oauth=True,
        allow_oauth_cache=True,
        token_file=token_path
    )


def pick_stream(yt):
    streams = yt.streams.filter(progressive=True)
    streams = [s for s in streams if s.resolution]

    if not streams:
        raise Exception("No progressive streams found")

    def res_to_int(s):
        return int(s.resolution.replace("p", ""))

    streams.sort(key=res_to_int)

    # log available streams
    log("[INFO] Available streams:")
    for i, s in enumerate(streams):
        log(f"  [{i}] {s.resolution} fps={getattr(s, 'fps', '?')} itag={s.itag}")

    # pick highest
    chosen = streams[-1]

    log(f"[INFO] Selected → {chosen.resolution} (itag={chosen.itag})")
    return chosen


def download_video(url, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    url = clean_url(url)
    yt = create_yt(url)

    title = sanitize(yt.title)
    channel = sanitize(yt.author or "unknown")

    filename = f"{title}.mp4"
    channel_dir = os.path.join(out_dir, channel)
    os.makedirs(channel_dir, exist_ok=True)

    out_path = os.path.join(channel_dir, filename)

    stream = pick_stream(yt)

    log(f"[INFO] Downloading: {channel} / {title}")
    stream.download(output_path=channel_dir, filename=filename)

    return out_path, filename, channel


def upload_with_s3cmd(file_path, bucket, channel_name):
    s3_key = f"{channel_name}/{os.path.basename(file_path)}"

    cmd = [
        "s3cmd",
        "put",
        file_path,
        f"s3://{bucket}/{s3_key}"
    ]

    log(f"[INFO] Uploading → s3://{bucket}/{s3_key}")
    run(cmd)
    log("[DONE] Upload complete")


def main():
    parser = argparse.ArgumentParser(
        description="Download YouTube video (pytubefix + OAuth) and upload to S3"
    )
    parser.add_argument("--bucket", default="buckk")
    parser.add_argument("--url", required=True)
    parser.add_argument("--tmp-dir", default="./downloads")

    args = parser.parse_args()

    try:
        file_path, filename, channel = download_video(args.url, args.tmp_dir)
        upload_with_s3cmd(file_path, args.bucket, channel)

    finally:
        # cleanup file
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)

        # cleanup empty dirs
        if os.path.isdir(args.tmp_dir):
            for root, dirs, files in os.walk(args.tmp_dir, topdown=False):
                if not dirs and not files:
                    os.rmdir(root)


if __name__ == "__main__":
    main()
