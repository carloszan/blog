+++
authors = ["Carlos Zansavio"]
title = "My data platform diary - Week 3"
date = "2026-07-30"
description = "Let's build a data platform together"
tags = [
    "data-engineering",
	"my-data-platform"
]
categories = [
	"data",
]
draft = true
+++

This is the tasks from last week:

- [x] MinIO
- [x] Kafka
- [x] Stream Nginx logs into a Kafka topic
- [x] Read the streaming data locally and process it
- [x] Plan the deploy

# Day 1
Today I need to plan the deploy. I need to get the container I created and I can run inside kubernetes. I think this is the most challenge until now because everything I'm doing will be confirmed. As it's already running locally, I think it will be easier.
claude --resume 96ece12b-3b24-4802-8131-0eac6dd7c580

# Day 2
First, I had to create a new access key in minio. To achieve that, I had to get the admin password, access the minio container and then create a new accesskey.
```bash
debian@minio:~$ sudo podman ps
[sudo] password for debian:
CONTAINER ID  IMAGE                         COMMAND               CREATED         STATUS         PORTS                             NAMES
2b64db06e0a4  docker.io/minio/minio:latest  server /data --co...  14 minutes ago  Up 14 minutes  0.0.0.0:9000-9001->9000-9001/tcp  systemd-minio
root@minio:~# podman exec -it systemd-minio /bin/bash
bash-5.1# mc alias list
local
  URL       : http://localhost:9000
  AccessKey :
  SecretKey :
  API       :
  Path      : auto
  Src       : /tmp/.mc/config.json
```

```bash
root@minio:~# podman inspect systemd-minio | grep -i MINIO_ROOT
                    "MINIO_ROOT_PASSWORD_FILE=secret_key",
                    "MINIO_ROOT_USER_FILE=access_key",
                    "MINIO_ROOT_USER=admin",
                    "MINIO_ROOT_PASSWORD=Fxt5Tuf4WBkXY7Vw/QTt7Wjxj6SrCWQn"
bash-5.1# mc alias set local http://localhost:9000 admin Fxt5Tuf4WBkXY7Vw/QTt7Wjxj6SrCWQn
bash-5.1# mc alias list
local
  URL       : http://localhost:9000
  AccessKey : admin
  SecretKey : Fxt5Tuf4WBkXY7Vw/QTt7Wjxj6SrCWQn
  API       : s3v4
  Path      : auto
  Src       : /tmp/.mc/config.json
```

```bash
bash-5.1# mc admin accesskey create local
Access Key: 0VZ*******END3TV
Secret Key: b****+T5C***
Expiration: NONE
Name:
Description:
```

Now creating a new user for spark-jobs:
```bash
root@minio:/etc# openssl rand -base64 24
+HjbHpu3qUg2ywNKmq1RSdhTtiXmyqm+
bash-5.1#   cat > spark-jobs-policy.json <<'EOF'
  {
    "Version": "2012-10-17",
    "Statement": [
      { "Effect": "Allow", "Action": ["s3:*"], "Resource":
  [
          "arn:aws:s3:::lakehouse",
  "arn:aws:s3:::lakehouse/*",
          "arn:aws:s3:::spark-logs",
  "arn:aws:s3:::spark-logs/*"
      ]}
    ]
  }
> EOF
bash-5.1# cat spark-jobs-policy.json
  {
    "Version": "2012-10-17",
    "Statement": [
      { "Effect": "Allow", "Action": ["s3:*"], "Resource":
  [
          "arn:aws:s3:::lakehouse",
  "arn:aws:s3:::lakehouse/*",
          "arn:aws:s3:::spark-logs",
  "arn:aws:s3:::spark-logs/*"
      ]}
    ]
  }
bash-5.1# mc admin policy create local spark-jobs-policy spark-jobs-policy.json
Created policy `spark-jobs-policy` successfully.
bash-5.1# mc admin user add local spark-jobs +HjbHpu3qUg2ywNKmq1RSdhTtiXmyqm+
Added user `spark-jobs` successfully.
bash-5.1# mc admin policy attach local spark-jobs-policy --user spark-jobs
Attached Policies: [spark-jobs-policy]
To User: spark-jobs
```

```bash
root@k3s-cp-1:~#   kubectl create secret generic minio-spark-jobs-creds     --namespace spark-jobs     --from-literal=AWS_ACCESS_KEY_ID=spark-jobs     --from-literal=AWS_SECRET_ACCESS_KEY='+HjbHpu3qUg2ywNKmq1RSdhTtiXmyqm+'
secret/minio-spark-jobs-creds created
```

I also fixed the CI. Now we have a smoke test on the container and registering the image on Github Container Registry.
# Day 4
We have a package uploaded to our data lake. I want to test the image locally:
```bash
docker run --rm -it \
   -e APP_NAME=nginx-stream \
   -e APP_VERSION=0.1.0 \
   -e APP_ENTRYPOINT=nginx_stream.main \
   -e ARTIFACT_BUCKET_URI=s3a://artifacts \
   -e SPARK_SDK_VERSION=0.1.0 \
   -e LAKEHOUSE_BUCKET=lakehouse \
   -e KAFKA_BROKERS=kafka.home.sjdr.cloud:9092 \
   -e AWS_ACCESS_KEY_ID="spark-jobs" \
   -e AWS_SECRET_ACCESS_KEY="+HjbHpu3qUg2ywNKmq1RSdhTtiXmyqm+" \
   ghcr.io/carloszan/spark-image:3.5.4-3.2.1 \
   /opt/spark/bin/spark-submit --master "local[2]" /opt/spark/bin/bootstrap.py
```

I needed to register my minio key again. **TODO:** I need to assign this key in the container creation. 
```bash
cat > spark-jobs-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:*"], "Resource": [
        "arn:aws:s3:::lakehouse", "arn:aws:s3:::lakehouse/*",
        "arn:aws:s3:::spark-logs", "arn:aws:s3:::spark-logs/*"
    ]},
    { "Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": [
        "arn:aws:s3:::artifacts", "arn:aws:s3:::artifacts/*"
    ]}
  ]
}
```
So, I had to recreate the spark-jobs user as the container was rebooted. I need to let this on the Minio's startup configuration. But it's working 😀.
```
26/09/07 16:34:55 INFO MicroBatchExecution: Streaming query has been idle and waiting for new data more than 10000 ms.
```

# Day 5
After some AI interactions, I finally made it work!!:
```python
df.select("raw_log").show(truncate=False)


+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+ |raw_log | +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+ |{"time":1790206604.0,"client_ip":"179.108.220.26","remote_addr":"172.69.39.94","request_method":"GET","request_uri":"/videos/4463ea4a-b79d-357a-409f-8fffd12d9eb3/hls1/main/715.mp4?DeviceId=TW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzE0Ni4wLjAuMCBTYWZhcmkvNTM3LjM2fDE3NzM2MTczODY2NjY1&MediaSourceId=4463ea4ab79d357a409f8fffd12d9eb3&VideoCodec=av1,hevc,h264,vp9&AudioCodec=aac&AudioStreamIndex=11&VideoBitrate=139616000&AudioBitrate=384000&MaxFramerate=23.976&SegmentContainer=mp4&MinSegments=1&BreakOnNonKeyFrames=False&PlaySessionId=e7eaf4ea2b2745eab9c0071dfa4eda99&ApiKey=d2658ad57d7c4521aba64e062fd8b1bc&TranscodingMaxAudioChannels=2&RequireAvc=false&EnableAudioVbrEncoding=true&Tag=6e2e7595dbf26e37e4b300cae75c8f19&SubtitleMethod=Encode&mpeg4-level=5&mpeg4-videobitdepth=8&mpeg4-profile=advancedsimpleprofile&av1-profile=main&av1-rangetype=SDR,HDR10,HDR10Plus,HLG&av1-level=19&vp9-rangetype=SDR,HDR10,HDR10Plus,HLG&hevc-profile=main,main10&hevc-rangetype=SDR,HDR10,HDR10Plus,HLG&hevc-level=186&hevc-deinterlace=true&h264-profile=high,main,baseline,constrainedbaseline,high10&h264-rangetype=SDR&h264-level=52&h264-deinterlace=true&TranscodeReasons=ContainerNotSupported,VideoCodecNotSupported,AudioCodecNotSupported&runtimeTicks=21478600000&actualSegmentLengthTicks=30040000","status":200,"body_bytes_sent":348453,"request_time":0.055,"upstream_response_time":"0.043","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790208319.0,"client_ip":"179.108.220.26","remote_addr":"172.69.11.139","request_method":"POST","request_uri":"/Sessions/Playing/Progress","status":204,"body_bytes_sent":0,"request_time":0.011,"upstream_response_time":"0.011","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790208320.0,"client_ip":"179.108.220.26","remote_addr":"172.71.234.39","request_method":"GET","request_uri":"/videos/4463ea4a-b79d-357a-409f-8fffd12d9eb3/hls1/main/1287.mp4?DeviceId=TW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzE0Ni4wLjAuMCBTYWZhcmkvNTM3LjM2fDE3NzM2MTczODY2NjY1&MediaSourceId=4463ea4ab79d357a409f8fffd12d9eb3&VideoCodec=av1,hevc,h264,vp9&AudioCodec=aac&AudioStreamIndex=11&VideoBitrate=139616000&AudioBitrate=384000&MaxFramerate=23.976&SegmentContainer=mp4&MinSegments=1&BreakOnNonKeyFrames=False&PlaySessionId=e7eaf4ea2b2745eab9c0071dfa4eda99&ApiKey=d2658ad57d7c4521aba64e062fd8b1bc&TranscodingMaxAudioChannels=2&RequireAvc=false&EnableAudioVbrEncoding=true&Tag=6e2e7595dbf26e37e4b300cae75c8f19&SubtitleMethod=Encode&mpeg4-level=5&mpeg4-videobitdepth=8&mpeg4-profile=advancedsimpleprofile&av1-profile=main&av1-rangetype=SDR,HDR10,HDR10Plus,HLG&av1-level=19&vp9-rangetype=SDR,HDR10,HDR10Plus,HLG&hevc-profile=main,main10&hevc-rangetype=SDR,HDR10,HDR10Plus,HLG&hevc-level=186&hevc-deinterlace=true&h264-profile=high,main,baseline,constrainedbaseline,high10&h264-rangetype=SDR&h264-level=52&h264-deinterlace=true&TranscodeReasons=ContainerNotSupported,VideoCodecNotSupported,AudioCodecNotSupported&runtimeTicks=38661480000&actualSegmentLengthTicks=30040000","status":200,"body_bytes_sent":320834,"request_time":0.06,"upstream_response_time":"0.049","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"}| |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"172.71.10.92","request_method":"GET","request_uri":"/UserViews?userId=8c2e3cbb1a7c49cab53346a85356e62c","status":200,"body_bytes_sent":845,"request_time":2.385,"upstream_response_time":"2.386","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"172.71.238.107","request_method":"GET","request_uri":"/web/84158.b965d0fbed7b231d8996.css","status":200,"body_bytes_sent":614,"request_time":0.005,"upstream_response_time":"0.006","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"172.69.11.39","request_method":"GET","request_uri":"/web/49275.667e823ce6c9b896cd5b.css","status":200,"body_bytes_sent":243,"request_time":0.003,"upstream_response_time":"0.003","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"172.69.39.66","request_method":"GET","request_uri":"/web/35308.0b1567d9abe0bdae4331.chunk.js","status":200,"body_bytes_sent":3283,"request_time":0.005,"upstream_response_time":"0.005","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"104.23.254.62","request_method":"GET","request_uri":"/web/84158.4141819ca8b5f6555c12.chunk.js","status":200,"body_bytes_sent":1229,"request_time":0.003,"upstream_response_time":"0.003","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"172.69.39.60","request_method":"GET","request_uri":"/web/49275.fcf2b77489e84c80e220.chunk.js","status":200,"body_bytes_sent":1887,"request_time":0.003,"upstream_response_time":"0.004","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"104.23.254.100","request_method":"GET","request_uri":"/web/35308.9bf57cfbd84387e3793e.css","status":200,"body_bytes_sent":415,"request_time":0.003,"upstream_response_time":"0.004","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"172.71.6.198","request_method":"GET","request_uri":"/web/MaterialIcons-Regular.2d8017489da689caedc1.woff2","status":200,"body_bytes_sent":125116,"request_time":0.014,"upstream_response_time":"0.015","http_referer":"[https://jellyfin.sjdr.cloud/web/1133.03edf3bb7ee048ee10be.css?d65b8f2985eba3cc3a41](https://jellyfin.sjdr.cloud/web/1133.03edf3bb7ee048ee10be.css?d65b8f2985eba3cc3a41)","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288519.0,"client_ip":"179.108.220.26","remote_addr":"172.71.10.92","request_method":"GET","request_uri":"/LiveTv/Programs/Recommended?userId=8c2e3cbb1a7c49cab53346a85356e62c&limit=1&isAiring=true&imageTypeLimit=1&enableImageTypes=Primary&enableImageTypes=Thumb&enableImageTypes=Backdrop&fields=ChannelInfo&fields=PrimaryImageAspectRatio&enableTotalRecordCount=false","status":200,"body_bytes_sent":52,"request_time":0.645,"upstream_response_time":"0.645","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"172.71.10.92","request_method":"GET","request_uri":"/UserItems/Resume?userId=8c2e3cbb1a7c49cab53346a85356e62c&limit=12&fields=PrimaryImageAspectRatio&mediaTypes=Audio&imageTypeLimit=1&enableImageTypes=Primary&enableImageTypes=Backdrop&enableImageTypes=Thumb&enableTotalRecordCount=false","status":200,"body_bytes_sent":52,"request_time":1.008,"upstream_response_time":"1.008","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"104.22.10.214","request_method":"GET","request_uri":"/web/14553.6ddb0da23f2698557d33.chunk.js","status":200,"body_bytes_sent":9418,"request_time":0.117,"upstream_response_time":"0.117","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"172.71.238.117","request_method":"GET","request_uri":"/web/node_modules.react-lazy-load-image-component.4c0850deb4bb234feecf.chunk.js","status":200,"body_bytes_sent":8615,"request_time":0.07199999999999999,"upstream_response_time":"0.072","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"172.71.10.92","request_method":"GET","request_uri":"/Shows/NextUp?userId=8c2e3cbb1a7c49cab53346a85356e62c&limit=24&fields=PrimaryImageAspectRatio&fields=DateCreated&fields=Path&fields=MediaSourceCount&imageTypeLimit=1&enableImageTypes=Primary&enableImageTypes=Backdrop&enableImageTypes=Thumb&nextUpDateCutoff=2025-09-24&enableTotalRecordCount=false&enableResumable=false&enableRewatching=false","status":200,"body_bytes_sent":52,"request_time":1.032,"upstream_response_time":"1.031","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"104.22.10.18","request_method":"GET","request_uri":"/web/homevideos.6cfdd3e92a49c0999e0d.chunk.js","status":200,"body_bytes_sent":207,"request_time":0.081,"upstream_response_time":"0.080","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"172.71.234.202","request_method":"GET","request_uri":"/web/homevideos.df2066698fd3e2b4647d.css","status":200,"body_bytes_sent":4023,"request_time":0.061,"upstream_response_time":"0.062","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"172.69.138.208","request_method":"GET","request_uri":"/web/85401.7a16f94bd4b18fe0ff55.chunk.js","status":200,"body_bytes_sent":5425,"request_time":0.08599999999999999,"upstream_response_time":"0.086","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | |{"time":1790288520.0,"client_ip":"179.108.220.26","remote_addr":"104.22.10.154","request_method":"GET","request_uri":"/web/52197.ae3dbcde199a3bdd936b.chunk.js","status":200,"body_bytes_sent":41117,"request_time":0.089,"upstream_response_time":"0.089","http_referer":"","http_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","host":"jellyfin.sjdr.cloud"} | +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+ only showing top 20 rows
```
claude --resume 43c60b22-3f40-46e8-8490-3fe8187cdfd8


claude --resume 261c3942-ec7b-4d45-b936-f9f6fc750b39