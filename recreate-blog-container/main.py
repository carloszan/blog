from fastapi import FastAPI, HTTPException
import httpx

create_container_input = {
    "HostConfig": {
        "Binds": [],
        "NetworkMode": "podman",
        "Dns": [],
        "ExtraHosts": [],
        "RestartPolicy": {
            "Name": "no"
        },
        "Privileged": False,
        "Init": False,
        "Runtime": "",
        "Devices": [],
        "Sysctls": {},
        "ShmSize": 67108864,
        "DeviceRequests": [],
        "NanoCpus": 0,
        "MemoryReservation": 0,
        "Memory": 0,
        "CapAdd": [
            "AUDIT_WRITE",
            "CHOWN",
            "DAC_OVERRIDE",
            "FOWNER",
            "FSETID",
            "KILL",
            "MKNOD",
            "NET_BIND_SERVICE",
            "NET_RAW",
            "SETFCAP",
            "SETGID",
            "SETPCAP",
            "SETUID",
            "SYS_CHROOT"
        ],
        "CapDrop": [
            "AUDIT_CONTROL",
            "BLOCK_SUSPEND",
            "DAC_READ_SEARCH",
            "IPC_LOCK",
            "IPC_OWNER",
            "LEASE",
            "LINUX_IMMUTABLE",
            "MAC_ADMIN",
            "MAC_OVERRIDE",
            "NET_ADMIN",
            "NET_BROADCAST",
            "SYSLOG",
            "SYS_ADMIN",
            "SYS_BOOT",
            "SYS_MODULE",
            "SYS_NICE",
            "SYS_PACCT",
            "SYS_PTRACE",
            "SYS_RAWIO",
            "SYS_RESOURCE",
            "SYS_TIME",
            "SYS_TTY_CONFIG",
            "WAKE_ALARM"
        ],
        "PublishAllPorts": False,
        "PortBindings": {
            "80/tcp": [
                {
                    "HostIp": "",
                    "HostPort": "8080"
                }
            ]
        },
        "AutoRemove": False
    },
    "NetworkingConfig": {
        "EndpointsConfig": {
            "podman": {
                "IPAMConfig": {
                    "IPv4Address": "",
                    "IPv6Address": ""
                }
            }
        }
    },
    "User": "",
    "WorkingDir": "",
    "OpenStdin": False,
    "Tty": False,
    "Volumes": {},
    "Hostname": "",
    "Domainname": "",
    "MacAddress": "",
    "Labels": {},
    "ExposedPorts": {
        "80/tcp": {}
    },
    "Env": [],
    "Image": "carloszan/blog:latest"
}

app = FastAPI()


@app.post("/execute-requests/")
async def execute_requests():
    """
    Endpoint that makes 4 HTTP requests in sequence:
    1. DELETE
    2. POST
    3. POST
    4. POST
    """
    results = []
    headers = {'X-API-Key': 'ptr_iOezwi8bqTba/9jsKUFYYeAFLoO+fIyYYyk1bQD3qT8='}

    async with httpx.AsyncClient() as client:
        client.headers = headers
        try:
            # 1. Delete Container
            delete_response = await client.delete("https://portainer.home.sjdr.cloud/api/endpoints/4/docker/containers/blog?v=latest&force=true")
            results.append({
                "request_number": 1,
                "method": "DELETE",
                "status_code": delete_response.status_code,
                "response": delete_response.text
            })

            # 2. Download Image
            download_image_response = await client.post("https://portainer.home.sjdr.cloud/api/endpoints/4/docker/images/create?fromImage=carloszan%2Fblog:latest")
            results.append({
                "request_number": 2,
                "method": "POST",
                "status_code": download_image_response.status_code,
                "response": download_image_response.text
            })

            # 3. Create Container
            create_container_response = await client.post("https://portainer.home.sjdr.cloud/api/endpoints/4/docker/containers/create?name=blog", json=create_container_input)
            results.append({
                "request_number": 3,
                "method": "POST",
                "status_code": create_container_response.status_code,
                "response": create_container_response.text
            })

            # 4. Start Container
            container_id = create_container_response.json()['Id']
            start_container_response = await client.post(f"https://portainer.home.sjdr.cloud/api/endpoints/4/docker/containers/{container_id}/start")
            results.append({
                "request_number": 4,
                "method": "POST",
                "status_code": start_container_response.status_code,
                "response": start_container_response.text
            })

            return {"success": True, "results": results}

        except httpx.RequestError as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred while making requests: {str(e)}"
            )
