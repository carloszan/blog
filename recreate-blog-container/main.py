import os
from typing import Annotated
from fastapi import FastAPI, HTTPException, Header
import httpx
from request import recreate_container_async

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = FastAPI()


@app.post("/execute-requests/")
async def execute_requests(api_key: Annotated[str | None, Header()] = None):
    actual_api_key = os.getenv('GITHUB_ACTION_KEY')
    if not actual_api_key:
        actual_api_key = 'test'

    if api_key != actual_api_key:
        raise HTTPException(
            status_code=404
        )

    try:
        await recreate_container_async()
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while making requests: {str(e)}"
        )
