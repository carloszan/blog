from fastapi import FastAPI, HTTPException
import httpx
from request import recreate_container_async

app = FastAPI()


@app.post("/execute-requests/")
async def execute_requests():
    try:
        await recreate_container_async()
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while making requests: {str(e)}"
        )
