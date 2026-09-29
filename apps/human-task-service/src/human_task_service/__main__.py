"""Run the standalone task API from an installed service distribution."""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "human_task_service.main:create_app_from_env", factory=True, host="0.0.0.0", port=8085
    )
