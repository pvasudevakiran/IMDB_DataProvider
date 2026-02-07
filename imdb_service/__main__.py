"""Run the API server. Usage: python -m imdb_service"""

import uvicorn

from imdb_service.config import get_settings

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "imdb_service.api:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
