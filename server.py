import uuid
from typing import Callable, Optional

import pandas as pd

import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException, status
from pandas import DataFrame

app = FastAPI()

data: dict[uuid.UUID, DataFrame] = {}
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB
DESIRED_FORMATS = ['text/csv']
ANALYSIS_CALLBACKS: dict[str, Callable[[DataFrame, str], int]] = {
    'average': lambda df, column: df[column].mean(),
    'maximum': lambda df, column: df[column].max(),
    'minimum': lambda df, column: df[column].min(),
    'standard_deviation': lambda df, column: df[column].std(),
}


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/health/")
async def health():
    return {"status": "healthy"}


@app.post("/upload")
def upload(file: UploadFile = File(...)):
    if file.size > MAX_FILE_SIZE:
        raise ValueError(f"File too large - {file.size} instead of {MAX_FILE_SIZE}")
    if file.content_type not in DESIRED_FORMATS:
        raise ValueError(f"File in wrong format - {file.content_type} not in {DESIRED_FORMATS}")
    try:
        df = pd.read_csv(file.file)
    except:
        raise ValueError('File is in wrong format')
    finally:
        file.file.close()

    unique_id = uuid.uuid1()
    data[unique_id] = df
    return unique_id


@app.post("/analysis")
def analysis(identifier: uuid.UUID, analysis_type: str, column: str, where: Optional[str] = None):
    try:
        df = data[identifier]
        if where:
            df = df.query(where)
        analysis_callback = ANALYSIS_CALLBACKS[analysis_type]
        result = analysis_callback(df, column)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=repr(e),
        )
    else:
        # IDK why, but returning int64 fails while float64 doesn't
        return float(result)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
