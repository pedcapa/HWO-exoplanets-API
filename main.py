import pandas as pd
from fastapi import FastAPI, HTTPException
import numpy as np

app = FastAPI()

try:
  df = pd.read_csv('./src/all_exo.csv', comment='#', sep=',')
except FileNotFoundError:
  raise RuntimeError("The specified CSV file could not be found. Please check the path.")

df = df.replace({np.nan: None})

# show the complete csv
@app.get("/exoplanets/all")
async def get_all_exoplanets():
  return [{"n": df.shape[0]}, df.to_dict(orient='records')]
