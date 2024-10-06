import pandas as pd
from fastapi import FastAPI, HTTPException
import numpy as np

app = FastAPI()

try:
  df = pd.read_csv('./src/all_exo.csv', comment='#', sep=',')
except FileNotFoundError:
  raise RuntimeError("The specified CSV file could not be found. Please check the path.")

df = df.replace({np.nan: None})

# show all the exoplanets with all the fields
@app.get("/exoplanets/all")
async def get_all_exoplanets():
  return [{"n": df.shape[0]}, df.to_dict(orient='records')]

# show all the exoplanets sorted by ESI -> descending order
@app.get("/exoplanets/esi")
async def get_exoplanets_esi():
  filtered_df = df[['pl_name', 'P_ESI']].dropna(subset=['P_ESI'])
  if filtered_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with a valid 'P_ESI' value were found.")
  data = [{"n": len(filtered_df)}, filtered_df.sort_values(by='P_ESI', ascending=False).to_dict(orient='records')]
  return data

# show n exoplanets sorted by ESI -> descending order
@app.get("/exoplanets/esi/{n_esi}")
async def get_n_exoplanets_esi(n: int):
  if n <= 0:
    raise HTTPException(status_code=400, details="The number of exoplanets (n) must be greater than zero.")
  
  filtered_df = df[['pl_name', 'P_ESI']].dropna(subset=['P_ESI']).sort_values(by='P_ESI', ascending=False)

  top_n_df = filtered_df.head(n)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid P_ESI values found.")

  data = [{"n": len(top_n_df)}, top_n_df.to_dict(orient='records')]
  return data

