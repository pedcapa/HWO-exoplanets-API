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
  data = [{"n": filtered_df.shape[0]}, filtered_df.sort_values(by='P_ESI', ascending=False).to_dict(orient='records')]
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

  data = [{"n": top_n_df.shape[0]}, top_n_df.to_dict(orient='records')]
  return data

# show all the fields from a specific exoplanet
@app.get("/exoplanets/name/{exoplanet_name}")
async def get_exoplanet_by_name(exoplanet_name: str):
  exoplanet_name_lower = exoplanet_name.lower()
  filtered_df = df[df['pl_name'].str.lower() == exoplanet_name_lower]

  if filtered_df.empty:
    raise HTTPException(status_code=404, detail=f"Exoplanet '{exoplanet_name}' not found.")

  data = filtered_df.to_dict(orient='records')
  return data

# show all the planets that can be seen with a specific diameter
@app.get("/exoplanets/diameter/{d_min_metros}")
async def get_exoplanets_by_diameter(d_min_metros: float):
  if 'D_min_metros' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'D_min_metros' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'D_min_metros']].dropna(subset=['D_min_metros'])
  filtered_df = filtered_df[filtered_df['D_min_metros'] < d_min_metros]

  if filtered_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets found with D_min_metros less than the specified value.")

  sorted_df = filtered_df.sort_values(by='D_min_metros', ascending=False)

  data = [{"n": sorted_df.shape[0]}, sorted_df[['pl_name', 'D_min_metros']].to_dict(orient='records')]
  return data

