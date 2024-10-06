import pandas as pd
from fastapi import FastAPI, HTTPException
import numpy as np
import httpx

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

# show all the exoplanets that can be seen with a specific diameter
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

# show N exoplanets that can be seen with a specific diameter
@app.get("/exoplanets/diameter/{d_min_metros}/{n}")
async def get_n_exoplanets_by_diameter(d_min_metros: float, n: int):
  if n <= 0:
    return None
  
  if 'D_min_metros' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'D_min_metros' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'D_min_metros']].dropna(subset=['D_min_metros'])
  filtered_df = filtered_df[filtered_df['D_min_metros'] < d_min_metros]

  if filtered_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets found with D_min_metros less than the specified value.")

  sorted_df = filtered_df.sort_values(by='D_min_metros', ascending=False)

  data = [{"n": sorted_df.shape[0]}, sorted_df[['pl_name', 'D_min_metros']].head(n).to_dict(orient='records')]
  return data

# show all the exoplanets that are potentially habitable
@app.get("/exoplanets/habitability")
async def get_exoplanets_by_habitability():
  if 'P_HABITABLE' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'P_HABITABLE' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'P_HABITABLE']].dropna(subset=['P_HABITABLE'])
  filtered_df = filtered_df[filtered_df['P_HABITABLE'] > 0]

  if filtered_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets found with P_HABITABLE more than 0.")

  sorted_df = filtered_df.sort_values(by='P_HABITABLE', ascending=False)

  data = [{"n": sorted_df.shape[0]}, sorted_df[['pl_name', 'P_HABITABLE']].to_dict(orient='records')]
  return data

# show N exoplanets that are potentially habitable
@app.get("/exoplanets/habitability/{n_hab}")
async def get_n_exoplanets_by_habitability(n_hab: int):
  if n_hab <= 0:
    raise HTTPException(status_code=400, detail="The number of exoplanets (n) must be greater than zero.")
  
  if 'P_HABITABLE' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'P_HABITABLE")
  
  filtered_df = df[['pl_name', 'P_HABITABLE']].dropna(subset=['P_HABITABLE']).sort_values(by='P_HABITABLE', ascending=False)

  top_n_df = filtered_df.head(n_hab)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid P_HABITABLE values found.")

  data = [{"n": top_n_df.shape[0]}, top_n_df.to_dict(orient='records')]
  return data

# show all the exoplanets sorted by radius (Earth radius coeficient) -> ascending order
@app.get("/exoplanets/radius")
async def get_exoplanets_by_radius():
  if 'pl_rade' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'pl_rade' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'pl_rade']].dropna(subset=['pl_rade'])
  filtered_df['pl_density'] = abs(filtered_df['pl_rade'] - 1)

  sorted_df = filtered_df.sort_values(by='pl_density')

  top_n_df = sorted_df.sort_values(by='pl_density', ascending=True)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid pl_rade values found.")

  data = [{"n": top_n_df.shape[0]}, top_n_df[['pl_name', 'pl_rade', 'pl_density']].to_dict(orient='records')]
  return data

# show N exoplanets sorted by its radius -> ascending order
@app.get("/exoplanets/radius/{n_rad}")
async def get_top_exoplanets_by_radius(n_rad: int):
  if n_rad <= 0:
    raise HTTPException(status_code=400, detail="The number of exoplanets (n) must be greater than zero.")

  if 'pl_rade' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'pl_rade' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'pl_rade']].dropna(subset=['pl_rade'])
  filtered_df['pl_density'] = abs(filtered_df['pl_rade'] - 1)

  sorted_df = filtered_df.sort_values(by='pl_density')

  top_n_df = sorted_df.head(n_rad).sort_values(by='pl_density', ascending=True)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid pl_rade values found.")

  data = [{"n": top_n_df.shape[0]}, top_n_df[['pl_name', 'pl_rade', 'pl_density']].to_dict(orient='records')]
  return data

# show all the exoplanets sorted by the distance from its system to earth in parsecs
@app.get("/exoplanets/distance")
async def get_exoplanets_by_distance():
  filtered_df = df[['pl_name', 'S_DISTANCE']].dropna(subset=['S_DISTANCE'])
  if filtered_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with a valid 'S_DISTANCE' value were found.")
  data = [{"n": filtered_df.shape[0]}, filtered_df.sort_values(by='S_DISTANCE').to_dict(orient='records')]
  return data

# show N exoplanets sorted by the distance from its system to earth in parsecs
@app.get("/exoplanets/distance/{n_distance}")
async def get_top_exoplanets_by_distance(n_distance: int):
  if n_distance <= 0:
    raise HTTPException(status_code=400, detail="The number of exoplanets (n) must be greater than zero.")

  if 'S_DISTANCE' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'S_DISTANCE' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'S_DISTANCE']].dropna(subset=['S_DISTANCE']).sort_values(by='S_DISTANCE', ascending=True)

  top_n_df = filtered_df.head(n_distance)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid S_DISTANCE values found.")

  data = [{"n": top_n_df.shape[0]}, top_n_df.to_dict(orient='records')]
  return data

# show all the exoplanets sorted by its mass (Earth mass coeficient) -> ascending order
@app.get("/exoplanets/masse")
async def get_exoplanets_by_masse():
  if 'pl_bmasse' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'pl_bmasse' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'pl_bmasse']].dropna(subset=['pl_bmasse'])
  filtered_df['pl_density'] = abs(filtered_df['pl_bmasse'] - 1)

  top_n_df = filtered_df.sort_values(by='pl_density', ascending=True)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid pl_bmasse values found.")

  data = [{"n": top_n_df.shape[0]}, top_n_df[['pl_name', 'pl_bmasse', 'pl_density']].to_dict(orient='records')]
  return data

# show N exoplanets sorted by its mass
@app.get("/exoplanets/masse/{n_masse}")
async def get_top_exoplanets_by_bmasse(n_masse: int):
  if n_masse <= 0:
    raise HTTPException(status_code=400, detail="The number of exoplanets (n) must be greater than zero.")

  if 'pl_bmasse' not in df.columns:
    raise HTTPException(status_code=400, detail="Column 'pl_bmasse' does not exist in the dataset.")

  filtered_df = df[['pl_name', 'pl_bmasse']].dropna(subset=['pl_bmasse'])
  filtered_df['pl_density'] = abs(filtered_df['pl_bmasse'] - 1)

  sorted_df = filtered_df.sort_values(by='pl_density')

  top_n_df = sorted_df.head(n_masse).sort_values(by='pl_density', ascending=True)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid pl_bmasse values found.")

  data = [{"n": top_n_df.shape[0]}, top_n_df[['pl_name', 'pl_bmasse', 'pl_density']].to_dict(orient='records')]
  return data

# show all the exoplanets sorted by how size similar are compare with the Earth
@app.get("/exoplanets/size")
async def get_exoplanets_by_size():
  if 'pl_rade' not in df.columns or 'pl_bmasse' not in df.columns:
    raise HTTPException(status_code=400, detail="Columns 'pl_rade' or 'pl_bmasse' do not exist in the dataset.")

  filtered_df = df[['pl_name', 'pl_rade', 'pl_bmasse']].dropna(subset=['pl_rade', 'pl_bmasse'])

  filtered_df['size_avg'] = (filtered_df['pl_rade'] + filtered_df['pl_bmasse']) / 2 

  filtered_df['pl_density'] = abs((filtered_df['pl_rade'] + filtered_df['pl_bmasse']) / 2 - 1)

  sorted_df = filtered_df.sort_values(by='pl_density', ascending=True)

  if sorted_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid 'pl_rade' and 'pl_bmasse' values found.")

  data = [{"n": sorted_df.shape[0]}, sorted_df[['pl_name', 'pl_rade', 'pl_bmasse', 'size_avg', 'pl_density']].to_dict(orient='records')]
  return data

# show N exoplanets sorted by its size (Earth coeficient)
@app.get("/exoplanets/size/{n_size}")
async def get_top_exoplanets_by_size(n_size: int):
  if n_size <= 0:
    raise HTTPException(status_code=400, detail="The number of exoplanets (n) must be greater than zero.")

  if 'pl_rade' not in df.columns or 'pl_bmasse' not in df.columns:
    raise HTTPException(status_code=400, detail="Columns 'pl_rade' or 'pl_bmasse' do not exist in the dataset.")

  filtered_df = df[['pl_name', 'pl_rade', 'pl_bmasse']].dropna(subset=['pl_rade', 'pl_bmasse'])

  filtered_df['size_avg'] = (filtered_df['pl_rade'] + filtered_df['pl_bmasse']) / 2

  filtered_df['pl_density'] = abs((filtered_df['pl_rade'] + filtered_df['pl_bmasse']) / 2 - 1)

  filtered_df['distance_from_1'] = abs(filtered_df['size_avg'] - 1)
  sorted_df = filtered_df.sort_values(by=['distance_from_1', 'size_avg'], ascending=[True, False])

  top_n_df = sorted_df.head(n_size)

  if top_n_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets with valid 'pl_rade' and 'pl_bmasse' values found.")

  data = top_n_df[['pl_name', 'pl_rade', 'pl_bmasse', 'pl_density', 'size_avg']].to_dict(orient='records')
  return data

# show all the intersections between exoplanets based on HWO diameter and how habitable the planet is
@app.get("/exoplanets/diameter_and_habitable/{d_min_metros}")
async def get_intersection_of_diameter_and_habitability(d_min_metros: float):
  diameter_data = await get_exoplanets_by_diameter(d_min_metros)
  habitability_data = await get_exoplanets_by_habitability()

  diameter_planets = pd.DataFrame(diameter_data[1])
  habitability_planets = pd.DataFrame(habitability_data[1])

  intersected_df = pd.merge(diameter_planets, habitability_planets, on='pl_name')

  if intersected_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets found that meet both diameter and habitability conditions.")

  data = [{"n": len(intersected_df)}, intersected_df.to_dict(orient='records')]
  return data

# sorted exoplanets with a diameter mininum less than d_min based on db_field
@app.get("/exoplanets/diameter/{d_min}/sort_by/{db_field}")
async def get_exoplanets_sorted_by_field(d_min: float, db_field: str):
  endpoint_map = {
    "esi": "P_ESI",
    "masse": "pl_bmasse",
    "radius": "pl_rade",
    "habitability": "P_HABITABLE",
    "distance": "S_DISTANCE",
    "size": "size_avg"
  }

  if db_field not in endpoint_map:
    raise HTTPException(status_code=400, detail=f"The field '{db_field}' is not supported.")

  async with httpx.AsyncClient() as client:
    diameter_response = await client.get(f"http://127.0.0.1:8000/exoplanets/diameter/{d_min}")
    if diameter_response.status_code != 200:
      raise HTTPException(status_code=diameter_response.status_code, detail="Error fetching diameter data.")

    field = endpoint_map[db_field]
    other_response = await client.get(f"http://127.0.0.1:8000/exoplanets/{db_field}")
    if other_response.status_code != 200:
      raise HTTPException(status_code=other_response.status_code, detail=f"Error fetching {db_field} data.")

  diameter_data = diameter_response.json()
  other_data = other_response.json()

  diameter_planets = pd.DataFrame(diameter_data[1])
  other_planets = pd.DataFrame(other_data[1])

  intersected_df = pd.merge(diameter_planets, other_planets, on='pl_name')

  if intersected_df.empty:
    raise HTTPException(status_code=404, detail="No exoplanets found that meet both diameter and other condition.")

  if field in intersected_df.columns:
    intersected_df = intersected_df.sort_values(by=field, ascending=False)
  else:
    raise HTTPException(status_code=400, detail=f"Field '{db_field}' is not present in the intersected dataset.")

  data = [{"n": intersected_df.shape[0]}, intersected_df.to_dict(orient='records')]
  return data