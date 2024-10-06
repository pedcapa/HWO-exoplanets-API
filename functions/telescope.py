# functions/telescope.py
import math

def minimum_diameter(
  starRadius: float, 
  planetRadius: float, 
  ES: float, 
  PS: float, 
  SNR0: float = 100, 
  SNR_min: float = 5, 
  ref_D: float = 6, 
  ref_ESmax: float = 15, 
  ref_distance: float = 10
) -> float:
  """
  starRadius: float -> exoplanet star radius (measure in sun radius) -> R_star
  
  planetRadius: float -> exoplanet radius (measure in earth radius) -> R_planet
  
  ES: float -> system distance -> from earth to exoplanet star system (measure in parsecs)
  
  PS: float -> planet star distance -> from exoplanet to exoplanet star (measure in AU)
  
  SNR0: float -> reference SNR
  
  SNR_min: float, optional -> minimum SNR
  
  ref_D: float, optional -> reference diameter (measure in meters)
  
  ref_ESmax: float, optional -> reference esmax constant (measure in parsecs)
  
  ref_distance: float, optional -> reference distance (measure in parsecs)

  return -> D_min: float
  """

  if starRadius <= 0 or planetRadius <= 0 or ES <= 0 or PS <= 0:
    return None
  
  numerator_D1 = (ES / ref_distance) * PS * math.sqrt(SNR_min)
  denominator_D1 = (starRadius * planetRadius * math.sqrt(SNR0))
  if denominator_D1 == 0:
    return None
  D1 = (numerator_D1 / denominator_D1) * ref_D
  D2: float = (ref_D * ES * PS) / ref_ESmax

  D_min: float = max(D1, D2)
  return D_min
