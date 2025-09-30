import xarray as xr
import cfgrib
import matplotlib.pyplot as plt
import os

grib_file = 't2m.grib'
zarr_dir = 't2m.zarr'

lat_point = -15.5
lon_point = -55.2

# --- Abrir GRIB ---
ds_grib_list = cfgrib.open_datasets(grib_file)
ds_grib = xr.merge([
    ds if ds['longitude'].max() <= 180 else ds.assign_coords(longitude=(ds['longitude'] + 180) % 360 - 180)
    for ds in ds_grib_list
], compat='override')

# --- Abrir Zarr ---
ds_zarr = xr.open_zarr(zarr_dir)

# --- Função para extrair média diária no ponto ---
def daily_mean_at_point(ds, lat, lon):
    """
    Recebe um Dataset (t2m), latitude e longitude.
    Retorna um array 1D com média diária no ponto.
    """
    # Seleciona ponto específico
    da = ds['t2m'].sel(latitude=lat, longitude=lon, method='nearest')
    
    # Converte para Celsius
    da_c = da - 273.15
    
    # Agrupa por dia e tira média de cada dia (sobre 'time' e 'step')
    daily_mean = da_c.groupby('time.date').mean(dim=['time','step'])
    
    return daily_mean

# --- Extrair médias diárias ---
grib_daily = daily_mean_at_point(ds_grib, lat_point, lon_point)
zarr_daily = daily_mean_at_point(ds_zarr, lat_point, lon_point)

# --- Dias ---
dias = range(len(grib_daily))

# --- Plot comparativo dia a dia ---
plt.figure(figsize=(12,6))
plt.plot(dias, grib_daily.values, marker='o', label='GRIB')
plt.plot(dias, zarr_daily.values, marker='x', label='Zarr')
plt.title(f'Comparação de Temperatura 2m diária no ponto ({lat_point}, {lon_point})')
plt.xlabel('Dia')
plt.ylabel('Temperatura média (°C)')
plt.xticks(dias)
plt.grid(True)
plt.legend()
plt.show()
