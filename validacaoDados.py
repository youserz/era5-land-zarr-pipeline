import cdsapi
import zipfile
import os
import xarray as xr
import cfgrib

# --- Configuração ---
zip_file = 't2m_download.zip'
grib_file = 't2m.grib'
zarr_dir = 't2m.zarr'

# Inicializa o cliente CDS
c = cdsapi.Client()

# --- Download da variável 2m_temperature ---
print("Iniciando download da variável t2m...")
c.retrieve(
    'reanalysis-era5-land',
    {
        'variable': ['2m_temperature'],
        'year': '2022',
        'month': '01',
        'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
                '11', '12', '13', '14', '15'],
        'time': [
            "00:00", "01:00", "02:00", "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00", "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00", "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"
        ],
        'area': [5.3, -74, -34, -34],  # [N, W, S, E]
        'format': 'grib',
    },
    zip_file
)
print(f"Download concluído: {zip_file}")

# --- Descompactar GRIB ---
try:
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        files = zip_ref.namelist()
        zip_ref.extractall('.')
        os.rename(files[0], grib_file)
        print(f"Arquivo extraído e renomeado para {grib_file}")
except Exception as e:
    print(f"Erro ao descompactar: {e}")
    exit()

# --- Converter GRIB para Zarr ---
try:
    print(f"\nConvertendo {grib_file} para Zarr...")
    datasets = cfgrib.open_datasets(grib_file)
    corrected = []
    for ds in datasets:
        if 'longitude' in ds.coords and ds['longitude'].max() > 180:
            ds.coords['longitude'] = (ds.coords['longitude'] + 180) % 360 - 180
        corrected.append(ds)

    ds_final = xr.merge(corrected, compat='override')
    ds_final.to_zarr(zarr_dir, mode='w', consolidated=True)
    print(f"Conversão concluída. Dados salvos em {zarr_dir}")

except Exception as e:
    print(f"Erro na conversão para Zarr: {e}")

print("Processo finalizado!")
