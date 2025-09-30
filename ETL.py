import cdsapi
import zipfile
import os
import xarray as xr
import cfgrib

zipTargetFile = 'download.zip'
gribFileName = 'data.grib'
zarrOutputDir = 'output.zarr'

variablesList = [
    "2m_dewpoint_temperature",
    "2m_temperature",
    "skin_temperature",
    "soil_temperature_level_1",
     "soil_temperature_level_2",
    "soil_temperature_level_3",
    "soil_temperature_level_4",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_pressure",
    "total_precipitation",
    "skin_reservoir_content",
    "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2",
    "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4",
    "forecast_albedo",
    "surface_latent_heat_flux",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
    "surface_sensible_heat_flux",
    "surface_solar_radiation_downwards",
    "surface_thermal_radiation_downwards",
    "lake_bottom_temperature",
    "lake_ice_depth",
    "lake_ice_temperature",
    "lake_mix_layer_depth",
    "lake_mix_layer_temperature",
    "lake_shape_factor",
    "lake_total_layer_temperature", 
    "snow_albedo",
    "snow_cover",
    "snow_density",
    "snow_depth",
    "snow_depth_water_equivalent",
    "snowfall",
    "snowmelt",
    "temperature_of_snow_layer",
    "evaporation_from_bare_soil",
    "evaporation_from_open_water_surfaces_excluding_oceans",
    "evaporation_from_the_top_of_canopy",
    "evaporation_from_vegetation_transpiration",
    "potential_evaporation",
    "runoff",
    "snow_evaporation",
    "sub_surface_runoff",
    "surface_runoff",
    "total_evaporation",
    "leaf_area_index_high_vegetation",
    "leaf_area_index_low_vegetation",
    "high_vegetation_cover",
    "glacier_mask",
    "lake_cover",
    "low_vegetation_cover",
    "lake_total_depth",
    "land_sea_mask",
    "soil_type",
    "type_of_high_vegetation",
    "type_of_low_vegetation"
]

c = cdsapi.Client()

print("Iniciando a requisicao para a CDS API...")
c.retrieve(
    'reanalysis-era5-land',
    {
        'variable': variablesList,
        'year': '2022',
        'month': '01',
        'day': '01',
        'time': [
            "00:00", "01:00", "02:00", "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00", "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00", "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"
        ],
        'area': [5.3, -74, -34, -34],
        'format': 'grib',
    },
    zipTargetFile)
print(f"Download concluido: '{zipTargetFile}'")


try:
    print(f"\nDescompactando '{zipTargetFile}'...")
    with zipfile.ZipFile(zipTargetFile, 'r') as zip_ref:
        fileList = zip_ref.namelist()
        zip_ref.extractall('.')
        
        if fileList:
            originalGribName = fileList[0]
            
            if originalGribName != gribFileName:
                if os.path.exists(gribFileName):
                    os.remove(gribFileName)
                
                os.rename(originalGribName, gribFileName)
                print(f"   - Arquivo extraido e renomeado para '{gribFileName}'")
            else:
                print(f"   - Arquivo extraido ja se chama '{gribFileName}', nao e necessario renomear.")
        else:
            raise Exception("O arquivo ZIP esta vazio!")
except Exception as e:
    print(f"Erro ao descompactar o arquivo: {e}")
    exit()


try:
    print(f"\nConvertendo '{gribFileName}' para o formato Zarr...")

    datasets = cfgrib.open_datasets(gribFileName)

    corrected_datasets = []
    for ds_part in datasets:
        if not isinstance(ds_part, xr.Dataset):
            continue

        if 'longitude' in ds_part.coords and ds_part['longitude'].max() > 180:
            ds_part.coords['longitude'] = (ds_part.coords['longitude'] + 180) % 360 - 180

        corrected_datasets.append(ds_part)

    if not corrected_datasets:
        raise ValueError("Nenhum dataset GRIB valido foi encontrado no arquivo.")
    
    ds = xr.merge(corrected_datasets, compat='override')

    print("\nDataset lido e combinado com sucesso. Informacoes:")
    print(ds)

    ds.to_zarr(zarrOutputDir, mode='w', consolidated=True)
    print(f"\nConversao para Zarr concluida. Dados salvos em: '{zarrOutputDir}'")

except Exception as e:
    print(f"Erro ao converter o arquivo GRIB para Zarr: {e}")

finally:
    print("\nLimpando arquivos temporarios...")
    if os.path.exists(zipTargetFile):
        os.remove(zipTargetFile)
        print(f"   - Arquivo '{zipTargetFile}' removido.")
    if os.path.exists(gribFileName):
        os.remove(gribFileName)
        print(f"   - Arquivo '{gribFileName}' removido.")
    print("Processo finalizado!")