import cdsapi
import logging
import os
from datetime import date, timedelta
from tqdm import tqdm


# --- Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("downloadEra5Land.log"),
        logging.StreamHandler()
    ]
)


def progresso(dataset, request, output_path, client):
    """
    Faz o download via cdsapi com barra de progresso usando tqdm.
    """
    # Primeiro envia o pedido ao CDS (gera o link de download)
    result = client.retrieve(dataset, request)
    url = result.location

    # Faz o download com barra de progresso
    with client.session.get(url, stream=True) as response:
        total = int(response.headers.get('content-length', 0))
        with open(output_path, 'wb') as file, tqdm(
            desc=os.path.basename(output_path),
            total=total,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=1024 * 1024):  # 1 MB
                size = file.write(data)
                bar.update(size)

    logging.info(f"✅ Download concluído: {output_path}")


def baixarDadosEra5LandEmLotes(
    startDate: date, 
    endDate: date, 
    outputDir: str = "dadosGrib"
):
    """
    Baixa dados do dataset ERA5-Land em lotes de 15 dias para um intervalo de datas.

    Args:
        startDate (date): Data de início do período de download.
        endDate (date): Data de fim do período de download.
        outputDir (str): Diretório onde os arquivos .grib serão salvos.
    """
    logging.info(f"Iniciando download em lotes para o período de {startDate} a {endDate}.")

    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
        logging.info(f"Diretório criado: {outputDir}")

    # Cliente do CDS
    client = cdsapi.Client()

    # --- Requisição Base ---
    request = {
        "variable": [
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
        ],
        "time": [
            "00:00", "01:00", "02:00", "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00", "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00", "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"
        ],
        "area": [5.3, -74, -34, -34],
        "format": "grib", # LINHA CORRIGIDA
    }


    # --- Loop de Download ---
    currentDate = startDate
    while currentDate <= endDate:
        periodEndDate = currentDate + timedelta(days=14)
        if periodEndDate > endDate:
            periodEndDate = endDate

        dateRange = [currentDate + timedelta(days=d) for d in range((periodEndDate - currentDate).days + 1)]
        
        requestDates = {
            'year': list(set([d.strftime('%Y') for d in dateRange])),
            'month': list(set([d.strftime('%m') for d in dateRange])),
            'day': list(set([d.strftime('%d') for d in dateRange])),
        }

        fullRequest = {**request, **requestDates}
        outputFilename = f"era5_land_{currentDate.strftime('%Y%m%d')}_{periodEndDate.strftime('%Y%m%d')}.grib"
        outputPath = os.path.join(outputDir, outputFilename)

        logging.info(f"Baixando lote de {currentDate.strftime('%Y-%m-%d')} a {periodEndDate.strftime('%Y-%m-%d')}")
        
        try:
            progresso("reanalysis-era5-land", fullRequest, outputPath, client)
        except Exception as e:
            logging.error(f"❌ Falha ao baixar o lote {currentDate} - {periodEndDate}. Erro: {e}")

        currentDate = periodEndDate + timedelta(days=1)


if __name__ == '__main__':
    dataInicio = date(2002, 1, 1)
    dataFim = date(2002, 1, 15)  # exemplo
    baixarDadosEra5LandEmLotes(startDate=dataInicio, endDate=dataFim)