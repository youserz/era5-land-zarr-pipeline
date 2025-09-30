import cdsapi
import logging
import os
import subprocess
from datetime import date, timedelta
from tqdm import tqdm
import time
import zipfile

# --- Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_e_split.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def progresso(dataset, request, outputPath, client, maxRetries=5):
    """
    Faz o download do dataset via cdsapi. Se for ZIP, extrai e retorna o caminho
    do novo arquivo .grib. Se não, retorna o caminho original.
    """
    for attempt in range(maxRetries):
        try:
            logging.info(f"Iniciando download de {os.path.basename(outputPath)} (tentativa {attempt+1})...")
            client.retrieve(dataset, request, outputPath)
            logging.info(f"Download concluído: {os.path.basename(outputPath)}")
            
            with open(outputPath, "rb") as f:
                magic = f.read(2)

            if magic == b"PK":  # É um arquivo ZIP
                logging.info(f"O arquivo {os.path.basename(outputPath)} é um ZIP, extraindo...")
                extract_dir = os.path.dirname(outputPath)
                
                arquivos_antes = set(os.listdir(extract_dir))
                with zipfile.ZipFile(outputPath, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                arquivos_depois = set(os.listdir(extract_dir))
                
                novos_arquivos = arquivos_depois - arquivos_antes
                
                os.remove(outputPath)
                logging.info(f"Arquivo ZIP removido: {os.path.basename(outputPath)}")

                for novo_arquivo in novos_arquivos:
                    if novo_arquivo.endswith('.grib'):
                        caminho_grib_real = os.path.join(extract_dir, novo_arquivo)
                        logging.info(f"Arquivo GRIB extraído encontrado: {novo_arquivo}")
                        return caminho_grib_real

                raise FileNotFoundError("Nenhum arquivo .grib foi encontrado após a extração do ZIP.")
            
            else: # Não era um ZIP
                return outputPath

        except Exception as e:
            logging.warning(f"Tentativa {attempt+1} falhou: {e}")
            time.sleep(10)

    logging.error(f"Falha ao baixar {os.path.basename(outputPath)} após {maxRetries} tentativas.")
    raise RuntimeError(f"Download falhou para {os.path.basename(outputPath)}")

def separarGribPorVariavel(caminhoGribGrande: str, variaveisShortname: list, outputDir: str):
    logging.info(f"Separando o arquivo {os.path.basename(caminhoGribGrande)}...")
    
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    for var in variaveisShortname:
        nomeBaseArquivo = os.path.basename(caminhoGribGrande).replace('.grib', '')
        caminhoSaidaVariavel = os.path.join(outputDir, f"{nomeBaseArquivo}_{var}.grib")
        
        comando = ["grib_copy", "-w", f"shortName={var}", caminhoGribGrande, caminhoSaidaVariavel]

        try:
            subprocess.run(comando, check=True, capture_output=True, text=True)
            logging.info(f"  -> Variável '{var}' extraída com sucesso.")
        except FileNotFoundError:
            logging.error("ERRO CRÍTICO: O comando 'grib_copy' não foi encontrado. Instale o 'eccodes'.")
            return
        except subprocess.CalledProcessError as e:
            logging.error(f"  -> Falha ao extrair a variável '{var}'. Erro: {e.stderr}")

def baixarDadosEra5LandEmLotes(startDate: date, endDate: date, gruposVariaveis: dict, outputDir: str = "dadosGrib_agrupados") -> list:
    logging.info(f"Iniciando download em lotes para o período de {startDate} a {endDate}.")
    
    requestBase = {
        "time": ["{:02d}:00".format(h) for h in range(24)],
        "area": [5.3, -74, -34, -34],
        "format": "grib",
    }
    
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    
    client = cdsapi.Client()
    currentDate = startDate
    arquivosBaixados = []
    
    while currentDate <= endDate:
        periodEndDate = currentDate + timedelta(days=1)
        if periodEndDate > endDate:
            periodEndDate = endDate

        dateRange = [currentDate + timedelta(days=d) for d in range((periodEndDate - currentDate).days + 1)]
        requestDates = {
            'year': sorted(list(set([d.strftime('%Y') for d in dateRange]))),
            'month': sorted(list(set([d.strftime('%m') for d in dateRange]))),
            'day': sorted(list(set([d.strftime('%d') for d in dateRange]))),
        }
        
        for nomeGrupo, listas in gruposVariaveis.items():
            listaLongname = listas['long_name']
            grupoOutputDir = os.path.join(outputDir, nomeGrupo)
            if not os.path.exists(grupoOutputDir):
                os.makedirs(grupoOutputDir)

            fullRequest = {**requestBase, **requestDates, "variable": listaLongname}
            
            outputFilename = f"era5_land_{nomeGrupo}_{currentDate.strftime('%Y%m%d')}_{periodEndDate.strftime('%Y%m%d')}.grib"
            outputPath = os.path.join(grupoOutputDir, outputFilename)

            logging.info(f"Baixando grupo '{nomeGrupo}' de {currentDate} a {periodEndDate}")
            
            try:
                caminhoRealGrib = progresso("reanalysis-era5-land", fullRequest, outputPath, client)
                if caminhoRealGrib:
                    arquivosBaixados.append(caminhoRealGrib)
            except Exception as e:
                logging.error(f"Falha ao baixar o grupo '{nomeGrupo}'. Erro: {e}")

        currentDate = periodEndDate + timedelta(days=1)
        
    return arquivosBaixados

# --- Ponto de Entrada ---
if __name__ == '__main__':
    dataInicio = date(2024, 1, 1)
    dataFim = date(2024, 1, 1)
    diretorioDownloadsAgrupados = "dadosGrib_agrupados"
    diretorioFinalSeparado = "dadosGrib_separados"

    gruposDeVariaveis = {
        "surface": {
            "long_name": [
                '2m_dewpoint_temperature', '2m_temperature', 'skin_temperature',
                '10m_u_component_of_wind', '10m_v_component_of_wind', 'surface_pressure',
                'forecast_albedo', 'surface_latent_heat_flux', 'surface_net_solar_radiation',
                'surface_net_thermal_radiation', 'surface_sensible_heat_flux',
                'surface_solar_radiation_downwards', 'surface_thermal_radiation_downwards'
            ],
            "short_name": [
                'd2m', 't2m', 'skt', 'u10', 'v10', 'sp',
                'fal', 'slhf', 'ssr', 'str', 'sshf',
                'ssrd', 'strd'
            ]
        },
        "soil": {
            "long_name": [
                'soil_temperature_level_1', 'soil_temperature_level_2',
                'soil_temperature_level_3', 'soil_temperature_level_4',
                'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2',
                'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4',
            ],
            "short_name": [
                'stl1', 'stl2', 'stl3', 'stl4',
                'swvl1', 'swvl2', 'swvl3', 'swvl4'
            ]
        },
        "snow": {
            "long_name": [
                'snow_albedo', 'snow_cover', 'snow_density', 'snow_depth',
                'snow_depth_water_equivalent', 'snowfall', 'snowmelt',
                'temperature_of_snow_layer'
            ],
            "short_name": [
                'asn', 'sc', 'rsn', 'sd',
                'sdwe', 'sf', 'smlt',
                'tsn'
            ]
        },
        "hydrology_vegetation": {
            "long_name": [
                'total_precipitation', 'skin_reservoir_content', 'evaporation_from_bare_soil',
                'evaporation_from_open_water_surfaces_excluding_oceans',
                'evaporation_from_the_top_of_canopy', 'evaporation_from_vegetation_transpiration',
                'potential_evaporation', 'runoff', 'snow_evaporation', 'sub_surface_runoff',
                'surface_ runoff', 'total_evaporation', 'leaf_area_index_high_vegetation',
                'leaf_area_index_low_vegetation'
            ],
            "short_name": [
                'tp', 'src', 'evabs', 'evaow', 'evatc', 'evavt',
                'pev', 'ro', 'evas', 'ssro', 'sro', 'e',
                'lai_hv', 'lai_lv'
            ]
        },
        "lakes": {
            "long_name": [
                'lake_bottom_temperature', 'lake_ice_depth', 'lake_ice_temperature',
                'lake_mix_layer_depth', 'lake_mix_layer_temperature', 'lake_shape_factor',
                'lake_total_layer_temperature'
            ],
            "short_name": [
                'lbt', 'licd', 'lict', 'lmld', 'lmlt', 'lshf', 'ltlt'
            ]
        }
    }

    arquivosParaSeparar = baixarDadosEra5LandEmLotes(
        startDate=dataInicio, 
        endDate=dataFim,
        gruposVariaveis=gruposDeVariaveis,
        outputDir=diretorioDownloadsAgrupados
    )

    logging.info("="*50)
    logging.info("Iniciando a separação dos arquivos GRIB baixados...")
    
    if not arquivosParaSeparar:
        logging.warning("Nenhum arquivo foi baixado. Encerrando o processo de separação.")
    else:
        for arquivoGribAgrupado in arquivosParaSeparar:
            nomeGrupoEncontrado = None
            nomeBaseArquivo = os.path.basename(arquivoGribAgrupado)
            for nomeGrupo in gruposDeVariaveis.keys():
                if f"_{nomeGrupo}_" in nomeBaseArquivo or nomeBaseArquivo == "data.grib":
                    # Lógica para encontrar o grupo pelo diretório se o nome for 'data.grib'
                    if nomeBaseArquivo == "data.grib":
                        partes_caminho = arquivoGribAgrupado.replace("\\", "/").split('/')
                        if len(partes_caminho) > 1 and partes_caminho[-2] in gruposDeVariaveis:
                            nomeGrupoEncontrado = partes_caminho[-2]
                            break
                    else: # Lógica original para nomes de arquivo completos
                        nomeGrupoEncontrado = nomeGrupo
                        break
            
            if nomeGrupoEncontrado:
                logging.info(f"Processando arquivo do grupo '{nomeGrupoEncontrado}': {os.path.basename(arquivoGribAgrupado)}")
                listaShortNames = gruposDeVariaveis[nomeGrupoEncontrado]["short_name"]
                
                outputDirGrupoSeparado = os.path.join(diretorioFinalSeparado, nomeGrupoEncontrado)

                separarGribPorVariavel(
                    caminhoGribGrande=arquivoGribAgrupado,
                    variaveisShortname=listaShortNames,
                    outputDir=outputDirGrupoSeparado
                )
            else:
                logging.warning(f"Não foi possível identificar o grupo para o arquivo: {arquivoGribAgrupado}. Pulando.")

    logging.info("Processo de extração e separação concluído.")