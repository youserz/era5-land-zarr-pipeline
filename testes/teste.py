import os
import glob
import logging
import argparse
import xarray as xr
import zarr

# --- Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def convertGribByVariable(baseInputDir: str, baseOutputDir: str):
    """
    Converte arquivos GRIB em múltiplos Zarr stores, um para cada variável,
    usando xr.open_mfdataset para lidar com GRIBs complexos.
    """
    if not os.path.exists(baseOutputDir):
        os.makedirs(baseOutputDir)
        logging.info(f"Diretório de saída criado: {baseOutputDir}")

    subDirs = [d for d in os.listdir(baseInputDir) if os.path.isdir(os.path.join(baseInputDir, d))]

    for subDir in subDirs:
        inputPath = os.path.join(baseInputDir, subDir)
        # Use a wildcard to select all GRIB files
        gribFiles = sorted(glob.glob(os.path.join(inputPath, '**', '*.grib'), recursive=True))
        
        if not gribFiles:
            logging.warning(f"Nenhum arquivo GRIB encontrado em '{inputPath}', pulando.")
            continue

        logging.info(f"Iniciando processamento do diretório: '{subDir}'")

        try:
            # Use open_mfdataset to handle multiple files and internal inconsistencies
            # combine='by_coords' is the key to correctly merge all data
            ds = xr.open_mfdataset(
                gribFiles, 
                engine='cfgrib', 
                combine='by_coords'
            )
            
            variables = list(ds.data_vars)
            logging.info(f"Variáveis encontradas e combinadas no grupo '{subDir}': {variables}")

            # Loop to process each variable from the combined dataset
            for varName in variables:
                logging.info(f"--- Processando variável: '{varName}' ---")
                outputZarrPath = os.path.join(baseOutputDir, f"{subDir}_{varName}.zarr")
                
                # Select the specific variable and save it to Zarr
                # to_zarr will overwrite by default if the store already exists
                variable_ds = ds[[varName]]
                variable_ds.to_zarr(outputZarrPath, mode='w', consolidated=True)
                
                logging.info(f"--- Finalizado: '{varName}' salvo em '{outputZarrPath}' ---")

        except Exception as e:
            logging.error(f"Falha ao processar o diretório '{subDir}'. Erro: {e}")
            continue

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Converte arquivos GRIB em um Zarr por variável.")
    parser.add_argument(
        "--inputDir",
        type=str,
        required=True,
        help="Diretório de entrada base contendo as subpastas."
    )
    parser.add_argument(
        "--outputDir",
        type=str,
        required=True,
        help="Diretório de saída para salvar os arquivos Zarr."
    )
    args = parser.parse_args()

    convertGribByVariable(baseInputDir=args.inputDir, baseOutputDir=args.outputDir)