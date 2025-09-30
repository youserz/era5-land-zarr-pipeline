# gribToZarr.py (v5 - Solução Definitiva)

import os
import glob
import logging
import argparse
import xarray as xr

# --- Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def convertGribToZarr(gribDir: str, zarrPath: str):
    """
    Converte uma coleção de arquivos GRIB em um Zarr de forma sequencial,
    isolando variáveis problemáticas para garantir a conversão.
    """
    if not os.path.isdir(gribDir):
        logging.error(f"❌ O diretório de entrada não foi encontrado: {gribDir}")
        return

    gribFiles = sorted(glob.glob(os.path.join(gribDir, '*.grib')))
    if not gribFiles:
        logging.error(f"❌ Nenhum arquivo .grib encontrado em: {gribDir}")
        return
        
    logging.info(f"Encontrados {len(gribFiles)} arquivos GRIB para processar em modo de alta compatibilidade.")

    # Loop para processar um arquivo de cada vez
    for i, gribFile in enumerate(gribFiles):
        try:
            logging.info(f"Processando arquivo {i + 1}/{len(gribFiles)}: {os.path.basename(gribFile)}")
            
            # --- PASSO 1: Lê todas as variáveis, EXCETO a problemática 'skt' ---
            # O 'shortName!': 'skt' significa "onde o shortName NÃO é skt".
            ds_main = xr.open_dataset(
                gribFile, 
                engine="cfgrib",
                backend_kwargs={'filter_by_keys': {'shortName!': 'skt'}}
            )

            # --- PASSO 2: Lê APENAS a variável 'skt' em um dataset separado ---
            ds_skt = xr.open_dataset(
                gribFile, 
                engine="cfgrib",
                backend_kwargs={'filter_by_keys': {'shortName': 'skt'}}
            )

            # --- PASSO 3: Une os dois datasets ---
            # O Xarray irá alinhar as coordenadas corretamente.
            ds = xr.merge([ds_main, ds_skt])
            
            # Define o modo de escrita
            mode = 'w' if i == 0 else 'a'
            append_dim = "time" if i > 0 else None
            
            if mode == 'w':
                logging.info(f"Criando novo Zarr store em: {zarrPath}")
                ds.to_zarr(zarrPath, mode=mode, consolidated=True)
            else:
                logging.info(f"Anexando dados ao Zarr store...")
                # Antes de anexar, removemos coordenadas que já existem no Zarr
                # para evitar conflitos.
                for coord in ds.coords:
                    if coord in xr.open_zarr(zarrPath).coords and coord != append_dim:
                        ds = ds.drop_vars(coord)
                
                ds.to_zarr(zarrPath, mode=mode, append_dim=append_dim)
                xr.consolidate_zarr_metadata(zarrPath)

        except Exception as e:
            logging.error(f"❌ Falha ao processar o arquivo: {os.path.basename(gribFile)}")
            logging.error(f"   Detalhe do erro: {e}")
            return

    logging.info("✅ Conversão para Zarr concluída com sucesso!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Converte arquivos GRIB em um Zarr consolidado (modo de alta compatibilidade).")
    parser.add_argument(
        "--gribDir",
        type=str,
        required=True,
        help="Diretório de entrada contendo os arquivos .grib."
    )
    parser.add_argument(
        "--zarrPath",
        type=str,
        required=True,
        help="Caminho para o arquivo .zarr de saída."
    )
    args = parser.parse_args()

    convertGribToZarr(
        gribDir=args.gribDir,
        zarrPath=args.zarrPath
    )