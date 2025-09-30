import xarray as xr
import os
import matplotlib.pyplot as plt
import cartopy.crs as ccrs

# --- Configuração ---
zarrStorePath = 'output.zarr'

# --- Testes ---
def testarZarr(caminho):
    """
    Realiza uma série de testes para validar o Zarr store.
    """
    if not os.path.isdir(caminho):
        print(f"Erro: O diretório '{caminho}' não foi encontrado.")
        return

    print(f"Iniciando testes no Zarr store: '{caminho}'")

    try:
        ds = xr.open_zarr(caminho)
        print("\n[SUCESSO] O Zarr store foi aberto com sucesso.")

        print("\n--- Estrutura do Dataset ---")
        print(ds)
        print("--------------------------\n")

        if 't2m' in ds.data_vars:
            print("Testando a variável 't2m' (2m_temperature)...")
            temperaturaMediaKelvin = ds['t2m'].mean()
            tempMediaCelsius = temperaturaMediaKelvin.compute().item() - 273.15
            print(f"[SUCESSO] Cálculo realizado. Temperatura média: {tempMediaCelsius:.2f} °C")
        else:
            print("[AVISO] Variável 't2m' não encontrada no dataset para teste de cálculo.")

        print("\nTestando o acesso a um ponto de dados específico...")
        pontoDeDados = ds.sel(latitude=-15.5, longitude=-55.2, method='nearest')

        # Seleciona o primeiro time e step
        tempNoPonto = pontoDeDados['t2m'].isel(time=0, step=0).compute().item() - 273.15
        print(f"[SUCESSO] Temperatura no ponto mais próximo de (lat=-15.5, lon=-55.2): {tempNoPonto:.2f} °C")

    except Exception as e:
        print(f"\n[FALHA] Ocorreu um erro ao testar o Zarr store: {e}")

def plotarMapa(caminho, nomeVariavel='t2m'):
    """
    Plota um mapa 2D de uma variável específica do Zarr store.
    """
    print(f"\nTentando gerar um mapa para a variável '{nomeVariavel}'...")
    try:
        ds = xr.open_zarr(caminho)

        if nomeVariavel not in ds.data_vars:
            print(f"[ERRO] Variável '{nomeVariavel}' não encontrada para plotagem.")
            return

        # Seleciona o primeiro time e step
        dadoParaPlotar = ds[nomeVariavel].isel(time=0, step=0)

        fig = plt.figure(figsize=(10, 8))
        ax = plt.axes(projection=ccrs.PlateCarree())

        dadoParaPlotar.plot(ax=ax, transform=ccrs.PlateCarree())

        ax.coastlines()
        ax.gridlines(draw_labels=True, linestyle='--')

        plt.title(f"Mapa da Variável: {nomeVariavel}")

        nomeArquivoGrafico = f'mapa_{nomeVariavel}.png'
        plt.savefig(nomeArquivoGrafico)
        print(f"[SUCESSO] Mapa salvo como '{nomeArquivoGrafico}'")
        plt.close(fig)

    except Exception as e:
        print(f"[FALHA] Não foi possível gerar o mapa: {e}")

if __name__ == '__main__':
    testarZarr(zarrStorePath)
    plotarMapa(zarrStorePath, nomeVariavel='t2m')
    plotarMapa(zarrStorePath, nomeVariavel='tp')
