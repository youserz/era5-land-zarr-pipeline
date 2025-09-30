import pygrib
import os

arquivo = "C:\\Users\\Zetta\\Documents\\ERA5Land\\dadosGrib_agrupados\\soil\\data.grib"

print(f"Tentando abrir: {arquivo}")
print("Tamanho do arquivo (bytes):", os.path.getsize(arquivo))

try:
    grbs = pygrib.open(arquivo)
    count = 0
    for grb in grbs:
        print(grb)
        count += 1
    print("Total de mensagens lidas:", count)
except Exception as e:
    print("Erro ao abrir:", e)
