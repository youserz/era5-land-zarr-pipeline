# ERA5‑Land Zarr Pipeline

Pipeline para download, tratamento e conversão de dados da reanálise **ERA5‑Land** para o formato **Zarr**.

---
## 🧰 Dependências

Este projeto depende de bibliotecas Python para manipulação de dados, reanálises climáticas, transformação e visualização. Um exemplo de arquivo `requisitos.txt` poderia conter:

```text
xarray
zarr
netCDF4
requests
pandas
numpy
matplotlib
```

Você pode instalar todas as dependências com:

```bash
pip install -r requisitos.txt
```

---

## 🔧 Configuração

1. Crie um arquivo `.env` (ou configure variáveis de ambiente no sistema) para armazenar configurações sensíveis, por exemplo:

   ```text
   API_KEY=seu_token
   OUTPUT_PATH=/caminho/para/saida
   ```

2. Certifique-se de configurar corretamente os caminhos de entrada e saída nos scripts.

---

## 🚀 Uso

### Executar o pipeline principal

```bash
python ETL.py
```

Esse script é responsável por:
- baixar os dados brutos da reanálise (ex: GRIB ou NetCDF),
- processar e transformar,
- converter para Zarr,
- salvar o resultado na pasta de saída.

### Visualização / Plotagem

```bash
python plotData.py
```

Gera gráficos ou mapeamentos a partir dos dados processados.

### Testar integridade / leitura do Zarr

```bash
python testarZarr.py
```

Serve para verificar se os arquivos Zarr produzidos estão coerentes (leitura, atributos, etc.).

---

## 📌 Observações & dicas

- Os dados da reanálise **ERA5‑Land** estão disponíveis via o *Climate Data Store* da Copernicus. ([cds.climate.copernicus.eu](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=download&utm_source=chatgpt.com))  
- O formato **Zarr** é otimizado para leitura paralela, chunking e uso em ambientes distribuídos, sendo bastante usado em ciência de dados ambientais.  
- Aproveite variáveis como chunk sizes e compressão nos seus scripts de criação do Zarr para otimizar desempenho.

---

## 👤 Autor / Contato

<img src="https://github.com/youserz.png" alt="Foto de youserz" width="120" style="border-radius: 50%;" />

- **Nome:** Bernardo Diniz  
- **GitHub:** [youserz](https://github.com/youserz)  
- **Projeto:** *era5-land-zarr-pipeline*  
