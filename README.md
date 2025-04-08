# PVGIS Parallel API Client  

Parallel fetcher for hourly solar radiation of Brazilian cities from [PVGIS](https://re.jrc.ec.europa.eu/pvg_tools/en/#HR).

## Features  
- **Parallel processing** (100 concurrent requests)  
- **Memory-aware throttling** (auto-pauses when RAM <300MB free)  
- **Gzip compression** (75% size reduction)  

## Prerequisites  
1. First generate coordinates using [Brazil HexGrid Generator](https://github.com/Mekepi/brazil-hexgrid-generator).  
2. Place output in `data` before running.  

## Usage  
```python
from src.PVGIS_5.3_paralleled import city_timeseries
city_timeseries([3304557])  # Rio de Janeiro
