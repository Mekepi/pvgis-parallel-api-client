from urllib3 import request
from multiprocessing import Process, Pipe
from multiprocessing.connection import PipeConnection
from time import time, sleep
from os import remove, mkdir, listdir
from os.path import dirname, abspath, isdir, isfile, getsize
from pathlib import Path
from psutil import virtual_memory
import gzip

def request_timeseries(coord:str, geocode:int, st_sigla:str, con:PipeConnection) -> None:
    line:list = [float(coord.split(",")[1]),float(coord.split(",")[0])]
    file_path:Path = Path("%s\\data\\%s\\[%i]\\[%i]timeseries(%.6f,%.6f).csv"%(dirname(abspath(__file__)), st_sigla, geocode, geocode,line[0],line[1]))

    try:
        file = open(file_path, "xb")
    except FileExistsError:
        if (getsize(file_path)>0):
            None#print(err)
        else:
            remove(file_path)
            con.send(coord)
    except Exception as err:
        print(err)
        con.send(coord)
    else: 
        try:
            file.write(request("GET","https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%.6f&lon=%.6f&components=1" %(line[0],line[1]),preload_content=False,retries=False,timeout=None).data)
        except Exception as err:
            file.close()
            remove(file_path)
            print(err)
            con.send(coord)
        else:
            file.close()
            with open(file_path, "r") as f:
                l:str = f.readline()
            b:bool = (
                      l.startswith("Latitude (decimal degrees):") or
                      l.startswith('{"message":"Location over the sea. Please, select another location"') or
                      l.startswith('{"message":"Internal Server Error","status":500}')
                     )
            if (not(b)):
                remove(file_path)
                con.send(coord)
    con.close()

def compress_file(file_path:Path) -> None:
    with open(file_path, "rb") as fin:
        with gzip.open("%s.gz"%(str(file_path)), "wb", 9,) as fout:
            fout.write(fin.read())
    remove(file_path)

def city_timeseries(geocode_list:list[int], compressed:bool = True, rt:bool = False) -> None:
    
    for geocode in geocode_list:
        start_time:float = time()

        sts = {
            12: "AC",
            27: "AL",
            13: "AM",
            16: "AP",
            29: "BA",
            23: "CE",
            53: "DF",
            32: "ES",
            52: "GO",
            21: "MA",
            31: "MG",
            50: "MS",
            51: "MT",
            15: "PA",
            25: "PB",
            26: "PE",
            22: "PI",
            41: "PR",
            33: "RJ",
            24: "RN",
            43: "RS",
            11: "RO",
            14: "RR",
            42: "SC",
            35: "SP",
            28: "SE",
            17: "TO"
        }
        st_sigla:str = sts[geocode//100000]

        if (not(rt)):
            br:str = next(f for f in listdir("%s"%(dirname(abspath(__file__)))) if f.startswith("Brasil"))
            stfolder:str = next(f for f in listdir("%s\\%s"%(dirname(abspath(__file__)), br)) if f[0:2] == st_sigla)
            coords_path:Path = Path("%s\\%s\\%s\\%s"%(dirname(abspath(__file__)), br, stfolder,
                                                                next(f for f in listdir("%s\\%s\\%s"%(dirname(abspath(__file__)), br, stfolder)) if int(f[1:8]) == geocode)))
            with open(coords_path, "r") as inputs:
                lines:list[str] = inputs.readlines()
        
        else:
            coords_path = Path("%s\\retry.dat"%(dirname(abspath(__file__))))
            with open(coords_path, "r") as inputs:
                lines = inputs.readlines()

        parent_cons:tuple[PipeConnection]
        child_cons:tuple[PipeConnection]
        parent_cons, child_cons = zip(*map(lambda _: Pipe(False), range(len(lines))))
        processes:list[Process] = list(map(lambda line, con: Process(target=request_timeseries, args=[line,geocode,st_sigla,con]), lines, child_cons))
        
        if (not(isdir("%s\\data"%(dirname(abspath(__file__)))))):
            try: mkdir("%s\\data"%(dirname(abspath(__file__))))
            except FileExistsError: None
        if (not(isdir("%s\\data\\%s"%(dirname(abspath(__file__)), st_sigla)))):
            try: mkdir("%s\\data\\%s"%(dirname(abspath(__file__)), st_sigla))
            except FileExistsError: None
        if (not(isdir("%s\\data\\%s\\[%i]"%(dirname(abspath(__file__)), st_sigla, geocode)))):
            try: mkdir("%s\\data\\%s\\[%i]"%(dirname(abspath(__file__)), st_sigla, geocode))
            except FileExistsError: None
        city_folder: str = "%s\\data\\%s\\[%i]"%(dirname(abspath(__file__)), st_sigla, geocode)
        
        k:int = 0
        processes_blocks:list[list[Process]] = []
        for k in range(660, len(processes), 660):
                processes_blocks.extend([processes[k-660:k]])
        processes_blocks.extend([processes[k:]])

        sleep_count:int = 0
        for block in processes_blocks:
            start = time()
            for process in block:
                process.start()
                sleep(0.1)
                while ((virtual_memory()[0]-virtual_memory()[3])/(1024**2)<311):
                    sleep(1)
                    sleep_count += 1
            
            for process in block:
                process.join()
                process.close()
            print("request block duration:", time()-start)
            start = time()
            if (compressed):
                list(map(lambda f: Process(target=compress_file, args=[f]).start(), [Path("%s\\%s"%(city_folder, file)) for file in listdir(city_folder) if file.endswith(".csv")]))
            
            print("starting compress exec time:", time()-start)
        
        print("  Sleep duration: %.2f"%(0.1*len(processes)+sleep_count))
        print("Request duration: %.2f"%(time()-start_time))
                
        parent_recvs:list[str] = [con.recv() for con in parent_cons if con.poll()]
        if (parent_recvs):
            retry_path:Path = Path("%s\\retry.dat"%(dirname(abspath(__file__))))
            with open(retry_path, "w") as retry:
                retry.writelines(parent_recvs)
            print("Retrying %i coordinates..."%(len(parent_recvs)))
            city_timeseries([geocode], compressed, True)
            if (isfile(retry_path)):
                remove(retry_path)
        
        if(not(rt)): print("[%i] execution time: %f" %(geocode, time()-start_time))

def state_timeseries(geocode_or_sigla:list, compressed:bool = True) -> None:
    sts = {
            12: "AC",
            27: "AL",
            13: "AM",
            16: "AP",
            29: "BA",
            23: "CE",
            53: "DF",
            32: "ES",
            52: "GO",
            21: "MA",
            31: "MG",
            50: "MS",
            51: "MT",
            15: "PA",
            25: "PB",
            26: "PE",
            22: "PI",
            41: "PR",
            33: "RJ",
            24: "RN",
            43: "RS",
            11: "RO",
            14: "RR",
            42: "SC",
            35: "SP",
            28: "SE",
            17: "TO"
        }
    for gs in geocode_or_sigla:
        if (type(gs) == int and gs in sts.keys()):
            st_sigla:str = sts[gs]
        elif (gs in sts.values()):
            st_sigla = gs
        
        if (st_sigla):
            br:str = next(f for f in listdir("%s"%(dirname(abspath(__file__)))) if f.startswith("Brasil"))
            stfolder:str = next(f for f in listdir("%s\\%s"%(dirname(abspath(__file__)), br)) if f[0:2] == st_sigla)
            geocode_list:list[int] = list((int(file[1:8]) for file in listdir("%s\\%s\\%s"%(dirname(abspath(__file__)), br, stfolder))))
            city_timeseries(geocode_list, compressed)
        else:
            print(gs, "<- inválido")

def brasil_timeseries(compressed:bool = True) -> None:
    br:str = next(f for f in listdir("%s"%(dirname(abspath(__file__)))) if f.startswith("Brasil"))
    for stfolder in listdir("%s\\%s"%(dirname(abspath(__file__)), br)):
        geocode_list:list[int] = list((int(file[1:8]) for file in listdir("%s\\%s\\%s"%(dirname(abspath(__file__)), br, stfolder))))
        city_timeseries(geocode_list, compressed)

def main() -> None:
    """ Todas as funções dependendem do nome da pasta ser Brasil no começo e as subpastas com as siglas dos estados no começo.
        E elas lerão a PRIMEIRA pasta que tenha o começo como Brasil.
        Deixei o número de coordenadas para ter uma ideia do volume de dados que irá baixar.
        Para o raio de 1,35 km foram 1.123.131. É esperado que sejam 7,123 TiB (7,831 TB). Talvez um pouco menos, desconsiderando coordenadas
        que caíram no mar.
        Caso queira aumentar o raio para diminuir o número de coordenadas, apenas lembre de não mexer no começo do nome das pastas e não mexer no nome dos arquivos. """

    """ A city_timeseries recebe uma lista de geocódigos(municípos -> 7 dígitos) e baixa os municípios em sequência,
        mas paralelamente todas as coordenadas do município. 
        Opcionais:
            compressed -> booleano que dita se irá comprimir os arquivos ou não. Por padrão, irá comprimir.
            rt -> NÃO atribua nada. """
    
    city_timeseries([1200500])

    """ A state_timeseries recebe uma lista de geocódigo(estado -> 2 primeiros dígitos dos municípios) ou sigla.
        Opcionais:
            compressed -> booleano que dita se irá comprimir os arquivos ou não. Por padrão, irá comprimir."""

    #state_timeseries(["AC", 13])

    """ A brasil_timeseries não recebe argumentos obrigatórios
        Opcionais:
            compressed -> booleano que dita se irá comprimir os arquivos ou não. Por padrão, irá comprimir."""

    #brasil_timeseries()

if __name__ == '__main__':
    main()
