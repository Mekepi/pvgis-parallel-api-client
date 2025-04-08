from urllib3 import request
from multiprocessing import Process, Pipe
from multiprocessing.connection import PipeConnection
from time import time, sleep
from os import remove, mkdir, listdir
from os.path import dirname, abspath, isdir, isfile, getsize
from pathlib import Path
from psutil import virtual_memory
from gzip import open as gsopen

def compress_file(file_path:Path):
    with open(file_path, "rb") as fin:
        with gsopen("%s.gz"%(str(file_path)), "wb", 9,) as fout:
            fout.write(fin.read())
    remove(file_path)

def request_timeseries(coord:str, geocode:int, st_sigla:str, con:PipeConnection, compressed:bool = True) -> None:
    line:list = [float(coord.split(",")[1]),float(coord.split(",")[0])]
    file_path:Path = Path("%s\\data\\%s\\[%i]\\[%i]timeseries(%.6f,%.6f).csv"%(dirname(abspath(__file__)), st_sigla, geocode, geocode,line[0],line[1]))

    try:
        file = open(file_path, "xb")
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
            if (l.startswith("Latitude (decimal degrees):")):
                if(compressed):
                    compress_file(file_path)
            elif (l.startswith('{"message":"Location over the sea. Please, select another location"') or
                  l.startswith('{"message":"Internal Server Error","status":500}')):
                None
            else:
                remove(file_path)
                con.send(coord)
    
    con.close()

def new_coord(coord:str, geocode:int, st_sigla:str, compressed:bool = True) -> bool:
    file_path:Path = Path("%s\\data\\%s\\[%i]\\[%i]timeseries(%.6f,%.6f).csv"%(dirname(abspath(__file__)), st_sigla, geocode,
                                                                               geocode, float(coord.split(',')[1]), float(coord.split(',')[0])))
    compressed_file_path:Path = Path("%s.gz"%(str(file_path)))

    if(isfile(compressed_file_path)):
       if(getsize(compressed_file_path)>0):
           return False
       else:
           remove(compressed_file_path)
    
    if (isfile(file_path)):
        if(getsize(file_path)>0):
            if(compressed):
                compress_file(file_path)
            return False
        else:
            remove(file_path)
    
    return True

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
                lines:list[str] = [line for line in inputs.readlines() if(new_coord(line, geocode, st_sigla, compressed))]
            
        else:
            coords_path = Path("%s\\retry.dat"%(dirname(abspath(__file__))))
            with open(coords_path, "r") as inputs:
                lines = inputs.readlines()
        

        if(lines):
            parent_cons:tuple[PipeConnection]
            child_cons:tuple[PipeConnection]
            parent_cons, child_cons = zip(*map(lambda _: Pipe(False), range(len(lines))))
            processes:list[Process] = list(map(lambda line, con: Process(target=request_timeseries, args=[line,geocode,st_sigla,con, compressed]), lines, child_cons))

            if (not(isdir("%s\\data"%(dirname(abspath(__file__)))))):
                try: mkdir("%s\\data"%(dirname(abspath(__file__))))
                except FileExistsError: None
            if (not(isdir("%s\\data\\%s"%(dirname(abspath(__file__)), st_sigla)))):
                try: mkdir("%s\\data\\%s"%(dirname(abspath(__file__)), st_sigla))
                except FileExistsError: None
            if (not(isdir("%s\\data\\%s\\[%i]"%(dirname(abspath(__file__)), st_sigla, geocode)))):
                try: mkdir("%s\\data\\%s\\[%i]"%(dirname(abspath(__file__)), st_sigla, geocode))
                except FileExistsError: None

            sleep_count:int = 0
            i:int = 0
            while(i<len(processes)):
                while(i<len(processes) and len([al for al in processes if al.is_alive()])<100):
                    while ((virtual_memory()[0]-virtual_memory()[3])/(1024**2)<311):
                        sleep(1)
                        sleep_count += 1
                    
                    processes[i].start()
                    i += 1
                    sleep(0.04)

            for process in processes:
                process.join()
                process.close()
            
            print("  Sleep duration: %.2f"%(0.04*len(processes)+sleep_count))
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
            

            if(not(rt)): print("[%i] execution time: %.2f" %(geocode, time()-start_time))

def state_timeseries(geocode_or_sigla:list[int|str], compressed:bool = True) -> None:
    states = {
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
        if (isinstance(gs, int) and gs in states.keys()):
            st_sigla:str = states[gs]
        elif (isinstance(gs, str) and gs in states.values()):
            st_sigla = gs
        
        if (st_sigla):
            start_time:float = time()
            br:str = next(f for f in listdir("%s"%(dirname(abspath(__file__)))) if f.startswith("Brasil"))
            stfolder:str = next(f for f in listdir("%s\\%s"%(dirname(abspath(__file__)), br)) if f[0:2] == st_sigla)
            geocode_list:list[int] = list((int(file[1:8]) for file in listdir("%s\\%s\\%s"%(dirname(abspath(__file__)), br, stfolder))))
            city_timeseries(geocode_list, compressed)
            print("[%i] %s execution time: %.2f" %(list(states.keys())[list(states.values()).index(st_sigla)], st_sigla, time()-start_time))
        else:
            print(gs, "<- inválido")

def brasil_timeseries(compressed:bool = True) -> None:
    start_time:float = time()
    br:str = next(f for f in listdir("%s"%(dirname(abspath(__file__)))) if f.startswith("Brasil"))
    sigla_list:list[str|int] = [stf[0:2] for stf in listdir("%s\\%s"%(dirname(abspath(__file__)), br))]
    state_timeseries(sigla_list, compressed)
    print("Brasil execution time: %.2f" %(time()-start_time))

def main() -> None:
    """ Todas as funções dependendem do nome da pasta ser Brasil no começo e as subpastas com as siglas dos estados no começo.
        E elas lerão a PRIMEIRA pasta que tenha o começo como Brasil.
        Deixei o número de coordenadas para ter uma ideia do volume de dados que irá baixar.
        Para o raio de 1,35 km foram 1.123.128.
        É esperado que sejam 7,401 TiB (8,138 TB). Talvez um pouco menos, desconsiderando coordenadas que caíram no mar e que os dados não vão até 2020, que são os mais recentes.
        Por padrão, irá comprimir com gzip, reduzindo o volume de dados para 1,451 TiB (1,595 TB).
        Caso queira aumentar o raio para diminuir o número de coordenadas, apenas lembre de não mexer no começo do nome das pastas e não mexer no nome dos arquivos. """

    """ A city_timeseries recebe uma lista de geocódigos(municípos -> 7 dígitos) e baixa os municípios em sequência,
        mas paralelamente todas as coordenadas do município. 
        Opcionais:
            compressed -> booleano que dita se irá comprimir os arquivos ou não. Por padrão, irá comprimir.
            rt -> NÃO atribua nada. """
    
    #city_timeseries([4300406])

    """ A state_timeseries recebe uma lista de geocódigos(estado -> 2 primeiros dígitos dos municípios) ou siglas.
        Opcionais:
            compressed -> booleano que dita se irá comprimir os arquivos ou não. Por padrão, irá comprimir."""

    state_timeseries(["RR"])

    """ A brasil_timeseries não recebe argumentos obrigatórios
        Opcionais:
            compressed -> booleano que dita se irá comprimir os arquivos ou não. Por padrão, irá comprimir."""

    #brasil_timeseries()

if __name__ == '__main__':
    main()
