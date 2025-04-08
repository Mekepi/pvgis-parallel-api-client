from urllib3 import request
from multiprocessing import Process, Pipe
from multiprocessing.connection import PipeConnection
from time import time, sleep
from os import remove, mkdir
from os.path import dirname, abspath, isdir, isfile, getsize
from pathlib import Path
from psutil import virtual_memory



def request_timeseries(coord:str, con:PipeConnection) -> None:

    line:list = [float(coord.split(",")[0]),float(coord.split(",")[1]),int(coord.split(",")[2]),int(coord.split(",")[3])]
    file_path:Path = Path("%s\\data\\timeseries(%.6f,%.6f)[%i,%i].csv"%(dirname(abspath(__file__)),line[0],line[1],line[2],line[3]))

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
            file.write(request("GET","https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%.6f&lon=%.6f&startyear=%i&endyear=%i&components=1" %(line[0],line[1],line[2],line[3]),preload_content=False,retries=False,timeout=None).data)
        except Exception as err:
            file.close()
            remove(file_path)
            print(err)
            con.send(coord)
        else:
            file.close()
            with open(file_path, "r") as f:
                b:bool = f.readline().startswith("Latitude (decimal degrees):")
            if (not(b)):
                remove(file_path)
                con.send(coord)
    con.close()


def main(file_path:Path) -> None:
    start_time:float = time()

    with open(file_path, "r") as inputs:
        lines:list[str] = inputs.readlines()

    parent_cons:tuple[PipeConnection]
    child_cons:tuple[PipeConnection]
    parent_cons, child_cons = zip(*map(lambda _: Pipe(False), range(len(lines))))
    processes:list[Process] = list(map(lambda line, con: Process(target=request_timeseries, args=[line,con]), lines, child_cons))
    
    if (not(isdir("%s\\data"%(dirname(abspath(__file__)))))):
        mkdir("%s\\data"%(dirname(abspath(__file__))))
    
    k:int = 0
    processes_blocks:list[list[Process]] = []
    for k in range(750, len(processes), 750):
            processes_blocks.extend([processes[k-750:k]])
    processes_blocks.extend([processes[k:]])

    sleep_count:int = 0
    for block in processes_blocks[k-3:k]:
        for process in block:
            process.start()
            sleep(0.1)
            while ((virtual_memory()[0]-virtual_memory()[3])/(1024**2)<311):
                sleep(1)
                sleep_count += 1
        
        for process in block:
            process.join()
            process.close()
    
    print("  Sleep duration:", 0.1*len(block)+sleep_count)
    print("Request duration:", time()-start_time)
            
    parent_recvs:list[str] = [con.recv() for con in parent_cons if con.poll()]
    if (parent_recvs):
        retry_path:Path = Path("%s\\retry.dat"%(dirname(abspath(__file__))))
        with open(retry_path, "w") as retry:
            retry.writelines(parent_recvs)
        print("Retrying %i coordinates..."%(len(parent_recvs)))
        main(retry_path)
        if (isfile(retry_path)):
            remove(retry_path)
    
    print("execution time: %f" %(time()-start_time))
            
if __name__ == '__main__':
    main(Path("SP.dat"))
