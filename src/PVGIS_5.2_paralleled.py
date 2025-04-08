from urllib3 import request
from multiprocessing import Process, Pipe
from multiprocessing.connection import PipeConnection
from time import time, sleep
from  os import remove
from os.path import dirname, abspath

def request_hourlydata(coord:str, con:PipeConnection) -> None:
    line:list = [float(coord.split(",")[0]),float(coord.split(",")[1]),int(coord.split(",")[2]),int(coord.split(",")[3])]
    
    file:str = "%s\\data\\hourlydata(%.6f,%.6f)[%i,%i].csv"%(dirname(abspath(__file__)),line[0],line[1],line[2],line[3])
    try:
        f = open(file, "xb")
    except FileExistsError as err:
        print(err)
    except Exception as err:
        print(err)
        con.send(coord)
    else: 
        try:
            f.write(request("GET","https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%.6f&lon=%.6f&startyear=%i&endyear=%i" %(line[0],line[1],line[2],line[3]),preload_content=False,timeout=None).data)
        except Exception as err:
            f.close()
            remove(file)
            print(err)
            con.send(coord)
        else:
            f.close()
            with open(file, "r") as f:
                b:bool = f.readline().startswith("Latitude (decimal degrees):")
            if (not(b)):
                remove(file)
                con.send(coord)
    con.close()


def main(file:str) -> None:
    start_time = time()

    with open("%s\\%s"%(dirname(abspath(__file__)), file), "r") as inputs:
        lines:list[str] = inputs.readlines()

    parent_cons:map[PipeConnection]
    child_cons:map[PipeConnection]
    parent_cons, child_cons = zip(*map(lambda _: Pipe(False), range(len(lines))))
    processes:list[Process] = list(map(lambda line, con: Process(target=request_hourlydata, args=[line,con]), lines, child_cons))
    
    for process in processes:
        process.start()
        sleep(0.04)
    print("  Sleep duration:", 0.04*len(processes))

    for process in processes:
        process.join()
        process.close()
    print("Request duration:", time()-start_time)
    
    parent_recvs:list[str] = [con.recv() for con in parent_cons if con.poll()]
    if (parent_recvs):
        with open("%s\\retry.dat"%(dirname(abspath(__file__))), "w") as retry:
            retry.writelines(parent_recvs)
        print("Retrying %i coordinates..."%(len(parent_recvs)))
        main("retry.dat")

    print("execution time: %f" %(time()-start_time))
            
if __name__ == '__main__':
    main("inputs2.dat")
