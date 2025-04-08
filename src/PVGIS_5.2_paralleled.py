from urllib3 import request
from multiprocessing import Process, Pipe
from time import time, sleep
from  os import path, remove

def request_hourlydata(line:str, con):
    line = [float(line[0]),float(line[1]),int(line[2]),int(line[3])]
    try:
        directory = path.dirname(path.abspath(__file__))
        f = open("%s\\data\\hourlydata(%f,%f)[%i,%i].csv"%(directory,line[0],line[1],line[2],line[3]), "x+b")
    except FileExistsError as err:
        print(err)
    else:
        f.write(request("GET","https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%f&lon=%f&startyear=%i&endyear=%i" %(line[0],line[1],line[2],line[3]),preload_content=False,timeout=None).data)
        f.close()
        if (open("%s\\data\\hourlydata(%f,%f)[%i,%i].csv"%(directory,line[0],line[1],line[2],line[3]), "r").readline().rsplit(":")[0]!="Latitude (decimal degrees)"):
            remove("%s\\data\\hourlydata(%f,%f)[%i,%i].csv"%(directory,line[0],line[1],line[2],line[3]))
            con.send(','.join(str(el) for el in line))



def main(file:str):

        start_time = time()

        try:
            inputs = open(file, "r")

        except FileNotFoundError as err:
            print(err)

        else:
            lines = inputs.readlines()
            inputs.close()
            parent_cons, child_cons = zip(*map(lambda _: Pipe(False), range(len(lines)))) 
            processes = list(map(lambda l, c: Process(target=request_hourlydata, args=[l.split(","),c]), lines, child_cons))
            
            total_request_delay = 0.03334*len(processes)
            for process in processes:
                process.start()
                sleep(0.03334)
            print("  Sleep duration:", total_request_delay)

            print("   Until joining:", time()-start_time)
            joining_start = time()
            for process in processes:
                process.join()
                process.close() 
            print("Joining duration:", time()-joining_start)
            
            retry_dir = "%s\\retry.dat"%(path.dirname(path.abspath(__file__)))
            retry = open(retry_dir, "w")
            retry.truncate(0)
            retry.writelines(list(map(lambda c: "%s\n"%(c.recv()),filter(lambda p: p.poll(),parent_cons))))
            retry.close()
            retry = open(retry_dir,"r")
            if (len(retry.readlines())>0):
                main(retry_dir)
            retry.close()
            total_request_delay += time()-joining_start

        print("total request delay: %f\nexecution time: %f" %(total_request_delay, time()-start_time))
            
if __name__ == '__main__':
    main("SP.dat")
