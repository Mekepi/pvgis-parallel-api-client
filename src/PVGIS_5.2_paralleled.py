from urllib3 import request
from multiprocessing import Process#, current_process
from time import time, sleep#, monotonic
from  os.path import dirname, abspath

def request_hourlydata(line:list):
    line = [float(line[0]),float(line[1]),int(line[2]),int(line[3])]
    try:
        directory = dirname(abspath(__file__))
        f = open("%s\\data\\hourlydata(%f,%f)[%i,%i].csv"%(directory,line[0],line[1],line[2],line[3]), "x+b")
    except FileExistsError as err:
        print(err)
    else:
        #request_start = monotonic()
        while (len(open("%s\\data\\hourlydata(%f,%f)[%i,%i].csv"%(directory,line[0],line[1],line[2],line[3]), "r").readlines())<50):
            f.truncate(0)
            f.write(request("GET","https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%f&lon=%f&startyear=%i&endyear=%i" %(line[0],line[1],line[2],line[3]),preload_content=False,timeout=None).data)
        #print("Request", current_process().name.rsplit("-")[1] ,"duration:", monotonic()-request_start)
        f.close()
        #if (open("%s\\data\\hourlydata(%f,%f)[%i,%i].csv"%(directory,line[0],line[1],line[2],line[3]), "r").readlines()<50):


def main(file):

        start_time = time()

        try:
            inputs = open(file, "r")

        except FileNotFoundError as err:
            print(err)

        else:
            processes = list(Process(target=request_hourlydata, args=[line.split(",")]) for line in inputs.readlines())
            inputs.close()

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
            #print("Joining duration:", time()-joining_start)
            total_request_delay += time()-joining_start

        print("total request delay: %f\nexecution time: %f" %(total_request_delay, time()-start_time))
            
if __name__ == '__main__':
    main("ES.dat")
