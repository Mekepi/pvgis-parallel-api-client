import requests
import multiprocessing as m
import time

def request_hourlydata(line:str):
    line = line.split(",")
    line = [float(line[0]),float(line[1]),int(line[2]),int(line[3])]
    try:
        f = open("hourlydata(%f,%f)[%i,%i].csv"%(line[0],line[1],line[2],line[3]), "x+b")
    except FileExistsError as err:
        print(err)
    else:
        request_start = time.monotonic()
        f.write(requests.get("https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?lat=%f&lon=%f&startyear=%i&endyear=%i" %(line[0],line[1],line[2],line[3])).content)
        print("Request", m.current_process().name.rsplit("-")[1] ,"duration:", time.monotonic()-request_start)
        f.close()

def pvgis_5_2_hourlydata(file):

        start_time = time.time()

        try:
            inputs = open(file, "r")

        except FileNotFoundError as err:
            print(err)

        else:
            processes = list(m.Process(target=request_hourlydata, args=[line]) for line in inputs.readlines())
            inputs.close()

            total_request_delay = 0.04*len(processes)
            for process in processes:
                process.start()
                time.sleep(0.04)
            
            print("  Sleep duration:", total_request_delay)
            print("   Until joining:", time.time()-start_time)
            joining_start = time.time()
            for process in processes:
                process.join()
                process.close()
            print("Joining duration:", time.time()-joining_start)
            total_request_delay += time.time()-joining_start

        print("total request delay: %f\nexecution time: %f" %(total_request_delay, time.time()-start_time))
            
if __name__ == '__main__':
    pvgis_5_2_hourlydata("inputs2.dat")
