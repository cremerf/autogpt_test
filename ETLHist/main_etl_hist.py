from dask.distributed import Client, LocalCluster
#from dask_cuda import LocalCUDACluster
from available_activities import available_activities
import os


###### EXECUTE THIS MODULE ONLY IF HISTORICAL DATA IS NEEDED TO BE LOADED ######

def main_run():

    available_activities()

if __name__ == '__main__':

    from multiprocessing import freeze_support
    freeze_support()
    
    cluster = LocalCluster(n_workers=2, threads_per_worker=4, memory_limit='5GB')
    client = Client(cluster)

    print(client.dashboard_link)
    main_run()


#cluster.scale(10)  
#cluster.adapt(minimum=1, maximum=10) 

