'''
This is a wrapper class written around multiprocessing
which can be used to call any function which needs to be called parallel
'''
import multiprocessing
import time
import random
import string

__author__ = 'pashah'

class ParallelProcess(object):
    '''
    Do MultiProcessing
    '''
    def __init__(self,multiprocess_count,multi_func,*args,**kwargs):
        '''
        constructor
        @multiprocess_count - How many Process Users want to Create
        @multi_func - Function Object
        @args - Arguments for Function
        @kwargs - Key Word Arguments for Function
        '''
        self.multiprocess_count = None
        self.multi_func_obj = None
        self.error_queue = {}
        self.args = None
        self.kwargs = None
        self.process_jobs = []
        self.jobs_pid = []
        self.all_process_completed = 0

        self.multiprocess_count = multiprocess_count
        self.multi_func_obj = multi_func
        self.args = args
        self.kwargs = kwargs
        for num in range(1,self.multiprocess_count+1):
            self.error_queue[num] = multiprocessing.Queue()

    def __del__(self):
        '''
        destructor
        '''
        self.__exit__()

    def __enter__(self):
        '''
            enter function
        '''
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        '''
        Exit method for context manager
        '''
        # close error queues
        try:
            self.cleanup_queue()
        except Exception, e:
            pass

        # clean parallel processes
        try:
            self.cleanup_multiprocess()
        except Exception, e:
            pass

    def cleanup_queue(self):
        '''
        Close all queues created to detect errors
        '''
        if self.error_queue is not None:
            for key in self.error_queue.iterkeys():
                self.error_queue[key].close()

    def get_random_string(self,count=10):
        '''
        return generate random string
        @count - characters in string
        '''
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(count))

    def call_multiprocess(self):
        '''
        Call multiprocessing
        '''
        for counter in range(1,self.multiprocess_count+1):
            process_name = 'MultiProcess_{0}_{1}'.format(self.get_random_string(),counter)
            child_process = multiprocessing.Process(target=self.multi_func_obj,name=process_name, args=(counter,self.error_queue[counter],self.args),kwargs=self.kwargs)
            self.process_jobs.append(child_process)
            child_process.start()
            self.jobs_pid.append(child_process.pid)

    def verify_multiprocessing_error(self):
        '''
        Call this function to verify whether any error has been raised in multiprocessing function
        '''
        error_result = None
        if self.error_queue is not None:
            for key in self.error_queue.iterkeys():
                if not self.error_queue[key].empty():
                    error_code = self.error_queue[key].get()
                    if error_code:
                        error_result = self.error_queue[key].get()
                        self.all_process_completed = self.multiprocess_count
                        raise Exception(error_result)
                    else:
                        self.all_process_completed += 1

    def wait_till_process_complete(self):
        '''
        This will wait for Process to Complete.
        In Error Queue its excepting 0 to successfully finish
        1 to raise an error
        '''
        while self.all_process_completed < self.multiprocess_count:
            self.verify_multiprocessing_error()
            time.sleep(0.5)

    def cleanup_multiprocess(self):
        '''
        Clean Multiprocess jobs
        '''
        # Terminate all jobs
        # Sometimes it has been observed that child process still becomes zombie
        # So always terminate and then join
        try:
            if len(self.process_jobs) != 0:
                for job in self.process_jobs:
                    try:
                        while job.is_alive():
                            os.system('kill -9 ' + str(job.pid) + ' > /dev/null 2>&1')
                    except Exception, e:
                        pass
                    finally:
                        time.sleep(0.1)
                        logging.info('multiprocessing Joining job [%s]'%(job))
                        job.join()
                        logging.info('multiprocessing Joined job [%s]'%(job))
        except Exception, e:
            pass

        try:
            self.kill_jobs()
        except Exception, e:
            pass

    def kill_jobs(self):
        '''
            Kill Jobs
        '''
        if self.jobs_pid is not None:
            for job_pid in self.jobs_pid:
                try:
                    os.system('kill -9 ' + str(job_pid) + ' > /dev/null 2>&1')
                    time.sleep(0.1)
                except Exception, e:
                    pass

    '''
    # This is how function must be wrote to take advantage of above
    import traceback

    def paralle_task(self,custom_id,error_queue,*args,**kwargs):
        try:
            # Do Your Action
            error_queue.put(0)
        except Exception, e:
            # To make sure function has raised an error and flag is set
            error_queue.put(1)
            err = "\n---------Traceback-----------\n{0}\n----------Error-----------\n{1}-------------------\n".format(traceback.format_exc(),e)
            # This puts actual error inside queue
            error_queue.put(err)
        finally:
            # anything user wanted to do it
    '''
