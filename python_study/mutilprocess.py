import multiprocessing

def worker(num):
	#"""thread worker function"""
	print 'dowork',num
	return
    
if __name__ == '__name__':
	jobs = []
	for i in range(5):
		p = mutilprocessing.Process(target=worker,args = (i,))
		jobs.append(p)
		p.start()
		p.join()