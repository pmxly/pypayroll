import time

if __name__ == '__main__':

    start = time.time()
    for x in range(1, 80000):
        isPrime = True
        for i in range(2, x):
            if x % i == 0:
                break
        if isPrime:
            pass
    end = time.time()
    print("时间：" + str(end-start)[:5])
