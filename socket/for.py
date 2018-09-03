# -*-coding:utf-8 -*-
for num in range(10,20):
    for num2 in range(2,num):
	    if num%num2==0:
		    j=num/num2
		    print '%d 等于 %d*%d' %(num,num2,j)
		    break
    else:
	    print num,'是质数'