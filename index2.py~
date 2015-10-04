#!/usr/bin/python2

import os,time,sys
import getpass
import commands
import threading

os.system("dialog --infobox 'Welcome TO my hadoop project' 7 30")
time.sleep(0.5)
os.system("dialog --backtitle '\t\t\t\t\t\t\t\t\tWelcome to the world of Hadoop ' --title 'WELCOME' --infobox 'Starting Project...' 10 30")
time.sleep(0.8)
def lw(i) :
		o=open("/rkproject/ips.txt")
		k=o.read()
		o.close()	
 		ipp=k.split('\n')
		#print g
		#type(g)
		#print("\n\n")
		#print k
		#type(k)
		print i
		g=ipp[int(i)]
		os.system('scp /rkproject/hdfs-site.xml /rkproject/core-site.xml /rkproject/mapred-site.xml root@%s:/etc/hadoop/ > /dev/null'%g)
		os.system('ssh %s  "hadoop-daemon.sh stop datanode ; hadoop-daemon.sh stop tasktracker hadoop-daemon.sh start datanode ; hadoop-daemon.sh start tasktracker ; /usr/java/jdk1.7.0_51/bin/jps > /dev/null"'%g)
		exit()


def nnjt() :
	ipn=open("/rkproject/ipname.txt")
	ipnn=ipn.read()
	ipn.close()
	ipj=open("/rkproject/ipjob.txt")
	ipjt=ipj.read()
	ipj.close()						
	f=open('/rkproject/hdfs-site.xml','w')
	f.truncate()
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
<!-- Put site-specific property overrides in this file. -->
	
<configuration>
<property>
<name>dfs.name.dir</name>
<value>/ram</value>
</property>
</configuration>''')
	f.close()
	
	
	f=open('/rkproject/core-site.xml','w')
	f.truncate()
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
<!-- Put site-specific property overrides in this file. -->
	
<configuration>
<property>
<name>fs.default.name</name>
<value>%s:9001</value>
</property>
</configuration>'''%ipnn)
	f.close()

	f=open('/rkproject/mapred-site.xml','w')
	f.truncate()
	f.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
<!-- Put site-specific property overrides in this file. -->
	
<configuration>
<property>
<name>mapred.job.tracker</name>
<value>%s:9002</value>
</property>
</configuration>'''%ipjt)
	f.close()

	os.system('scp /rkproject/hdfs-site.xml /rkproject/core-site.xml /rkproject/mapred-site.xml root@%s:/etc/hadoop/'%ipnn)
	os.system('scp /rkproject/hdfs-site.xml /rkproject/core-site.xml /rkproject/mapred-site.xml root@%s:/etc/hadoop/'%ipjt)
	os.system("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s   echo Y | hadoop namenode -format > /dev/null "%ipnn)
	os.system("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s   hadoop-daemon.sh stop namenode > /dev/null"%ipnn)	
	os.system("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s   hadoop-daemon.sh start namenode > /dev/null"%ipnn)
	os.system("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s   hadoop-daemon.sh stop jobtracker > /dev/null"%ipjt)
	os.system("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s   hadoop-daemon.sh start jobtracker > /dev/null"%ipjt)
	







def menu() :

			while True :
				os.system("dialog --backtitle 'Home Page' --title 'MENU' --menu 'Enter Your Choice..' 16 60 4 1 'Make Hadoop Cluster Custom ' 2 'Make Hadoop Cluster Automatic' 3 'Advance options(Hive,Pig,Sqoop,)' 4 'Exit' 2> /rkproject/choice.txt")
				w=open("/rkproject/choice.txt")
				g=w.read()
				w.close()
				if g=="1" :
					os.system("dialog --backtitle 'Custom' --title 'MENU' --menu 'Choose one Option' 16 60 5 1 'Make Namenode and Jobtracker ' 2 'get the full detail of connected computers' 3 'Make Datanode and Tasktracker' 4 'Start All Services' 5 'Stop All Services' 6 'Back to Main Menu'  2> /rkproject/choice1.txt")
					q=open("/rkproject/choice1.txt")
					l=q.read()
					q.close()
					if l=="1" :
						os.system("dialog --backtitle 'HADOOP' --title 'Namenode' --inputbox 'Enter the ip of namenode' 10 50   2>/rkproject/ipname.txt")
						os.system("dialog --backtitle 'HADOOP' --title 'Jobtracker' --inputbox 'Enter the ip of jobtracker' 10 50   2>/rkproject/ipjob.txt")					
						nnjt()
						exit()
					if l=="2" :
						
						i=open("/rkproject/allip.txt","w")
						i.write("S.no 		IP 			Hard Disk 		Free Ram\n\n")
						i.close()
						n=commands.getoutput("nmap -sP --exclude 192.168.122.1 192.168.122.0/24 | grep 192 | cut -d : -f 2 | cut -c 22-36 ")
						w=n.split('\n')
						s=0
						for i in w :
							ram=commands.getoutput("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s free -m | grep Mem | awk '{print $4}'"%i)
							disk=commands.getoutput("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s df --total -h | grep total | awk '{print$4}'"%i)
							iram=ram.split('\n')
							idisk=disk.split('\n')	
							ip=open("/rkproject/allip.txt","a")
							ip.write('''%s    	 %s 	     	%s			%s\n'''%(s,i,idisk[0],iram[0]))
							ip.close()	
							s+=1	
						os.system("dialog --textbox /rkproject/allip.txt  18 80")	
							
						exit()
					if l=="3" :
						os.system("dialog --infobox 'Be Patient We are Processing..' 3 34")
						i=open("/rkproject/allip.txt","w")
						i.write("S.no 		IP 			Hard Disk 		Free Ram\n\n")
						i.close()
						commands.getoutput("nmap -sP 192.168.122.0/24 | grep 192 | cut -d : -f 2 | cut -c 22-36 > /root/Desktop/ip.txt")
						v=open("/root/Desktop/ip.txt")
						n=v.read()
						v.close()	
						ip=n.split('\n')
						s=0
						for i in ip :
							ram=commands.getoutput("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s free -m | grep Mem | awk '{print $4}'"%i)
							disk=commands.getoutput("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s df --total -h | grep total | awk '{print$4}'"%i)
							iram=ram.split('\n')
							idisk=disk.split('\n')	
							ip=open("/rkproject/allip.txt","a")
							ip.write('''%s    	 %s 	     	%s			%s\n'''%(s,i,idisk[0],iram[0]))
							ip.close()	
							s+=1	
						os.system("dialog --textbox /rkproject/allip.txt  18 80")
						os.system("dialog --infobox 'Be Patient We are Processing.' 3 34")
						time.sleep(1.5)
						os.system("dialog --infobox 'Be Patient We are Processing..' 3 34")
						time.sleep(1.5)
						os.system("dialog --infobox 'Be Patient We are Processing...' 3 34")
						time.sleep(1.5)
						os.system("dialog --infobox 'Be Patient We are Processing....' 3 34")	
						time.sleep(1.5)
						os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The S.No of ip You Wana Make Datanode And Tasktracker'  8 40   2>/rkproject/sno.txt")
						sno=open("/rkproject/sno.txt")
						t=sno.read()
						sno.close()
						f=t.split()
						t=[]
						for i  in f  :
					
							temp = threading.Thread(target=lw,args=(i,))
							temp.start()
							t.append(temp)
						for i in t :
							i.join()
						os.system("dialog --infobox 'Datanodes And Tasktrackers Are Created Successfully...' 8 40")	
						exit()
					if l=="6":
						os.system("dialog --backtitle 'HADOOP' --title 'WARNING' --inputbox  'are you sure  Y/N'  5 40   2>/rkproject/deci.txt")
						f2=open("/rkproject/deci.txt")
						d=f2.read()
						f2.close()
						if d=="y" or d=="Y" or d=="yes" or d=="YES" or d=="Yes":
							import index2
							index2.menu()
						#exit()
					else:
						continue
				if g=="4" :
					exit()
menu()	

