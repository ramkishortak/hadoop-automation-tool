#!/usr/bin/python2

import os,time,sys
import getpass
import commands
import threading

os.system("dialog --infobox 'Welcome TO my hadoop project' 3 35")
time.sleep(0.5)
os.system("dialog --backtitle '\t\t\t\t\t\t\t\t\tWelcome to the world of Hadoop ' --title 'WELCOME' --infobox 'Starting Project' 3 35")
time.sleep(0.8)
os.system("dialog --backtitle '\t\t\t\t\t\t\t\t\tWelcome to the world of Hadoop ' --title 'WELCOME' --infobox 'Starting Project.' 3 35")
time.sleep(0.8)
os.system("dialog --backtitle '\t\t\t\t\t\t\t\t\tWelcome to the world of Hadoop ' --title 'WELCOME' --infobox 'Starting Project..' 3 35")
time.sleep(0.8)
os.system("dialog --backtitle '\t\t\t\t\t\t\t\t\tWelcome to the world of Hadoop ' --title 'WELCOME' --infobox 'Starting Project...' 3 35")
time.sleep(0.8)
os.system("dialog --backtitle '\t\t\t\t\t\t\t\t\tWelcome to the world of Hadoop ' --title 'WELCOME' --infobox 'Starting Project....' 3 35")
time.sleep(0.8)
def hive():
	os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The ip of Client...'  8 40   2>/rkproject/clientip.txt")
	j=open("/rkproject/clientip.txt")
	g=j.read()
	j.close()
	os.system("ssh %s tar -xzvf /root/Desktop/apache-hive-0.13.1-bin.tar.gz "%g)
	os.system("ssh %s mv apache-hive-0.13.1-bin  /hive "%g)
	h = open('/root/.bashrc','a')
	h.write('''
export HIVE_HOME=/hive/
export PATH=/hive/bin:$PATH
''')
	h.close()
	os.system("scp /root/.bashrc root@%s:/root/"%g)

#	os.system('''ssh %s cat  >> /root/.bashrc ; export HIVE_HOME=/hive/
#export PATH=/hive/bin:$PATH '''%g)
#pig should be on client

def pig():
	os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The ip of Client...'  8 40   2>/rkproject/clientip.txt")
	j=open("/rkproject/clientip.txt")
	c=j.read()
	j.close()
	os.system("ssh %s tar xzvf /root/Desktop/pig-0.12.1-bin.tar.gz "%c)
		
	os.system("ssh %s mv pig-0.12.1 pig "%c)
	os.system("ssh %s mv pig / "%c)
	p = open('/root/.bashrc','a')
	p.write('''
export PIG_HOME=/pig/
export PATH=/pig/bin:$PATH
''')
	p.close()
	os.system("scp /root/.bashrc root@%s:/root/"%c)	

def sqoop():
	os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The ip of Client...'  8 40   2>/rkproject/clientip.txt")
	j=open("/rkproject/clientip.txt")
	c=j.read()
	j.close()
	os.system("ssh %s tar xzvf /root/Desktop/sqoop-1.4.5.bin__hadoop-1.0.0.tar.gz "%c)
		
	os.system("ssh %s sqoop-1.4.5.bin__hadoop-1.0.0 sqoop "%c)
	os.system("ssh %s sqoop / "%c)
	p = open('/root/.bashrc','a')
	p.write('''
export SQOOP_HOME=/sqoop/
export PATH=/sqoop/bin:$PATH
''')
	p.close()
	os.system("scp /root/.bashrc root@%s:/root/"%c)	




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
		os.system('ssh %s  "hadoop-daemon.sh stop datanode ; hadoop-daemon.sh stop tasktracker ; hadoop-daemon.sh start datanode ; hadoop-daemon.sh start tasktracker ; /usr/java/jdk1.7.0_51/bin/jps > /dev/null"'%g)
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
	os.system("dialog --backtitle 'HADOOP' --title 'USERNAME' --inputbox 'Enter Username' 10 50   2>/rkproject/uname.txt")
	os.system("dialog --backtitle 'HADOOP' --title 'PASSWORD' --insecure --passwordbox 'Enter Password' 10 50   2>/rkproject/passwd.txt")
	f=open("/rkproject/uname.txt")
	a=f.read()
	f.close()
	f1=open("/rkproject/passwd.txt")
	b=f1.read()
	f1.close()

	if a=="":
		if b=="":
			while True :
				os.system("dialog --backtitle 'Home Page' --title 'MENU' --menu 'Enter Your Choice..' 18 75 7 1 'Make Hadoop Cluster Custom ' 2 'Make Hadoop Cluster Automatic' 3 'Advance options(Hive,Pig,Sqoop,)' 4 'Upload File In Hadoop Cluster' 5 'Shut Down the all computers in just one click '  6 'Set Space quota' 7 'Exit' 2> /rkproject/choice.txt")
				w=open("/rkproject/choice.txt")
				g=w.read()
				w.close()
				if g=="1" :
					os.system("dialog --backtitle 'Custom' --title 'MENU' --menu 'Choose one Option' 25 75 7 1 'Make Namenode and Jobtracker ' 2 'get the full detail of connected computers' 3 'Make Datanode and Tasktracker' 4 'Start All Services' 5 'Stop All Services' 6 'Make Client' 7 'Back to Main Menu'  2> /rkproject/choice1.txt")
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
						os.system("dialog --infobox 'Fetching Information...' 3 40")
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
						commands.getoutput("nmap -sP  192.168.122.0/24 | grep 192 |  cut  -d: -f 2 | cut -c 22-36 > /root/Desktop/ip.txt")
						f=open("/root/Desktop/ip.txt")
						n=f.read()
						f.close()	
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
						os.system("dialog --infobox 'Be Patient We are Processing.' 3 40")
						time.sleep(1.5)
						os.system("dialog --infobox 'Be Patient We are Processing..' 3 40")
						time.sleep(1.5)
						os.system("dialog --infobox 'Be Patient We are Processing...' 3 40")
						time.sleep(1.5)
						os.system("dialog --infobox 'Be Patient We are Processing....' 3 40")	
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
					if l=="4" :
						os.system("dialog --infobox 'Just a Moment...' 4 40")
						os.system('start-all.sh > /dev/null')					
						os.system("dialog --infobox 'All Services Have Been Started...' 4 40")
						time.sleep(2.0)
					if l=="5" :
						os.system("dialog --infobox 'Just a Moment...' 4 40")
						os.system('stop-all.sh > /dev/null')					
						os.system("dialog --infobox 'All Services Have Been Stopped...' 4 40")
						time.sleep(2.0)	
					if l=="6":					
						os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The ip of Client...'  8 40   2>/rkproject/clientip.txt")
						j=open("/rkproject/clientip.txt")
						g=j.read()
						j.close()
						
						os.system('scp /rkproject/hdfs-site.xml /rkproject/core-site.xml /rkproject/mapred-site.xml root@%s:/etc/hadoop/ > /dev/null'%g)
						os.system('ssh %s  "hadoop-daemon.sh stop datanode > /dev/null; hadoop-daemon.sh stop tasktracker > /dev/null; "'%g)
						os.system("dialog --infobox 'Client is now Ready To Use...' 8 40")
						time.sleep(2.0)
						import index2
						index2.menu()
					if l=="7":
						os.system("dialog --backtitle 'HADOOP' --title 'WARNING' --inputbox  'are you sure  Y/N'  5 50   2>/rkproject/deci.txt")
						f2=open("/rkproject/deci.txt")
						d=f2.read()
						f2.close()
						if d=="y" or d=="Y" or d=="yes" or d=="YES" or d=="Yes":
							import index2
							index2.menu()
						#exit()
					else:
						continue
				if g=="3":
					os.system("dialog --backtitle 'Custom' --title 'MENU' --menu 'Choose one Option' 25 75 4 1 'Setup Hive Framework ' 2 'Setup Pig Framework' 3 'Setup sqoop Framework'   4 'Back to Main Menu'  2> /rkproject/choice1.txt")
					q=open("/rkproject/choice1.txt")
					j=q.read()
					q.close()
					if j=="1" :
						os.system("dialog --infobox 'Please wait while loading...' 4 40 ")
						hive()
						os.system("dialog --infobox 'Setup of Hive is successfully done...' 4 40 ")
						time.sleep(2.0)
					if j=="2" :
						
						os.system("dialog --infobox 'Please wait while loading...' 4 40 ")
						pig()
						os.system("dialog --infobox 'Setup of Pig is successfully done...' 4 40 ")
						time.sleep(2.0)
						
					if j=="3" :
						
						os.system("dialog --infobox 'Please wait while loading...' 4 40 ")
						sqoop()
						os.system("dialog --infobox 'Setup of sqoop is successfully done...' 4 40 ")
						time.sleep(2.0)
					

					
				if g=="2":
					import typ_inst
					typ_inst.login()
				if g=="7" :
					os.system("dialog --backtitle 'HADOOP' --title 'WARNING' --inputbox  'Are you sure you want to Exit..?  Y/N'  5 50   2>/tmp/decision.txt")
					f2=open("/tmp/decision.txt")
					d=f2.read()
					f2.close()
					if d=="y" or d=="Y" or d=="yes" or d=="YES" or d=="Yes":
						exit()
					else:
						continue
				if g=="4":
					os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The Path of the file u wana upload...'  8 60   2>/rkproject/filepath.txt")
					os.system("dialog --infobox 'Uploading file...3' 4 40 ")
					time.sleep(1.0)
					os.system("dialog --infobox 'Uploading file...2' 4 40 ")
					time.sleep(1.0)
					os.system("dialog --infobox 'Uploading file...1' 4 40 ")
					time.sleep(1.0)
					os.system("dialog --infobox 'Uploading file...0' 4 40 ")
					time.sleep(1.0)
					j=open("/rkproject/filepath.txt")
					z=j.read()
					j.close()
					os.system('hadoop fs -put %s /'%z)
					os.system("dialog --infobox 'File Uploaded Successful' 4 40 ")
					time.sleep(2.0)
				if g=="5" :
					
			
					k=commands.getoutput("ifconfig  virbr0 | grep 'inet addr' | awk '{print $2}' | cut -f2 -d:")
			
					d=commands.getoutput("nmap -sP --exclude %s 192.168.122.0/24 | grep 192 | cut -d : -f 2 | cut -c 22-36"%k)
					ip=d.split('\n')
					s=0
					for i in ip :
						os.system("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s init 0"%i)
						s += 1	
					exit()	
				if g=="6" :
					os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The ip of Namenode'  8 40   2>/rkproject/nnnew.txt")
					j=open("/rkproject/nnnew.txt")
					g=j.read()
					j.close()
					os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The SIZE QUOTA'  8 40   2>/rkproject/nnnew.txt")
					k=open("/rkproject/nnnew.txt")
					r=k.read()
					k.close()
					os.system("dialog --backtitle 'HADOOP'  --inputbox  'Enter The address of directory u wana set QUOTA...'  8 40   2>/rkproject/nnnew.txt")
					l=open("/rkproject/nnnew.txt")
					u=l.read()
					l.close()
					
					os.system("ssh %s hadoop dfsadmin -setSpaceQuota %s   %s"%(g,r,u))
					

menu()	

