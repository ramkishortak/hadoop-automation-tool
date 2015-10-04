#!/usr/bin/python2

import threading
import os
import commands
import getpass
def lw(i) :
		print i		
 		g=ip[int(i)]
		os.system('scp /root/Desktop/hdfs-site.xml /root/Desktop/core-site.xml /root/Desktop/mapred-site.xml root@%s:/etc/hadoop/'%g)
		os.system('ssh %s  "hadoop-daemon.sh start datanode ; hadoop-daemon.sh start tasktracker ; /usr/java/jdk1.7.0_51/bin/jps"'%g)
		exit()
x=raw_input('enter username\n')
		
s=getpass.getpass('enter password')
		
if x == 'root' and s == 'q'  :
	
		print '''\t\t 1.create NAMENODE
		 2.CREATE DATANODE
		 3.CREATE JOBTRACKER
		 4.CREATE TASKTRACKER 
		 5.Make Custom Datanodes And Tasktrackers 
		 6.Shut-Off Cluster
		 7.Exit 
		 '''


		c = input()
		s=commands.getoutput("ifconfig  wlan0 | grep 'inet addr' | awk '{print $2}' | cut -f2 -d:")
		 
		if int(c) == 1 :
	
			f=open('/etc/hadoop/hdfs-site.xml','w')
			f.truncate()
			f.writelines('''<?xml version="1.0"?>
		<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
		<!-- Put site-specific property overrides in this file. -->
	
		<configuration>
		<property>
		<name>dfs.name.dir</name>
		<value>/ram</value>
		</property>
		</configuration>
		''')
			f.close()
		
		# for coresite.xml
			n=open('/etc/hadoop/core-site.xml','w')
			n.truncate()

			print s
			n.writelines('''<?xml version="1.0"?>
		<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
		<!-- Put site-specific property overrides in this file. -->
	
		<configuration>
		<property>0.0
0	0	<name>fs.default.name</name>
		<value>hdfs://%s:9001</value>
		</property>
		</configuration>
		'''%s)
			n.close()
			#now the format and start commands
			os.system('hadoop-daemon.sh stop namenode')
			os.system('hadoop namenode -format -y')
			os.system('hadoop-daemon.sh start namenode')
			
			
		elif int(c) == 2 :
	
			f=open('/etc/hadoop/hdfs-site.xml','w')
			f.writelines('''<?xml version="1.0"?>
		<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
		<!-- Put site-specific property overrides in this file. -->
	
		<configuration>
		<property>
		<name>dfs.data.dir</name>
		<value>/ram2</value>
		</property>
		</configuration>''')
			f.close()
		# for coresite.xml
			n=open('/etc/hadoop/core-site.xml','w')
			n.writelines('''<?xml version="1.0"?>
		<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
		<!-- Put site-specific property overrides in this file. -->
	
		<configuration>
		<property>
		<name>fs.default.name</name>
		<value>hdfs://%s:9001</value>
		</property>
		</configuration>
		'''%s)
			n.close()
		#commands
			os.system('hadoop-daemon.sh stop datanode')
			os.system('hadoop-daemon.sh start datanode')
			os.system('jps')
		elif int(c) == 3 :
			f=open('/etc/hadoop/mapred-site.xml','w')
			f.truncate()
			f.writelines('''<?xml version="1.0"?>
		<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
		<!-- Put site-specific property overrides in this file. -->
	
		<configuration>
		<property>
		<name>df.job.tracker</name>
		<value>%s:9002</value>
		</property>
		</configuration>
		'''%s)
			f.close()
			os.system('hadoop jobtracker -format')
			os.system('hadoop-daemon.sh start jobtracker')
			os.system('jps')
		elif int(c) == 4 :
			f=open('/etc/hadoop/mapred-site.xml','w')
			f.truncate()
			f.writelines('''<?xml version="1.0"?>
		<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	
		<!-- Put site-specific property overrides in this file. -->
	
		<configuration>
		<property>
		<name>df.job.tracker</name>
		<value>%s:9002</value>
		</property>
		</configuration>
		'''%s)
			f.close()
			os.system('hadoop-daemon.sh start tasktracker')
			os.system('jps')
		
		elif int(c) == 5 :
			d=commands.getoutput("nmap -sP 192.168.122.0/24 | grep 192 | cut -d : -f 2 | cut -c 22-36")
			ip=d.split('\n')
	#	print ip
	#	print('\n\n')		
	#	print ip[1 : 3]	
			s=0
			print '\n\n\t\tS.No\tIP ADDRESS\t\tRAM\tHard Disk\n'
			for i in ip :
				ram=commands.getoutput("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s free -m | grep Mem | awk '{print $4}'"%i)
				disk=commands.getoutput("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s df --total -h | grep total | awk '{print $4}'"%i)
				
				
				print "\t\t%d - %s              %s           %s"%(s,i,ram,disk) 
#				os.system('sleep 1')
				s += 1
			
			j=raw_input('Enter the S.no. of ip You wana Make datanode & tasktracker')
			f=j.split('\t')
			t=[]
			for i  in f  :
					
				temp = threading.Thread(target=lw,args=(i,))
				temp.start()
				t.append(temp)
			for i in t :
				i.join()
				
					

		elif int(c) == 7 :
			exit()
		elif int(c) == 6 :
			
			k=commands.getoutput("ifconfig  virbr0 | grep 'inet addr' | awk '{print $2}' | cut -f2 -d:")
			
			d=commands.getoutput("nmap -sP --exclude %s 192.168.122.0/24 | grep 192 | cut -d : -f 2 | cut -c 22-36"%k)
			ip=d.split('\n')
			s=0
			for i in ip :
				os.system("sshpass -p 'redhat' ssh -o StrictHostKeyChecking=no root@%s init 0"%i)
				s += 1	
			exit()	
				
		else :
			print"sahi number daal de "

	

