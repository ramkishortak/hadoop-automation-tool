#!/usr/bin/python2

import os,time,sys
import commands
import getpass
import thread
import operator

#for namenode
def n_hdfs(k):
		os.system("sshpass -p redhat ssh -o StrictHostKeyChecking=no %s rm -rf /namenode;   mkdir /namenode ; mkdir -p /root/Desktop/typical/{datanode,namenode};"%k)
		nn = open('/root/Desktop/typical/namenode/hdfs-site.xml','w')
		nn.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>dfs.name.dir</name>
<value>/namenode</value>
</property>
</configuration>
''')
		nn.close()
		os.system("scp /root/Desktop/typical/namenode/hdfs-site.xml  root@%s:/etc/hadoop/    > /dev/null"%k)

def n_core(k):
		nc = open('/root/Desktop/typical/namenode/core-site.xml','w')
		nc.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://%s:9001</value>
</property>
</configuration>
'''%k)
		nc.close()
		os.system("scp /root/Desktop/typical/namenode/core-site.xml  root@%s:/etc/hadoop/    > /dev/null"%k)

def n_mapred(k):
		nj = open('/root/Desktop/typical/namenode/mapred-site.xml','w')
		nj.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>mapred.job.tracker</name>
<value>%s:9002</value>
</property>

</configuration>
'''%k)
		nj.close()
		os.system("scp /root/Desktop/typical/namenode/mapred-site.xml  root@%s:/etc/hadoop/   > /dev/null"%k)

###############
#for datanode
def d_hdfs(q):
		#os.system("sshpass -p redhat ssh -o StrictHostKeyChecking=no %s  mkdir /datanode"%k)
		nn = open('/root/Desktop/typical/datanode/hdfs-site.xml','w')
		nn.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>dfs.data.dir</name>
<value>/datanode</value>
</property>
</configuration>
''')
		nn.close()
		#os.system("scp /root/Desktop/typical/datanode/hdfs-site.xml  root@%s:/etc/hadoop/"%k)

def d_core(q):
		nc = open('/root/Desktop/typical/datanode/core-site.xml','w')
		nc.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://%s:9001</value>
</property>
</configuration>
'''%q)
		nc.close()
#		os.system("scp /root/Desktop/typical/datanode/core-site.xml  root@%s:/etc/hadoop/"%k)

def d_mapred(q):
		nj = open('/root/Desktop/typical/datanode/mapred-site.xml','w')
		nj.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>mapred.job.tracker</name>
<value>%s:9002</value>
</property>

</configuration>
'''%q)
		nj.close()
		#os.system("scp /root/Desktop/typical/datanode/mapred-site.xml  root@%s:/etc/hadoop/"%k)

###########

def n_start(k):
	os.system("dialog --infobox 'processing please wait...' 3 34")
	os.system("ssh %s iptables -F > /dev/null"%k)
	os.system("ssh %s setenforce 0 > /dev/null"%k)
	os.system("ssh %s service iptables save > /dev/null"%k)
	os.system("ssh %s echo Y | hadoop namenode -format > /dev/null"%k)
	os.system("dialog --infobox 'Almost done...' 3 34")
	os.system("ssh %s hadoop-daemon.sh stop namenode > /dev/null"%k)
	os.system("ssh %s hadoop-daemon.sh start namenode > /dev/null"%k)
	os.system("ssh %s hadoop-daemon.sh stop jobtracker > /dev/null"%k)
	os.system("ssh %s hadoop-daemon.sh start jobtracker > /dev/null"%k)


def d_start(k):
	#os.system("dialog --infobox 'processing please wait...' 3 34")
	os.system("ssh %s iptables -F  > /dev/null"%k)
	os.system("ssh %s setenforce 0 > /dev/null"%k)
	os.system("ssh %s service iptables save  > /dev/null"%k)
	os.system("ssh %s hadoop-daemon.sh stop datanode > /dev/null"%k)
	os.system("ssh %s hadoop-daemon.sh start datanode > /dev/null"%k)
	os.system("ssh %s hadoop-daemon.sh stop tasktracker > /dev/null"%k)
	os.system("ssh %s hadoop-daemon.sh start tasktracker > /dev/null"%k)


def copy(k):
	os.system("ssh %s  rm -rf /datanode  > /dev/null"%k)
	os.system("ssh %s  mkdir /datanode  > /dev/null"%k)
	os.system("scp /root/Desktop/typical/datanode/hdfs-site.xml  root@%s:/etc/hadoop/    > /dev/null"%k)
	os.system("scp /root/Desktop/typical/datanode/core-site.xml  root@%s:/etc/hadoop/   > /dev/null"%k)
	os.system("scp /root/Desktop/typical/datanode/mapred-site.xml  root@%s:/etc/hadoop/   > /dev/null"%k)


#os.system("dialog --backtitle 'HADOOP' --title 'WELCOME' --infobox 'Welcome To My Hadoop Project.' 10 30")
#time.sleep(1.0)
def login():
	 
			while True:

				os.system("dialog --backtitle 'HADOOP' --title 'MENU' --menu 'select a option' 12 50 2 1 'create automatical cluster' 2 'go back to main menu' 2>/tmp/menu.txt")
				m=open("/tmp/menu.txt")
				ch=m.read()
				m.close()
				#print type(ch)
				if ch=="1":
				
#find netid of current computer		
					os.system("dialog --infobox 'processing please wait...' 3 34")	
					net=commands.getoutput("ifconfig virbr0 | grep 192 | awk '{print$2}'|cut -c 6-20")
					net=net+'/24'
					netip=commands.getoutput("nmap -sP 192.168.122.0/24 | grep 192 | awk '{print$5}'")
					netiplist=netip.split('\n')
		
							#find total ram memory of each active ip in the network
					memo=dict() #dictionary containing memory info of all active ips
					#print "hi"
					for item in netiplist: 
						if item=='192.168.109.1' or item=='192.168.109.2' or item=='192.168.109.254':
							continue
						x=commands.getoutput("ssh %s  free -m | grep Mem | awk '{print$2}'"%item)
						memo[item]=x 
	

					sorted_x = sorted(memo.items(), key=operator.itemgetter(1))

					nn_ip = sorted_x[0][0]
					k=nn_ip
					#print k
					n_hdfs(k)
					n_core(k)	
					n_mapred(k)
					n_start(k)
					d_hdfs(k)
					d_core(k)	
					d_mapred(k)

					i=0
					for df in sorted_x:
						i+=1
					#print i	


					j=1
					for fd in sorted_x:
						if j<i:
							print sorted_x[j][0]
							k=sorted_x[j][0]
							thread.start_new_thread(copy,(k,))
							thread.start_new_thread(d_start,(k,))
							j+=1
				elif ch=="2":
					os.system("dialog --backtitle 'HADOOP' --title 'WARNING' --inputbox  'are you sure  Y/N'  5 40   2>/tmp/decision.txt")
					f2=open("/tmp/decision.txt")
					d=f2.read()
					f2.close()
					if d=="y" or d=="Y" or d=="yes" or d=="YES" or d=="Yes":
						import index2
						index2.menu()
						#exit()
					else:
						continue
				elif ch=="":
					os.system("dialog --backtitle 'HADOOP' --title 'WARNING' --inputbox  'are you sure  Y/N'  5 40   2>/tmp/decision.txt")
					f2=open("/tmp/decision.txt")
					d=f2.read()
					f2.close()
					if d=="y" or d=="Y" or d=="yes" or d=="YES" or d=="Yes":
						exit()
					else:
						continue
					
		#	else:
		#		print "wrong choice"
		#else:
			#os.system("dialog --msgbox 'password is incorrect' 7 30")
			#login()
#	else:
#		os.system("dialog --msgbox 'username is incorrect' 7 30")
		#login()

#login()		



		
		


