import sys
import getopt
import socket
import subprocess
import os
import stat
import time

csv_rows=[]

def CSVwrite(filename,csvarray,fieldnames,ioflag,indexrow):
    ret=False
    try:
	import csv
	csvfile = open(filename,ioflag)
	csvwriter = csv.DictWriter(csvfile, delimiter=',', fieldnames=fieldnames)
	if indexrow==True:
	    csvwriter.writerow(dict((fn,fn) for fn in fieldnames))
	for row in csvarray:
            #print row
    	    csvwriter.writerow(row)
	csvfile.close()
	ret=True
    except:
	print 'CSVwrite except'
	pass
    return ret

def createHostsfile():
    f = open("hosts", "w")
    for row in csv_rows:
        #scp -i "ai_tce.pem" scripts/instances.txt  ubuntu@ec2-18-191-197-117.us-east-2.compute.amazonaws.com:/home/ubuntu/
        # CSVFile
        # AWS : need to use private ip instead of public ip for yarn cluster
        host = row['PrivateIP'] + "   " + row['NodeName']+"\n"
        f.write(host)
    f.close()

# /home/ubuntu/install_files/hadoop-2.7.7/etc/hadoop/slaves
def createSlavesfile():
    f = open("slaves", "w")
    for row in csv_rows:
        slave = row['NodeName']+"\n"
        f.write(slave)
    f.close()

def createHostnamefile(row):
    filename = 'hostname'
    if os.path.exists(filename):
          os.remove(filename)
    f = open(filename, "w")
    hostname = row['NodeName']+"\n"
    f.write(hostname)
    f.close()

def createInitScript(row,cluster,master_dns):
    filename = 'init.sh'
    if os.path.exists(filename):
          os.remove(filename)
    f = open(filename, "w")
    # put commands into init.sh
    # general one
    initcmds = "sudo cp ~/Scripts/hostname /etc \n"
    initcmds += "sudo cp ~/Scripts/hosts /etc \n"
    initcmds += "source /home/ubuntu/env.sh\n"
    if cluster == "standalone":
        if row['NodeName'].find("master") != -1:
            initcmds += "$SPARK_HOME/sbin/start-master.sh -h "+ row['DNS']+"\n"
        elif row['NodeName'].find("slave") != -1:
            initcmds += "$SPARK_HOME/sbin/start-slave.sh spark://"+master_dns+":7077\n"
    elif cluster == "yarn":
        if row['NodeName'].find("master") != -1:
            initcmds+='sleep 1\n'
            #/home/ubuntu/install_files/hadoop-2.7.7/bin/hadoop namenode -format
            initcmds += "$HADOOP_HOME/bin/hadoop namenode -format\n"
            #/home/ubuntu/install_files/hadoop-2.7.7/sbin/start-dfs.sh
            initcmds += "$HADOOP_HOME/sbin/start-dfs.sh\n"
            #/home/ubuntu/install_files/hadoop-2.7.7/sbin/start-yarn.sh
            initcmds += "$HADOOP_HOME/sbin/start-yarn.sh\n"
        elif row['NodeName'].find("slave") != -1:
            initcmds+='\n'

    f.write(initcmds)
    f.close()

def createStopScript(row,cluster):
    filename = 'stop.sh'
    if os.path.exists(filename):
          os.remove(filename)
    f = open(filename, "w")
    # put commands into init.sh
    # general one
    stopcmds = "source /home/ubuntu/env.sh\n"
    if cluster == "standalone":
        if row['NodeName'].find("master") != -1:
            stopcmds += "$SPARK_HOME/sbin/stop-master.sh\n"
        elif row['NodeName'].find("slave") != -1:
            stopcmds += "$SPARK_HOME/sbin/stop-slave.sh\n"
    elif cluster == "yarn":
        stopcmds+=''

    f.write(stopcmds)
    f.close()

def Ops_SCP(dst,filename):
    noproxy_cmd = ["scp", "-i","ai_tce.pem",filename, dst]
    proxy_cmd = ["scp","-o", "ProxyCommand='nc -x proxy-us.intel.com:1080 %h %p'" ,"-i","ai_tce.pem",filename, dst]
    #print proxy_cmd
    if Need_Intel_Proxy==True:
        cmd = proxy_cmd
    else:
        cmd = noproxy_cmd
    print cmd
    p = subprocess.Popen(cmd)
    sts = os.waitpid(p.pid, 0)

def Ops_generate_scp_script(dst,filename,action):
    if os.path.isfile("./tmp/scp.sh"):
          os.remove("./tmp/scp.sh")
    if Need_Intel_Proxy==True:
        intel_proxy_cmd = "-o ProxyCommand='nc -x proxy-us.intel.com:1080 %h %p'"
    else:
        intel_proxy_cmd = ''

    # Create connect scripts
    if os.path.isdir('./tmp')==False:
        os.mkdir("./tmp")
    if action == 'push':
        scp_cmd ="scp "+intel_proxy_cmd+" "+"  -i ../ai_tce.pem "+filename+" "+dst
    elif action == "pull":
        scp_cmd ="scp "+intel_proxy_cmd+" "+"  -i ../ai_tce.pem "+dst+"  "+filename
    filepath = "./tmp/scp.sh"
    f = open(filepath, "w")
    f.write(scp_cmd)
    f.close()
    os.chmod(filepath, stat.S_IRUSR |stat.S_IEXEC |stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH )
    return filepath

def Ops_generate_ssh_script(row,cluster):
    # ssh -o ProxyCommand='nc -x proxy-us.intel.com:1080 %h %p' -L 8888:localhost:8888 -L 8080:localhost:8080  -i "ai_tce.pem" ubuntu@$1
    if Need_Intel_Proxy==True:
        intel_proxy_cmd = "-o ProxyCommand='nc -x proxy-us.intel.com:1080 %h %p'"
    else:
        intel_proxy_cmd = ''
    if row['NodeName'] == "master":
        if cluster == "standalone":
            port_tunnel = "-L 8888:localhost:8888 -L 8080:localhost:8080 "
        elif cluster == "yarn":
            port_tunnel = "-L 8888:localhost:8888 -L 8088:localhost:8088"
    else:
        port_tunnel = ''

    # Create connect scripts
    if os.path.isdir('./tmp')==False:
        os.mkdir("./tmp")
    ssh_cmd ="ssh "+intel_proxy_cmd+" "+port_tunnel+" "+"  -i ../ai_tce.pem "+" ubuntu@"+row['DNS']
    filename = row['NodeName']+".sh"
    filepath = "./tmp/"+filename
    f = open(filepath, "aw")
    f.write(ssh_cmd)
    f.close()
    os.chmod(filepath, stat.S_IRUSR |stat.S_IEXEC |stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH )
    return filepath

def getPrivateAddr(dns):
    IP='0.0.0.0'
    # get /etc/hostname
    dst = "ubuntu@"+dns+":/etc/hostname"
    filepath = "."
    filepath = Ops_generate_scp_script(dst,filepath,"pull")
    try:
        os.system(filepath)
    except:
        print "ERROR : NO VALID DNS!"
        return IP
    # parse IP from hostname
    with open("./hostname") as f:
            lines = f.readlines()
    for l in lines:
        l=l.strip("\n")
        print l
        IP=l.split("-")[1]+'.'+l.split("-")[2]+'.'+l.split("-")[3]+'.'+l.split("-")[4]
        print IP

    return IP

def parseCSV(filename,csvfile):
    dns_list=[]
    ip_list=[]
    fieldnames = ['NodeName', 'IP','DNS','PrivateIP']
    index=0
    print(filename)
    with open(filename) as f:
            lines = f.readlines()
    # get dns
    for l in lines:
        dns=l.split(' ')[1]
        dns=dns.strip(' ')
        dns=dns.strip('\n')
        dns_list.append(dns)
    # get ip and rows
    for dns in dns_list:
        print dns
        addr = socket.getaddrinfo(dns,None)[0][4][0]
        print addr
        if index == 0:
            nodename = 'master'
        else:
            nodename = 'slave'+str(index)
        private_addr = getPrivateAddr(dns)
        index+=1
        row={fieldnames[0]:nodename , fieldnames[1]:addr,fieldnames[2]:dns,fieldnames[3]:private_addr}
        print row
        csv_rows.append(row)
    csvfile = "spark_cluster.csv";
    fieldnames = ['NodeName', 'IP','DNS','PrivateIP']
    CSVwrite(csvfile,'',fieldnames,'wb',True)
    CSVwrite(csvfile,csv_rows,fieldnames,'ab',False)

def main(filename,cluster,csvfile):
    print 'main'
    # read from CSV file if input is csv file
    if filename == csvfile:
        print "using exist CSV file"
	import csv
        with open(csvfile, 'rb') as f:
                reader = csv.reader(f)
                index = 0
                fieldnames = []
                csvrow={}
                for row in reader:
                    if index == 0 :
                        fieldnames = row
                    else:
                        csvrow={fieldnames[0]:row[0] , fieldnames[1]:row[1],fieldnames[2]:row[2],fieldnames[3]:row[3]}
                        csv_rows.append(csvrow)
                    index+=1
    else:
        parseCSV(filename,csvfile)

    createHostsfile()
    for row in csv_rows:
        if row['NodeName']== 'master':
            master_dns = row['DNS']
            if cluster == "yarn":
                # /home/ubuntu/install_files/hadoop-2.7.7/etc/hadoop/slaves
                createSlavesfile()
                dst ="ubuntu@"+row['DNS']+":/home/ubuntu/install_files/hadoop-2.7.7/etc/hadoop/"
                filepath = Ops_generate_scp_script(dst,"slaves","push")
                os.system(filepath)
    gnome_terminal_cmd = ["gnome-terminal"]
    for row in csv_rows:
        # SCP the config file
        #scp -i "ai_tce.pem" scripts/instances.txt  ubuntu@ec2-18-191-197-117.us-east-2.compute.amazonaws.com:/home/ubuntu/

        # CSVFile
        dst = "ubuntu@"+row['DNS']+":/home/ubuntu/Scripts"
        filepath = Ops_generate_scp_script(dst,csvfile,"push")
        os.system(filepath)

        # /etc/hosts
        dst = "ubuntu@"+row['DNS']+":/home/ubuntu/Scripts"
        filepath = Ops_generate_scp_script(dst,"hosts","push")
        os.system(filepath)

        # /etc/hostname
        createHostnamefile(row)
        filepath = Ops_generate_scp_script(dst,"hostname","push")
        os.system(filepath)

        # create init.sh
        createInitScript(row,cluster,master_dns)
        filepath = Ops_generate_scp_script(dst,"init.sh","push")
        os.system(filepath)

        #~/.ssh/config
        dst = "ubuntu@"+row['DNS']+":/home/ubuntu/.ssh"
        filepath = Ops_generate_scp_script(dst,"config","push")
        os.system(filepath)

        # compose gnome-terminal command
        filepath = Ops_generate_ssh_script(row,cluster)

        #gnome-terminal  --tab -e "bash -c 'ps -ef; bash'" --tab -e "bash -c 'ls;bash'" --tab -e "bash -c 'top -n 1; bash'"
        gnome_terminal_cmd.append("--tab")
        gnome_terminal_cmd.append("-e")
        bashcmd = "bash -c '"+filepath+"';bash"
        gnome_terminal_cmd.append(bashcmd)
        #raw_input("Press Enter to continue...")

    print gnome_terminal_cmd
    p = subprocess.Popen(gnome_terminal_cmd)
    sts = os.waitpid(p.pid, 0)

    time.sleep( 5 )
    # stop script
    for row in csv_rows:
        # create stop.sh
        createStopScript(row,cluster)
        filepath = Ops_generate_scp_script(dst,"stop.sh","push")
        os.system(filepath)
        time.sleep( 1 )

    if os.path.isdir('./tmp')==True:
        os.system("rm -rf ./tmp")

if __name__ == "__main__":
    import argparse
    csvfile = "spark_cluster.csv";
    Need_Intel_Proxy=False
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='Optional app description')
    parser.add_argument("-f", "--file", help="file name for instances",
            dest="file", default="")
    parser.add_argument("-c", "--cluster", help="cluster mode. standalone, yarn",
            dest="cluster", default="standalone")
    parser.add_argument("-p", "--proxy", help="proxy type",
            dest="proxy", default="")
    # Required positional argument

    args = parser.parse_args()
    if args.file == '':
        parser.print_help()
        exit()
    if args.proxy == 'intel':
        Need_Intel_Proxy=True
    main(args.file,args.cluster,csvfile)
