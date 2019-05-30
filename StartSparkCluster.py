import sys
import getopt
import socket
import subprocess
import os
import stat
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
def parseCSV(filename,csvfile):
    dns_list=[]
    ip_list=[]
    fieldnames = ['NodeName', 'IP','DNS']
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
        index+=1
        row={fieldnames[0]:nodename , fieldnames[1]: addr,fieldnames[2]:dns}
        print row
        csv_rows.append(row)
    csvfile = "spark_cluster.csv";
    fieldnames = ['NodeName', 'IP','DNS']
    CSVwrite(csvfile,'',fieldnames,'wb',True)
    CSVwrite(csvfile,csv_rows,fieldnames,'ab',False)

def main(filename,csvfile):
    print 'main'
    parseCSV(filename,csvfile)
    gnome_terminal_cmd = ["gnome-terminal"]
    for row in csv_rows:
        # SCP the config file
        #scp -i "ai_tce.pem" scripts/instances.txt  ubuntu@ec2-18-191-197-117.us-east-2.compute.amazonaws.com:/home/ubuntu/
        dst = "ubuntu@"+row['DNS']+":/home/ubuntu/Scripts"
        noproxy_cmd = ["scp", "-i","ai_tce.pem",csvfile, dst]
        proxy_cmd = ["scp","-o", "ProxyCommand='nc -x proxy-us.intel.com:1080 %h %p'" ,"-i","ai_tce.pem",csvfile, dst]
        #print proxy_cmd
        if Need_Intel_Proxy==True:
            cmd = proxy_cmd
        else:
            cmd = noproxy_cmd
        print cmd
        #p = subprocess.Popen(cmd)
        #sts = os.waitpid(p.pid, 0)
        # compose gnome-terminal command
        # ssh -o ProxyCommand='nc -x proxy-us.intel.com:1080 %h %p' -L 8888:localhost:8888 -L 8080:localhost:8080  -i "ai_tce.pem" ubuntu@$1
        if Need_Intel_Proxy==True:
            intel_proxy_cmd = "-o ProxyCommand='nc -x proxy-us.intel.com:1080 %h %p'"
        else:
            intel_proxy_cmd = ''
        if row['NodeName'] == "master":
            port_tunnel = "-L 8888:localhost:8888 -L 8080:localhost:8080"
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

        #gnome-terminal  --tab -e "bash -c 'ps -ef; bash'" --tab -e "bash -c 'ls;bash'" --tab -e "bash -c 'top -n 1; bash'"
        gnome_terminal_cmd.append("--tab")
        gnome_terminal_cmd.append("-e")
        bashcmd = "bash -c '"+filepath+"';bash"
        gnome_terminal_cmd.append(bashcmd)

    print gnome_terminal_cmd
    p = subprocess.Popen(gnome_terminal_cmd)
    sts = os.waitpid(p.pid, 0)
    if os.path.isdir('./tmp')==True:
        os.system("rm -rf ./tmp")

if __name__ == "__main__":
    import argparse
    csvfile = "spark_cluster.csv";
    Need_Intel_Proxy=True
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='Optional app description')
    parser.add_argument("-f", "--file", help="file name for instances",
            dest="file", default="")
    # Required positional argument

    args = parser.parse_args()
    if args.file == '':
        parser.print_help()
        exit()
    main(args.file,csvfile)
