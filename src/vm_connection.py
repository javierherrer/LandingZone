import paramiko

class VM():

    _DIR_TEMPORAL = "/user/temporal"

    def __init__(self, hostname="10.4.41.51", port=22, username="bdm", password="bdm"):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self._get_vm_connection()

    def _get_vm_connection(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.hostname, self.port, username=self.username, password=self.password)
        print("Connected to {}".format(self.hostname))

    def _execute_linux(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        err = ''.join(stderr.readlines())
        out = ''.join(stdout.readlines())
        final_output = str(out) + str(err)
        return final_output

    def exe(self, cmd):
        return self._execute_linux(cmd)

    def details(self):
        print("Connection Details:")
        print("IP: \t{}".format(self.hostname))
        print("Port: \t{}".format(self.port))
        print("Username: \t{}".format(self.username))
        print("Password: \t{}".format(self.password))

    def transfer_files(self, lpath, rpath):
        ftp_client = self.ssh.open_sftp()
        ftp_client.put(lpath, rpath)
        ftp_client.close()
