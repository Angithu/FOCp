from watchdog.events import FileSystemEventHandler
import threading
import glob
from ftplib import FTP
def ftpconnect(host, port, username, password):
    print(host, port, username, password)
    ftp = FTP()
    ftp.set_debuglevel(2)
    ftp.connect(host, port)
    ftp.login(username, password)
    return ftp

# FTP function
class WatchdogWithFile(FileSystemEventHandler):
    def __init__(self, lock, settingcon,LOG):
        FileSystemEventHandler.__init__(self)
        self.cnt = 0
        self.timer = None
        self.lock = lock
        self.settingcon = settingcon
        self._LOG = LOG
        print("save",self.settingcon.get("savefilename"))
    def on_modified(self, event):
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(0.2, self.setup, args=(event.src_path, self.settingcon, self.lock,self._LOG))
        self.timer.start()
    def setup(self,filename,pathconfig ,Lock, Log):
        Lock.acquire()
        try:
            targetPattern = pathconfig.get("remotefilesite") + pathconfig.get("filenametype")
            print(targetPattern)
            kind_file = glob.glob(targetPattern)
            print(kind_file)
            if len(kind_file) > 0:
                Log.info(str(kind_file) + " " + str(filename) + " --select")
                # print(str(kind_file) + " " + str(filename) + "  select")
                for i in range(len(kind_file)):
                    file_type = kind_file[i].split('\\')
                    kind_type = (filename.split('\\'))
                    if file_type[(len(file_type) - 1)] == kind_type[(len(kind_type) - 1)]:
                        SMGFTP = ftpconnect(pathconfig.get("ip"), pathconfig.get("port"), pathconfig.get("username"), pathconfig.get("password"))
                        # print(SMGFTP.getwelcome())  # 獲得歡迎資訊A
                        SMGFTP.cwd(pathconfig.get("savefilename"))  # 設定FTP路徑
                        # print(filename, "Add file")
                        Log.info(filename + " --Add file")
                        print(open(filename, 'rb'), "read")
                        SMGFTP.storbinary("STOR " + str(kind_type[(len(kind_type) - 1)]), open(filename, 'rb'))
                        SMGFTP.quit()
                        break
            else:
                # print(str(kind_file) + " " + str(filename) + "  false")
                Log.info(str(kind_file) + " " + str(filename) + "  False")
        except Exception as e:
            Log.error("FTP_ERROR: "+ str(e))
            # print("ERROR " + str(e))
        finally:
            Lock.release()
            self.timer = None
