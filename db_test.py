import subprocess
import time

def runScript(script):
    popen = subprocess.Popen("./main", stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    script = [i+'\n' for i in script]
    res = []
    # for command in script:
        # time.sleep(1)
        # stdout_data = popen.stdin.write(b'select\n')



    
    return popen.communicate(input=b'.exit\n')

print(runScript([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit"]))

