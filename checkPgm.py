import psutil, time, sys
import subprocess

def main(input):
    for p in psutil.process_iter():
        if p.pid == input and p.status=='running':
            print('processus trouvé', p)
            break
    else:
        print("Not found")
        subprocess.Popen('python get_twitter', shell=True)

if __name__ == "__main__":
    while True:
        with open("pidTwitter.txt", "r") as fic:
            input = int(fic.read())
            print(input)


        #for line in p.stdout.readlines():
            #print(line)

        # nom du processus
        main(input)
        time.sleep(30)

