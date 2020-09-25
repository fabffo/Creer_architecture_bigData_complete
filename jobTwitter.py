#Lancement de l'API TWITTER en flux continu
# si l'api est en erreur, le programme est automatiquement relancé au bout de 30 secondes
import subprocess, time
if __name__ == "__main__":
    while True:
        p = subprocess.Popen('python get_twitter', shell=True)
        for line in p.stdout.readlines():
            print(line)
        print("fin premier cycle")
        time.sleep(30)
