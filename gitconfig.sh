# Option 1: Initializing ==> Uncomment untill line before Option 2
#https://www.howtoforge.com/tutorial/install-git-and-github-on-ubuntu-14.04/
#https://git-scm.com/book/fr/v1/Les-bases-de-Git-Travailler-avec-des-d%C3%A9p%C3%B4ts-distants
#  Run in Docker container
#  docker exec -i -t sparkc sh work/shud/gitconfig.sh

#Initializing git on
#cd work
#echo "*****  Initializing git on $PWD/shud  *****" 
#git init shud
#cd shud
#git remote add origin https://github.com/janngomaa/shud.git

#Option 2: Cloning existing repo

#git clone https://github.com/janngomaa/shud

echo "*****  Adding all element in $PWD to git repo  *****" 

cd work/shud

git add *

git commit -m "$1"

git pull remote master

git push origin master