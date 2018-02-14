# Option 1: Initializing ==> Uncomment untill line before Option 2
#https://www.howtoforge.com/tutorial/install-git-and-github-on-ubuntu-14.04/
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

git push origin master