#  Run Scrapy from Docker 
#  docker exec -i -t sparkc sh work/shud/gitconfig.sh

#Initializing git on
#cd work
#echo "*****  Initializing git on $PWD/shud  *****" 
#git init shud
#cd shud
#git remote add origin https://github.com/janngomaa/shud.git

echo "*****  Adding all element in $PWD to git repo  *****" 

cd work/shud

git add *

git commit -m "$1"

git push origin master