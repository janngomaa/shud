#  Run Scrapy from Docker 
#  docker exec -i -t sparkc sh work/shud/gitconfig.sh

cd work

echo "*****  Initializing git on $PWD/shud  *****" 
git init shud

cd shud
echo "*****  Adding all element in $PWD to git repo  *****" 
git add *

git commit -m "Shud Initial commit"

#git remote add origin https://github.com/janngomaa/shud.git

git pull

git push origin master