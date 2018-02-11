#  Run Scrapy from Docker 
#  docker exec -i -t sparkc sh work/shud/shudamazon/run.sh

cd work/shud/shudamazon

echo "*****  Running Upw Spider on $PWD  *****" 

scrapy crawl amazon --s CLOSESPIDER_PAGECOUNT=10 -o shuddata.jl #-s LOG_FILE=upw.log