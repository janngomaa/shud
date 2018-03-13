from scrapy.spiders import BaseSpider
from scrapy.selector import Selector
from shudamazon.items import BasicCrawlerItem
from scrapy.http import Request
import re


class MySpider(BaseSpider):
	name = "basic_crawler"
	allowed_domains = ['amazon.ca']
	start_urls = ["https://www.amazon.ca/gp/goldbox"]

	def parse(self, response):
		hxs = Selector(response)
		
		print("Crawling " + str(response.url))

		#CODE for scraping book titles
		book_titles = hxs.xpath('//*[@id="dealTitle"]/span/text()').extract()
	 	for title in book_titles:
			book = BasicCrawlerItem()
			book["title"] = title
			yield book


		visited_links=[]
		links = hxs.xpath('//a/@href').extract()
                link_validator = re.compile("^(?:http|https):(.)*[ref=gbps_tit_s](.)*")

		
		for link in links:
			if link_validator.match(link) and not link in visited_links:
				visited_links.append(link)
				yield Request(link, self.parse)
			else:
				full_url=response.urljoin(link)
				visited_links.append(full_url)
				yield Request(full_url, self.parse)
