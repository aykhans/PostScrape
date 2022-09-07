## Installition
pip install scrapy<br />

## Run
cd src/<br />
scrapy crawl turbo.az -O cars.jl<br />
**or**<br />
scrapy crawl turbo.az -O cars.jl -a first_page=3 -a last_page=10<br />
*Default: first_page=1, last_page=2*