from scrapy.spiders import Spider
from scrapy import Request


class CarDataSpider(Spider):
    name = "turbo.az"
    allowed_domains = ('turbo.az',)
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
    }

    def __init__(self, first_page=1, last_page=2, *args, **kwargs):
        super(CarDataSpider, self).__init__(*args, **kwargs)
        self.first_page = first_page
        self.last_page = int(last_page)

    def start_requests(self):
        urls = [
            f'https://turbo.az/autos?page={self.first_page}',
        ]

        for url in urls:
            yield Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        posts = response.xpath('//div[@class="products-container"]//div[@class="products"]')[2].xpath('./div/a[@class="products-i__link"]/@href')

        for p in posts:
            yield response.follow(f"https://turbo.az{p.get()}", callback=self.parse_detail_url, headers=self.headers)

        next_page = response.xpath('//a[@rel="next"]/@href').get()
        if next_page is not None and int(next_page[next_page.rfind('=')+1:]) < self.last_page:
            yield response.follow(f"https://turbo.az{next_page}", callback=self.parse, headers=self.headers)

    def parse_detail_url(self, r):
        images = [r.xpath('//a[@class="product-photos-large"]/@href').get()]
        images += r.xpath('//div[@class="product-photos"]/div/a/@href').getall()
        if r.xpath('//div[@class="shop-container"]'):
            avto_salon = True
            phone = r.xpath('//div[@class="shop-contact--phones-list"]//a[@class="shop-contact--phones-number"]/text()').getall()
        else:
            avto_salon = False
            phone = [r.xpath('//a[@class="phone"]/text()').get()]

        barter, loan = False, False

        if r.xpath('//li[@class="product-properties-i product-properties-i_loan"]'):
            loan = True
        if r.xpath('//li[@class="product-properties-i product-properties-i_barter"]'):
            barter = True

        seats_count = r.xpath('//label[@for="ad_seats_count"]')
        prior_owners_count = r.xpath('//label[@for="ad_prior_owners_count"]')

        price = r.xpath('//li[@class="product-properties-i product-properties_price"]//div[@class="product-price"]/text()').get().replace(' ', '')
        currency = r.xpath('//li[@class="product-properties-i product-properties_price"]//div[@class="product-price"]/span/text()').get()
        extra_fields = r.xpath('//p[@class="product-extras-i"]/text()').getall()
        description = r.xpath('//div[@class="product-description"]/p/text()').getall()
        market = r.xpath('//li[@class="product-properties-i product-properties-market"]//div/text()').get()
        data = r.xpath('//li[@class="product-properties-i"]/div[@class="product-properties-value"]/text()').getall()
        data2 = r.xpath('//li[@class="product-properties-i"]/div[@class="product-properties-value"]/a/text()').getall()

        if seats_count and prior_owners_count:
            if '4 və daha çox' in data and '8+' in data:
                prior_owners_count = data.pop(data.index('4 və daha çox'))
                seats_count = data.pop(data.index('8+'))
            elif '4 və daha çox' in data:
                prior_owners_count = data.pop(data.index('4 və daha çox'))
                seats_count = r.xpath('//li[@class="product-properties-i"]/div[@class="product-properties-value"]/span/text()').get()
            elif '8+' in data:
                seats_count = data.pop(data.index('8+'))
                prior_owners_count = r.xpath('//li[@class="product-properties-i"]/div[@class="product-properties-value"]/span/text()').get()
            else:
                seats_count, prior_owners_count = r.xpath('//li[@class="product-properties-i"]/div[@class="product-properties-value"]/span/text()').getall()

        elif seats_count:
            if '8+' in data:
                seats_count = data.pop(data.index('8+'))
            else:
                seats_count = r.xpath('//li[@class="product-properties-i"]/div[@class="product-properties-value"]/span/text()').get()
            prior_owners_count = None
        elif prior_owners_count:
            if '4 və daha çox' in data:
                prior_owners_count = data.pop(data.index('4 və daha çox'))
            else:
                prior_owners_count = r.xpath('//li[@class="product-properties-i"]/div[@class="product-properties-value"]/span/text()').get()
            seats_count = None
        else:
            seats_count = None
            prior_owners_count = None

        if len(data) < 11:
            crashed, painted = None, None
        else:
            crashed = 'Vuruğu yoxdur' not in data[10]
            painted = 'rənglənməyib' not in data[10]

        yield {
            'url': r.url,
            'avto_salon': avto_salon,
            'images': images,
            'phone': phone,
            'extra_fields': extra_fields,
            'description': description,
            'city': data[0],
            'brand': data2[0],
            'model': data2[1],
            'year': int(data2[2]),
            'category': data[1],
            'color': data[2],
            'engine_volume': int(float(data[3][:-2])*1000),
            'engine_power': int(data[4][:-5]),
            'fuel_type': data[5],
            'mileage': int(data[6][:-3].replace(' ', '')),
            'mileage_type': data[6][-2:],
            'transmission': data[7],
            'gear': data[8],
            'price': int(price),
            'currency': currency,
            'loan': loan,
            'barter': barter,
            'market': market,
            'seats_count': seats_count,
            'prior_owners_count': prior_owners_count,
            'crashed':  crashed,
            'painted': painted,
        }