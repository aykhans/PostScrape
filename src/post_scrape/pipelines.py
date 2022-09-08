import sqlite3


class SqlitePostPipeline:
    def __init__(self):
        self.id = 1
        self.con = sqlite3.connect('cars.db')
        self.cur = self.con.cursor()

        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS cars(
            id INTEGER,
            url TEXT,
            avto_salon TEXT,
            description TEXT,
            city TEXT,
            brand TEXT,
            model TEXT,
            year INTEGER,
            category TEXT,
            color TEXT,
            engine_volume INTEGER,
            engine_power INTEGER,
            fuel_type TEXT,
            mileage INTEGER,
            mileage_type TEXT,
            transmission TEXT,
            gear TEXT,
            price INTEGER,
            currency TEXT,
            loan TEXT,
            barter TEXT,
            market TEXT,
            seats_count TEXT,
            prior_owners_count TEXT,
            crashed TEXT,
            painted TEXT
        )
        """)
        # IMAGE
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS images(
            post_id INTEGER,
            url TEXT
        )
        """)
        # PHONE
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            post_id INTEGER,
            phone TEXT
        )
        """)
        # EXTRA_FIELDS
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS extra_fields(
            post_id INTEGER,
            extra_field TEXT
        )
        """)
        
        self.cur.execute("SELECT * FROM cars ORDER BY id DESC LIMIT 1")
        result = self.cur.fetchone()
        if result is not None: self.id = result[0] + 1

    def process_item(self, item, spider):
        self.cur.execute("""
            INSERT INTO cars (id, url, avto_salon, description, city, brand, model, year, category, color, engine_volume, engine_power, fuel_type, mileage, mileage_type, transmission, gear, price, currency, loan, barter, market, seats_count, prior_owners_count, crashed, painted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            self.id,
            item['url'],
            item['avto_salon'],
            ' '.join(item['description']),
            item['city'],
            item['brand'],
            item['model'],
            item['year'],
            item['category'],
            item['color'],
            item['engine_volume'],
            item['engine_power'],
            item['fuel_type'],
            item['mileage'],
            item['mileage_type'],
            item['transmission'],
            item['gear'],
            item['price'],
            item['currency'],
            item['loan'],
            item['barter'],
            item['market'],
            item['seats_count'],
            item['prior_owners_count'],
            item['crashed'],
            item['painted']
        ))
        # IMAGE
        for image in item['images']:
            self.cur.execute("""INSERT INTO images (post_id, url) VALUES (?, ?)""",
                            (self.id, image))
        # PHONE
        for phone in item['phone']:
            self.cur.execute("""INSERT INTO phones (post_id, phone) VALUES (?, ?)""",
                            (self.id, phone))
        # EXTRA_FIELDS
        for field in item['extra_fields']:
            self.cur.execute("""INSERT INTO extra_fields (post_id, extra_field) VALUES (?, ?)""",
                            (self.id, field))

        self.con.commit()
        self.id += 1
        return item
